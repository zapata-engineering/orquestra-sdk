################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
RuntimeInterface implementation that uses Ray DAG/Ray Core API.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import re
import traceback
import typing as t
from datetime import datetime, timedelta, timezone
from functools import singledispatch
from pathlib import Path

import pydantic

from .. import exceptions, secrets
from .._base import (
    _exec_ctx,
    _git_url_utils,
    _graphs,
    _log_adapter,
    _services,
    dispatch,
    serde,
)
from .._base._db import WorkflowDB
from .._base._env import RAY_DOWNLOAD_GIT_IMPORTS_ENV, RAY_GLOBAL_WF_RUN_ID_ENV
from .._base.abc import ArtifactValue, LogReader, RuntimeInterface
from ..schema import ir
from ..schema.configs import RuntimeConfiguration
from ..schema.local_database import StoredWorkflowRun
from ..schema.workflow_run import (
    RunStatus,
    State,
    TaskInvocationId,
    TaskRun,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
)
from . import _client, _id_gen, _ray_logs
from ._client import RayClient


def _locate_user_fn(fn_ref: ir.FunctionRef):
    """
    Dereferences 'fn_ref', loads the module attribute, and extracts the
    underlying user function if it was a TaskDef.
    """
    obj: t.Any = dispatch.locate_fn_ref(fn_ref)

    # dsl.task() wraps a callable in a TaskDef object. We need to locate the
    # underlying user function, not the Task object.
    try:
        return obj._TaskDef__sdk_task_body
    except AttributeError:
        return obj


@dataclasses.dataclass(frozen=True)
class PosArgUnpackSpec:
    """
    Specifies that a given positional argument passed by Ray to a task is a
    tuple and needs to be unwrapped.

    For example, if the workflow contained this:
        foo, bar = my_task1()
        baz = my_task2("a", "b", foo)

    the corresponding spec would be:
        PosArgUnpackSpec(param_index=2, unpack_index=0)
    """

    # Index of the param in the function's parameter list.
    param_index: int

    # Index of the value returned from the previous task.
    unpack_index: int


@dataclasses.dataclass(frozen=True)
class KwArgUnpackSpec:
    """
    Specifies that a given positional argument passed by Ray to a task is a
    tuple and needs to be unwrapped.

    For example, if the workflow contained this:
        foo, bar = my_task1()
        baz = my_task2(a="a", b="b", c=foo)

    the corresponding spec would be:
        KwArgUnpackSpec(param_name="c", unpack_index=0)
    """

    # Name the keyword param in the function's signature.
    param_name: str

    # Index of the value returned from the previous task.
    unpack_index: int


class TupleUnwrapper:
    """
    Adds a preprocessing step to a user function. Before args and kwargs are
    passed to the function, `TupleUnwrapper` looks into `pos_specs` and
    `kw_specs` to unwrap tuples.
    """

    def __init__(
        self,
        fn,
        pos_specs: t.Sequence[PosArgUnpackSpec],
        kw_specs: t.Sequence[KwArgUnpackSpec],
    ):
        """
        Args:
            fn: user function to apply the preprocessing to.
            pos_specs: for every item in this list, a corresponding positional
                argument will be unwrapped.
            kw_specs: for every item in this list, a corresponding keyword
                argument will be unwrapped.
        """
        self._fn = fn
        self._pos_specs = pos_specs
        self._kw_specs = kw_specs

    def __call__(self, *args, **kwargs):
        """
        'args' and 'kwargs' are what Ray passes to functions. Some of these
        might be tuples that we need to unpack.

        More more info and the motivation for tuple unwrapping, see:
        https://zapatacomputing.atlassian.net/browse/ORQSDK-490
        """
        # This approach is mutation-based (copy fn_args + change by index),
        # because that's the easiest way for us not to rely on
        # `pos_specs`'s element order.
        unpacked_args = list(args)
        for pos_spec in self._pos_specs:
            arg_val = args[pos_spec.param_index]
            unpacked_val = arg_val[pos_spec.unpack_index]
            unpacked_args[pos_spec.param_index] = unpacked_val

        unpacked_kwargs = dict(kwargs)
        for kw_spec in self._kw_specs:
            arg_val = kwargs[kw_spec.param_name]
            unpacked_val = arg_val[kw_spec.unpack_index]
            unpacked_kwargs[kw_spec.param_name] = unpacked_val

        return self._fn(*unpacked_args, **unpacked_kwargs)


# fake Ray DAG node that aggregates IR workflow's outputs
def _aggregate_outputs(*regular_wf_outputs):
    return regular_wf_outputs


def _make_ray_dag_node(
    client: RayClient,
    ray_options: t.Mapping,
    ray_args: t.Iterable[t.Any],
    ray_kwargs: t.Mapping[str, t.Any],
    pos_unpack_specs: t.Sequence[PosArgUnpackSpec],
    kw_unpack_specs: t.Sequence[KwArgUnpackSpec],
    project_dir: t.Optional[Path],
    user_fn_ref: t.Optional[ir.FunctionRef] = None,
) -> _client.FunctionNode:
    """
    Prepares a Ray task that fits a single ir.TaskInvocation. The result is a
    node in a Ray DAG.

    Args:
        client: Ray API facade
        ray_options: dict passed to RayClient.add_options()
        ray_args: constants or futures required to build the DAG
        ray_kwargs: constants or futures required to build the DAG
        pos_unpack_specs: determines if the task will need to unwrap a
            positional argument value at execution time.
        kw_unpack_specs: determines if the task will need to unwrap a
            keyword argument value at execution time.
        user_fn_ref: function reference for a function to be executed by Ray.
            if None - executes data aggregation step
    """

    @client.remote
    def _ray_remote(*inner_args, **inner_kwargs):
        if project_dir is not None:
            dispatch.ensure_sys_paths([str(project_dir)])

        user_fn = _locate_user_fn(user_fn_ref) if user_fn_ref else _aggregate_outputs
        wrapped = TupleUnwrapper(
            fn=user_fn,
            pos_specs=pos_unpack_specs,
            kw_specs=kw_unpack_specs,
        )
        logger = _log_adapter.workflow_logger()
        try:
            with _exec_ctx.ray():
                return wrapped(*inner_args, **inner_kwargs)
        except Exception as e:
            # Log the stacktrace as a single log line.
            logger.exception(traceback.format_exc())

            # We need to stop further execution of this workflow. If we don't raise, Ray
            # will think the task succeeded with a return value `None`.
            raise e

    named_remote = client.add_options(_ray_remote, **ray_options)
    dag_node = named_remote.bind(*ray_args, **ray_kwargs)

    return dag_node


def _instant_from_timestamp(unix_timestamp: t.Optional[float]) -> t.Optional[datetime]:
    """
    Parses a unix epoch timestamp (UTC seconds since 1970) into a
    timezone-aware datetime object.
    """
    if unix_timestamp is None:
        return None
    return datetime.fromtimestamp(unix_timestamp, timezone.utc)


class InvUserMetadata(pydantic.BaseModel):
    """
    Information about a task invocation we store as a Ray metadata dict.

    Pydantic helps us check that the thing we read from Ray is indeed a dictionary we
    set (i.e. it has proper fields).
    """

    # Invocation ID. Scoped to a single workflow def. Allows to distinguish
    # between multiple calls of as single task def inside a workflow.
    # Duplicated across workflow runs.
    task_invocation_id: ir.TaskInvocationId

    # (Hopefully) globally unique identifier of as single task execution. Allows
    # to distinguish invocations of the same task across workflow runs.
    task_run_id: TaskRunId


class WfUserMetadata(pydantic.BaseModel):
    """
    Information about a workflow run we store as a Ray metadata dict.

    Pydantic helps us check that the thing we read from Ray is indeed a dictionary we
    set (i.e. it has proper fields).
    """

    # Full definition of the workflow that's being run.
    workflow_def: ir.WorkflowDef


@singledispatch
def _pip_string(_: ir.Import) -> t.List[str]:
    return []


@_pip_string.register
def _(imp: ir.PythonImports):
    return [serde.stringify_package_spec(package) for package in imp.packages]


@_pip_string.register
def _(imp: ir.GitImport):
    # Only download Git imports if a specific environment variable is set
    # Short circuit the Git import otherwise
    if os.getenv(RAY_DOWNLOAD_GIT_IMPORTS_ENV) != "1":
        return []
    protocol = imp.repo_url.protocol
    if not protocol.startswith("git+"):
        protocol = f"git+{protocol}"
    url = _git_url_utils.build_git_url(imp.repo_url, protocol)
    return [f"{url}@{imp.git_ref}"]


def _import_pip_env(ir_invocation: ir.TaskInvocation, wf: ir.WorkflowDef):
    task_def = wf.tasks[ir_invocation.task_id]
    imports = [
        wf.imports[id_]
        for id_ in (
            task_def.source_import_id,
            *(task_def.dependency_import_ids or []),
        )
    ]
    return [chunk for imp in imports for chunk in _pip_string(imp)]


def _gather_args(
    arg_ids: t.Sequence[ir.ArgumentId],
    ray_consts: t.Mapping[ir.ConstantNodeId, t.Any],
    ray_futures: t.Mapping[ir.ArtifactNodeId, t.Any],
    artifact_nodes: t.Mapping[ir.ArtifactNodeId, ir.ArtifactNode],
) -> t.Tuple[t.Sequence[t.Any], t.Sequence[PosArgUnpackSpec]]:

    ray_args = []
    pos_unpack_specs: t.List[PosArgUnpackSpec] = []

    for param_i, arg_id in enumerate(arg_ids):
        try:
            arg_val = ray_consts[arg_id]
            ray_args.append(arg_val)
        except KeyError:
            # 'arg_id' isn't in 'ray_consts' -> it's an artifact, a result of
            # calling another task.

            # if the Workflow.steps are sorted in a topological order we should be
            # able to access the argument future
            arg_future = ray_futures[arg_id]
            ray_args.append(arg_future)

            if (unpack_index := artifact_nodes[arg_id].artifact_index) is not None:
                pos_unpack_specs.append(
                    PosArgUnpackSpec(param_index=param_i, unpack_index=unpack_index)
                )

    return ray_args, pos_unpack_specs


def _gather_kwargs(
    kwarg_ids: t.Mapping[ir.ParameterName, ir.ArgumentId],
    ray_consts: t.Mapping[ir.ConstantNodeId, t.Any],
    ray_futures: t.Mapping[ir.ArtifactNodeId, t.Any],
    artifact_nodes: t.Mapping[ir.ArtifactNodeId, ir.ArtifactNode],
) -> t.Tuple[t.Mapping[str, t.Any], t.Sequence[KwArgUnpackSpec]]:
    ray_kwargs = {}
    kw_unpack_specs: t.List[KwArgUnpackSpec] = []

    for param_name, arg_id in kwarg_ids.items():
        try:
            arg_val = ray_consts[arg_id]
            ray_kwargs[param_name] = arg_val
        except KeyError:
            # 'arg_id' isn't in 'ray_consts' -> it's an artifact, a result of
            # calling another task.

            # if the Workflow.steps are sorted in a topological order we should
            # be able to access the argument future
            arg_future = ray_futures[arg_id]
            ray_kwargs[param_name] = arg_future

            if (unpack_index := artifact_nodes[arg_id].artifact_index) is not None:
                kw_unpack_specs.append(
                    KwArgUnpackSpec(param_name=param_name, unpack_index=unpack_index)
                )
    return ray_kwargs, kw_unpack_specs


def _make_ray_dag(
    client: RayClient, wf: ir.WorkflowDef, wf_run_id: str, project_dir: Path
):
    ray_consts: t.Dict[ir.ConstantNodeId, t.Any] = {
        id: serde.deserialize_constant(node) for id, node in wf.constant_nodes.items()
    }
    for id, secret in wf.secret_nodes.items():
        ray_consts[id] = secrets.get(
            secret.secret_name, config_name=secret.secret_config
        )
    # a mapping of "artifact ID" <-> "the ray Future needed to get the value"
    ray_futures: t.Dict[ir.ArtifactNodeId, t.Any] = {}

    for ir_invocation in _graphs.iter_invocations_topologically(wf):
        # Prep args, kwargs, and the specs required to unpack tuples
        ray_args, pos_unpack_specs = _gather_args(
            arg_ids=ir_invocation.args_ids,
            ray_consts=ray_consts,
            ray_futures=ray_futures,
            artifact_nodes=wf.artifact_nodes,
        )

        ray_kwargs, kw_unpack_specs = _gather_kwargs(
            kwarg_ids=ir_invocation.kwargs_ids,
            ray_consts=ray_consts,
            ray_futures=ray_futures,
            artifact_nodes=wf.artifact_nodes,
        )

        # We want to store both the TaskInvocation.id and TaskRun.id. We use
        # TaskInvocation.id to refer to Ray tasks later. Solution: Ray task
        # name for identification and Ray metadata for anything else.
        inv_metadata = InvUserMetadata(
            task_run_id=_gen_task_run_id(wf_run_id=wf_run_id, invocation=ir_invocation),
            task_invocation_id=ir_invocation.id,
        )

        pip = _import_pip_env(ir_invocation, wf)

        ray_options = {
            # We're using task invocation ID as the Ray "task ID" instead of task run ID
            # because it's easier to query this way. Use the "user_metadata" to get both
            # identifiers.
            "name": ir_invocation.id,
            "metadata": _pydatic_to_json_dict(inv_metadata),
            # If there are any python packages to install for step - set runtime env
            "runtime_env": (_client.RuntimeEnv(pip=pip) if len(pip) > 0 else None),
            "catch_exceptions": False,
        }

        ray_future = _make_ray_dag_node(
            client=client,
            user_fn_ref=wf.tasks[ir_invocation.task_id].fn_ref,
            ray_options=ray_options,
            ray_args=ray_args,
            ray_kwargs=ray_kwargs,
            pos_unpack_specs=pos_unpack_specs,
            kw_unpack_specs=kw_unpack_specs,
            project_dir=project_dir,
        )

        for output_id in ir_invocation.output_ids:
            ray_futures[output_id] = ray_future

    # Gather futures for the last, fake task, and decide what args we need to unwrap.
    aggr_task_args, aggr_task_specs = _gather_args(
        arg_ids=wf.output_ids,
        ray_consts=ray_consts,
        ray_futures=ray_futures,
        artifact_nodes=wf.artifact_nodes,
    )

    last_future = _make_ray_dag_node(
        client=client,
        # The last step is implicit; it doesn't map to any user-defined Task
        # Invocation. We don't need to assign any metadata to it.
        ray_options={
            "name": None,
            "metadata": None,
            "runtime_env": None,
            "catch_exceptions": True,
        },
        ray_args=aggr_task_args,
        ray_kwargs={},
        pos_unpack_specs=aggr_task_specs,
        kw_unpack_specs=[],
        project_dir=None,
    )

    # Data aggregation step is run with catch_exceptions=True - so it returns tuple of
    # return value and Exception. Here the exception is caught and rethrown in more
    # user-friendly fashion
    @client.remote
    def handle_data_aggregation_error(result: t.Tuple[t.Any, Exception]):
        # The exception field will be None on success.
        err = result[1]
        if err is not None:
            if isinstance(err, _client.TaskError):
                raise exceptions.InvalidWorkflowDefinitionError(
                    "Data Aggregation step failed. It might be caused by the return "
                    "object being dependent on task-scope installed library. Please "
                    "return objects that are available for the interpreter. "
                    f"Original exception: {err}"
                )
            else:
                raise err
        else:
            return result[0]

    return handle_data_aggregation_error.bind(last_future)


def _pydatic_to_json_dict(pydantic_obj) -> t.Mapping[str, t.Any]:
    """
    Produces a JSON-serializable dict.
    """
    return json.loads(pydantic_obj.json())


def _generate_wf_run_id(wf_def: ir.WorkflowDef):
    """
    Implements the "tagging" design doc:
    https://zapatacomputing.atlassian.net/wiki/spaces/ORQSRUN/pages/479920161/Logging+Tagging

    Assumed to be globally unique.

    Example value: "wf.multioutput_wf.91aa7aa"
    """
    wf_name = wf_def.name
    hex_str = _id_gen.gen_short_uid(char_length=7)

    return f"wf.{wf_name}.{hex_str}"


def _gen_task_run_id(wf_run_id: str, invocation: ir.TaskInvocation):
    """
    Loosely corresponds to the "unified ID" in the tagging design doc:
    https://zapatacomputing.atlassian.net/wiki/spaces/ORQSRUN/pages/479920161/Logging+Tagging

    Assumed to be globally unique.

    Example value: "wf.multioutput_wf.91aa7aa@invocation-3-task-make-company-name.91e4b"
    """
    inv_id = invocation.id
    hex_str = _id_gen.gen_short_uid(char_length=5)

    return f"{wf_run_id}@{inv_id}.{hex_str}"


if _client.WorkflowStatus is not None:
    RAY_ORQ_STATUS = {
        _client.WorkflowStatus.RUNNING: State.RUNNING,
        _client.WorkflowStatus.CANCELED: State.TERMINATED,
        _client.WorkflowStatus.SUCCESSFUL: State.SUCCEEDED,
        _client.WorkflowStatus.FAILED: State.FAILED,
        _client.WorkflowStatus.RESUMABLE: State.FAILED,
        _client.WorkflowStatus.PENDING: State.WAITING,
    }
else:
    RAY_ORQ_STATUS = {}


def _task_state_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> State:
    """
    Heuristic to figure out SDK task run state from Ray's workflow status and times.

    Note that `wf_status` is status of the whole workflow, but this procedure
    is meant to be applied for tasks.
    """
    if wf_status == _client.WorkflowStatus.RESUMABLE:
        return State.FAILED
    elif wf_status == _client.WorkflowStatus.CANCELED:
        return State.TERMINATED

    if start_time:
        if end_time:
            return State.SUCCEEDED
        elif wf_status == _client.WorkflowStatus.FAILED:
            return State.FAILED

    if not start_time:
        return State.WAITING

    return RAY_ORQ_STATUS[wf_status]


def _workflow_state_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> State:
    if start_time and end_time and wf_status == _client.WorkflowStatus.RUNNING:
        # If we ask Ray right after a workflow has been completed, Ray reports
        # workflow status as "RUNNING". This happens even after we await Ray
        # workflow completion via 'ray.get()'.
        return State.SUCCEEDED

    if not start_time and not end_time and wf_status == _client.WorkflowStatus.RUNNING:
        # In theory this case shouldn't happen, but experience shows that Ray
        # has sometime problems with race conditions in status reporting. This
        # is a defensive 'if' that allows us to be certain that whenever our
        # workflow run is "running", it always has start_time.
        return State.WAITING

    return RAY_ORQ_STATUS[wf_status]


def _task_status_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> RunStatus:
    return RunStatus(
        state=_task_state_from_ray_meta(
            wf_status=wf_status,
            start_time=start_time,
            end_time=end_time,
        ),
        start_time=_instant_from_timestamp(start_time),
        end_time=_instant_from_timestamp(end_time),
    )


def _workflow_status_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> RunStatus:
    return RunStatus(
        state=_workflow_state_from_ray_meta(
            wf_status=wf_status,
            start_time=start_time,
            end_time=end_time,
        ),
        start_time=_instant_from_timestamp(start_time),
        end_time=_instant_from_timestamp(end_time),
    )


def _wrap_single_outputs(
    values: t.Sequence[t.Union[ArtifactValue, t.Tuple[ArtifactValue, ...]]],
    invocations: t.Sequence[ir.TaskInvocation],
) -> t.Sequence[t.Tuple[ArtifactValue, ...]]:
    """
    Ensures all values are tuples. This data shape is required by
    ``RuntimeInterface.get_available_outputs()``.
    """
    wrapped: t.MutableSequence = []
    for task_output, inv in zip(values, invocations):
        if len(inv.output_ids) == 1:
            wrapped.append((task_output,))
        else:
            wrapped.append(task_output)

    return wrapped


@dataclasses.dataclass(frozen=True)
class RayParams:
    """Parameters we pass to Ray. See Ray documentation for reference of what values are
    possible and what they do:
    - https://docs.ray.io/en/latest/package-ref.html#ray-init
    - https://docs.ray.io/en/latest/workflows/package-ref.html#ray.workflow.init
    """

    address: t.Optional[str] = None
    log_to_driver: bool = True
    storage: t.Optional[t.Union[str, _client.Storage]] = None
    _temp_dir: t.Optional[str] = None
    configure_logging: bool = True


# Defensive timeout for Ray operations that are expected to be instant.
# Sometimes, Ray behaves unintuitively.
JUST_IN_CASE_TIMEOUT = 10.0


class RayRuntime(RuntimeInterface):
    def __init__(
        self,
        client: RayClient,
        config: RuntimeConfiguration,
        project_dir: Path,
    ):
        self._client = client
        self._config = config
        self._project_dir = project_dir

        self._log_reader: LogReader = _ray_logs.DirectRayReader(
            _services.ray_temp_path()
        )

    @classmethod
    def from_runtime_configuration(
        cls, project_dir: Path, config: RuntimeConfiguration, verbose: bool
    ) -> "RuntimeInterface":
        client = RayClient()

        ray_params = RayParams(
            address=config.runtime_options["address"],
            log_to_driver=config.runtime_options["log_to_driver"],
            storage=config.runtime_options["storage"],
            _temp_dir=config.runtime_options["temp_dir"],
            configure_logging=config.runtime_options["configure_logging"],
        )
        cls.startup(ray_params)
        return RayRuntime(client=client, config=config, project_dir=project_dir)

    @classmethod
    def startup(cls, ray_params: RayParams):
        """
        Initialize a global Ray connection. If you need a separate connection
        with different params, you need to call .shutdown first(). Globals
        suck.

        This operation is idempotent – calling it multiple times with the same
        arguments has the same effect as calling it once.

        Args:
            ray_params: connection params. Most notable field: 'address' –
                depending on the value, a new Ray cluster will be spawned, or
                we'll just connect to an already existing one.

        Raises:
            exceptions.RayActorNameClashError: when multiple Ray actors exist with the
                same name.
        """

        # Turn off internal Ray logs, unless there is an error
        # If Ray is set to configure logging, this will be overriden
        logger = logging.getLogger("ray")
        logger.setLevel(logging.ERROR)

        client = RayClient()
        client.init(**dataclasses.asdict(ray_params))

        try:
            client.workflow_init()
        except ValueError as e:
            message = (
                "This is likely due to a race condition in starting the local "
                "runtime. Please try again."
            )
            # NOTE: We check for the specific case of an actor name clash before
            # raising RayActorNameClashError. If it's anything else we still want
            # to advise the user that it's a probable race condition.
            if re.findall(
                r"Actor with name|already exists in the namespace workflow",
                str(e),
                re.IGNORECASE,
            ):
                raise exceptions.RayActorNameClashError(message) from e
            else:
                raise ValueError(message) from e

    @classmethod
    def shutdown(cls):
        """Clean up Ray connection. Call it if you want to connect to Ray with different
        parameters.

        Safe to call multiple times in a row.
        """
        client = RayClient()
        client.shutdown()

    def create_workflow_run(self, workflow_def: ir.WorkflowDef) -> WorkflowRunId:
        global_run_id = os.getenv(RAY_GLOBAL_WF_RUN_ID_ENV)
        wf_run_id = global_run_id or _generate_wf_run_id(workflow_def)

        dag = _make_ray_dag(self._client, workflow_def, wf_run_id, self._project_dir)
        wf_user_metadata = WfUserMetadata(workflow_def=workflow_def)

        # Unfortunately, Ray doesn't validate uniqueness of workflow IDs. Let's
        # hope we won't get a collision.
        _ = self._client.run_dag_async(
            dag,
            workflow_id=wf_run_id,
            metadata=_pydatic_to_json_dict(wf_user_metadata),
        )

        config_name: str
        config_name = self._config.config_name
        wf_run = StoredWorkflowRun(
            workflow_run_id=wf_run_id,
            config_name=config_name,
            workflow_def=workflow_def,
        )
        with WorkflowDB.open_project_db(self._project_dir) as db:
            db.save_workflow_run(wf_run)

        return wf_run_id

    def get_all_workflow_runs_status(self) -> t.List[WorkflowRun]:
        all_workflows = self._client.list_all()
        wf_runs = []
        for run_id, _ in all_workflows:
            try:
                wf_runs.append(self.get_workflow_run_status(run_id))
            except exceptions.NotFoundError as e:
                raise e
        return wf_runs

    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError
        """
        try:
            wf_status = self._client.get_workflow_status(workflow_id=workflow_run_id)
            wf_meta = self._client.get_workflow_metadata(workflow_id=workflow_run_id)
        except (_client.workflow_exceptions.WorkflowNotFoundError, ValueError) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run {workflow_run_id} wasn't found"
            ) from e

        wf_user_metadata = WfUserMetadata.parse_obj(wf_meta["user_metadata"])
        wf_def = wf_user_metadata.workflow_def

        inv_ids = wf_def.task_invocations.keys()
        # We assume that:
        # - create_workflow_run() created a separate Ray Task for each IR's
        #   TaskInvocation
        # - each Ray Task's name was set to TaskInvocation.id
        ray_task_metas = [
            self._client.get_task_metadata(workflow_id=workflow_run_id, name=inv_id)
            for inv_id in inv_ids
        ]

        return WorkflowRun(
            id=workflow_run_id,
            workflow_def=wf_def,
            task_runs=[
                TaskRun(
                    id=task_meta["user_metadata"]["task_run_id"],
                    invocation_id=task_meta["user_metadata"]["task_invocation_id"],
                    status=_task_status_from_ray_meta(
                        wf_status=wf_status,
                        start_time=task_meta["stats"].get("start_time"),
                        end_time=task_meta["stats"].get("end_time"),
                    ),
                    message=None,
                )
                for task_meta in ray_task_metas
            ],
            status=_workflow_status_from_ray_meta(
                wf_status=wf_status,
                start_time=wf_meta["stats"].get("start_time"),
                end_time=wf_meta["stats"].get("end_time"),
            ),
        )

    def get_workflow_run_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[t.Any]:
        try:
            return self._client.get_workflow_output(workflow_run_id)
        except ValueError as e:
            raise exceptions.NotFoundError(
                f"Workflow run {workflow_run_id} wasn't found"
            ) from e

    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[t.Any]:
        workflow_status = self.get_workflow_run_status(workflow_run_id)
        workflow_state = workflow_status.status.state
        if workflow_state != State.SUCCEEDED:
            raise exceptions.WorkflowRunNotSucceeded(
                f"{workflow_run_id} has not succeeded. {workflow_state}",
                workflow_state,
            )
        return self.get_workflow_run_outputs(workflow_run_id)

    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Dict[ir.TaskInvocationId, t.Tuple[ArtifactValue, ...]]:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: if no run
                with `workflow_run_id` was found.
        """
        # The approach is based on two steps:
        # 1. Get task run status.
        # 2. Ask Ray for outputs of only the succeeded ones.
        #
        # It would be nice to only ask Ray only once to get all outputs, async,
        # catch errors for the tasks that haven't been finished yet, and return
        # the available outputs. At the time of writing this (ray 2.0.0), it
        # isn't possible. Ray always blocks for workflow completion.
        try:
            wf_run = self.get_workflow_run_status(workflow_run_id)
        except exceptions.WorkflowRunNotFoundError as e:
            # Explicitly re-raise.
            raise e

        # Note: matching task invocations with other objects down below relies on the
        # sequence order.
        succeeded_inv_ids: t.Sequence[ir.TaskInvocationId] = [
            run.invocation_id
            for run in wf_run.task_runs
            if run.status.state == State.SUCCEEDED
        ]

        succeeded_obj_refs: t.List[_client.ObjectRef] = [
            self._client.get_task_output_async(
                workflow_id=workflow_run_id,
                # We rely on the fact that _make_ray_dag() assigns invocation
                # ID as the Ray task name.
                task_id=inv_id,
            )
            for inv_id in succeeded_inv_ids
        ]

        # The values are supposed to be ready to get, but we're using a timeout
        # to ensure we don't block indefinitely.
        succeeded_values: t.Sequence[t.Any] = self._client.get(
            succeeded_obj_refs, timeout=JUST_IN_CASE_TIMEOUT
        )

        # Ray returns a plain value instead of 1-element tuple for 1-output tasks.
        # We need to wrap such outputs in tuples to maintain the same data shape across
        # RuntimeInterface implementations.
        wrapped_values = _wrap_single_outputs(
            values=succeeded_values,
            invocations=[
                wf_run.workflow_def.task_invocations[inv_id]
                for inv_id in succeeded_inv_ids
            ],
        )

        return dict(zip(succeeded_inv_ids, wrapped_values))

    def stop_workflow_run(self, workflow_run_id: WorkflowRunId) -> None:
        # cancel doesn't throw exceptions on non-existing runs... using this as
        # a workaround to inform client of an error
        try:
            _ = self._client.get_workflow_status(workflow_id=workflow_run_id)
        except (_client.workflow_exceptions.WorkflowNotFoundError, ValueError) as e:
            raise exceptions.NotFoundError(
                f"Workflow run {workflow_run_id} wasn't found"
            ) from e
        self._client.cancel(workflow_run_id)

    def get_workflow_logs(self, wf_run_id: WorkflowRunId):
        return self._log_reader.get_workflow_logs(wf_run_id)

    def get_task_logs(self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
        return self._log_reader.get_task_logs(wf_run_id, task_inv_id)

    def list_workflow_runs(
        self,
        *,
        limit: t.Optional[int] = None,
        max_age: t.Optional[timedelta] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
    ) -> t.List[WorkflowRun]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            status: Only return runs of runs with the specified status.

        Returns:
                A list of the workflow runs
        """
        now = datetime.now(timezone.utc)

        if state is not None:
            if not isinstance(state, list):
                state_list = [state]
            else:
                state_list = state
        else:
            state_list = None

        # Grab the workflows we know about from the DB
        all_workflows = self._client.list_all()

        wf_runs = []
        for wf_run_id, _ in all_workflows:
            try:
                wf_run = self.get_workflow_run_status(wf_run_id)
            except exceptions.NotFoundError:
                continue

            # Let's filter the workflows at this point, instead of iterating over a list
            # multiple times
            if state_list is not None and wf_run.status.state not in state_list:
                continue
            if max_age is not None and (
                now - (wf_run.status.start_time or now) > max_age
            ):
                continue
            wf_runs.append(wf_run)

        # We have to wait until we have all the workflow runs before sorting
        if limit is not None:
            wf_runs = sorted(wf_runs, key=lambda run: run.status.start_time or now)[
                -limit:
            ]
        return wf_runs


def get_current_ids() -> t.Tuple[
    t.Optional[WorkflowRunId], t.Optional[TaskInvocationId], t.Optional[TaskRunId]
]:
    """
    Uses Ray context to figure out what are the IDs of the currently running workflow
    and task.

    The returned TaskInvocationID and TaskRunID are None if we weren't able to get them
    from current Ray context.
    """
    # NOTE: this is tightly coupled with how we create Ray workflow DAG, how we assign
    # IDs and metadata.
    client = _client.RayClient()

    try:
        wf_run_id = client.get_current_workflow_id()
    except AssertionError:
        # Ray has an 'assert' about checking the workflow context outside of a workflow
        return None, None, None

    # We don't need to care what kind of ID it is, we only need it to get the metadata
    # dict.
    ray_task_name = client.get_current_task_id()
    task_meta: dict = client.get_task_metadata(
        workflow_id=wf_run_id, name=ray_task_name
    )

    try:
        user_meta = InvUserMetadata.parse_obj(task_meta.get("user_metadata"))
    except pydantic.ValidationError:
        # This ray task wasn't annotated with InvUserMetadata. It happens when
        # `get_current_ids()` is used from a context that's not a regular Orquestra Task
        # run. One example is the one-off task that we use to construct Ray DAG inside a
        # Ray worker process.
        return wf_run_id, None, None

    return wf_run_id, user_meta.task_invocation_id, user_meta.task_run_id
