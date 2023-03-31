import os
import traceback
import typing as t
from functools import singledispatch
from pathlib import Path

from typing_extensions import assert_never

from .. import exceptions, secrets
from .._base import _exec_ctx, _git_url_utils, _graphs, _log_adapter, dispatch, serde
from .._base._env import RAY_DOWNLOAD_GIT_IMPORTS_ENV, RAY_SET_TASK_RESOURCES_ENV
from ..kubernetes.quantity import parse_quantity
from ..schema import ir, responses, workflow_run
from . import _client, _id_gen
from ._client import RayClient
from ._wf_metadata import InvUserMetadata, pydatic_to_json_dict


def _arg_from_graph(argument_id: ir.ArgumentId, workflow_def: ir.WorkflowDef):
    try:
        return workflow_def.constant_nodes[argument_id]
    except KeyError:
        pass

    try:
        return workflow_def.secret_nodes[argument_id]
    except KeyError:
        pass

    # If we reach this point, we MUST be an artifact.
    # Any exception raised here (i.e. KeyError) means something major went wrong.
    return workflow_def.artifact_nodes[argument_id]


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


def _aggregate_outputs(*args):
    return args


class _TaskResult(t.NamedTuple):
    packed: responses.WorkflowResult
    unpacked: t.Tuple[responses.WorkflowResult, ...]


class ArgumentUnwrapper:
    def __init__(
        self,
        user_fn: t.Callable,
        args_metadata: t.Mapping[int, ir.ArtifactNode],
        kwargs_metadata: t.Mapping[str, ir.ArtifactNode],
        deserialize: bool,
    ):
        self._user_fn = user_fn
        self._args_metadata = args_metadata
        self._kwargs_metadata = kwargs_metadata
        self._deserialize = deserialize

    def _get_metadata(self, key: t.Union[int, str]) -> t.Optional[ir.ArtifactNode]:
        if isinstance(key, int):
            return self._args_metadata.get(key)
        elif isinstance(key, str):
            return self._kwargs_metadata.get(key)
        else:
            assert_never(key)

    def _unpack_argument(
        self,
        arg: t.Union[ir.ConstantNode, ir.SecretNode, _TaskResult],
        meta_key: t.Union[int, str],
    ):
        if isinstance(arg, (ir.ConstantNodeJSON, ir.ConstantNodePickle)):
            return serde.deserialize(arg) if self._deserialize else arg
        elif isinstance(arg, ir.SecretNode):
            return (
                secrets.get(arg.secret_name, config_name=arg.secret_config)
                if self._deserialize
                else arg
            )
        elif isinstance(arg, _TaskResult):
            meta = self._get_metadata(meta_key)
            if meta is None or meta.artifact_index is None:
                return (
                    serde.deserialize(arg.packed) if self._deserialize else arg.packed
                )
            else:
                return (
                    serde.deserialize(arg.unpacked[meta.artifact_index])
                    if self._deserialize
                    else arg.unpacked[meta.artifact_index]
                )
        else:
            assert_never(arg)

    def __call__(self, *wrapped_args, **wrapped_kwargs):
        args = []
        kwargs = {}

        for i, arg in enumerate(wrapped_args):
            args.append(self._unpack_argument(arg, i))

        for name, kwarg in wrapped_kwargs.items():
            kwargs[name] = self._unpack_argument(kwarg, name)

        return self._user_fn(*args, **kwargs)


def _make_ray_dag_node(
    client: RayClient,
    ray_options: t.Mapping,
    ray_args: t.Iterable[t.Any],
    ray_kwargs: t.Mapping[str, t.Any],
    args_metadata: t.Mapping,
    kwargs_metadata: t.Mapping,
    n_outputs: t.Optional[int],
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
        user_fn_ref: function reference for a function to be executed by Ray.
            if None - executes data aggregation step
    """

    @client.remote
    def _ray_remote(*inner_args, **inner_kwargs):
        if project_dir is not None:
            dispatch.ensure_sys_paths([str(project_dir)])

        if user_fn_ref is None:
            serialization = False
            user_fn = _aggregate_outputs
        else:
            serialization = True
            user_fn = _locate_user_fn(user_fn_ref)

        wrapped = ArgumentUnwrapper(
            user_fn=user_fn,
            args_metadata=args_metadata,
            kwargs_metadata=kwargs_metadata,
            deserialize=serialization,
        )

        logger = _log_adapter.workflow_logger()
        try:
            with _exec_ctx.ray():
                wrapped_return = wrapped(*inner_args, **inner_kwargs)

                packed: responses.WorkflowResult = (
                    serde.result_from_artifact(wrapped_return, ir.ArtifactFormat.AUTO)
                    if serialization
                    else wrapped_return
                )
                unpacked: t.Tuple[responses.WorkflowResult, ...]

                if n_outputs is not None and n_outputs > 1:
                    unpacked = tuple(
                        serde.result_from_artifact(
                            wrapped_return[i], ir.ArtifactFormat.AUTO
                        )
                        if serialization
                        else wrapped_return[i]
                        for i in range(n_outputs)
                    )
                else:
                    unpacked = (packed,)

                return _TaskResult(
                    packed=packed,
                    unpacked=unpacked,
                )
        except Exception as e:
            # Log the stacktrace as a single log line.
            logger.exception(traceback.format_exc())

            # We need to stop further execution of this workflow. If we don't raise, Ray
            # will think the task succeeded with a return value `None`.
            raise e

    named_remote = client.add_options(_ray_remote, **ray_options)
    dag_node = named_remote.bind(*ray_args, **ray_kwargs)

    return dag_node


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


def _gather_args(arg_ids, workflow_def, ray_futures):
    ray_args = []
    ray_args_metadata: t.Dict[int, t.Optional[ir.ArtifactNode]] = {}
    for i, arg_id in enumerate(arg_ids):
        ir_node = _arg_from_graph(arg_id, workflow_def)
        if isinstance(ir_node, ir.ArtifactNode):
            ray_args.append(ray_futures[arg_id])
            ray_args_metadata[i] = ir_node
        else:
            ray_args.append(ir_node)
            ray_args_metadata[i] = None

    return tuple(ray_args), ray_args_metadata


def _gather_kwargs(kwargs, workflow_def, ray_futures):
    ray_kwargs = {}
    ray_kwargs_metadata: t.Dict[str, t.Optional[ir.ArtifactNode]] = {}
    for name, kwarg_id in kwargs.items():
        ir_node = _arg_from_graph(kwarg_id, workflow_def)
        if isinstance(ir_node, ir.ArtifactNode):
            ray_kwargs[name] = ray_futures[kwarg_id]
            ray_kwargs_metadata[name] = ir_node
        else:
            ray_kwargs[name] = ir_node
            ray_kwargs_metadata[name] = None

    return ray_kwargs, ray_kwargs_metadata


def make_ray_dag(
    client: RayClient,
    workflow_def: ir.WorkflowDef,
    workflow_run_id: workflow_run.WorkflowRunId,
    project_dir: t.Optional[Path] = None,
):
    # a mapping of "artifact ID" <-> "the ray Future needed to get the value"
    ray_futures: t.Dict[ir.ArtifactNodeId, t.Any] = {}

    # Environment variable is used to configure if we apply task invocation resources
    # to a Ray remote's options
    add_resources = os.getenv(RAY_SET_TASK_RESOURCES_ENV) == "1"

    for invocation in _graphs.iter_invocations_topologically(workflow_def):
        user_task = workflow_def.tasks[invocation.task_id]
        pos_args, pos_args_metadata = _gather_args(
            invocation.args_ids, workflow_def, ray_futures
        )
        kwargs, kwargs_metadata = _gather_kwargs(
            invocation.kwargs_ids, workflow_def, ray_futures
        )
        # We want to store both the TaskInvocation.id and TaskRun.id. We use
        # TaskInvocation.id to refer to Ray tasks later. Solution: Ray task
        # name for identification and Ray metadata for anything else.
        inv_metadata = InvUserMetadata(
            task_run_id=_gen_task_run_id(
                wf_run_id=workflow_run_id, invocation=invocation
            ),
            task_invocation_id=invocation.id,
        )

        pip = _import_pip_env(invocation, workflow_def)

        ray_options = {
            # We're using task invocation ID as the Ray "task ID" instead of task run ID
            # because it's easier to query this way. Use the "user_metadata" to get both
            # identifiers.
            "name": invocation.id,
            "metadata": pydatic_to_json_dict(inv_metadata),
            # If there are any python packages to install for step - set runtime env
            "runtime_env": (_client.RuntimeEnv(pip=pip) if len(pip) > 0 else None),
            "catch_exceptions": False,
        }

        # Task resources
        if add_resources and invocation.resources is not None:
            if invocation.resources.cpu is not None:
                cpu = parse_quantity(invocation.resources.cpu)
                cpu_int = cpu.to_integral_value()
                ray_options["num_cpus"] = int(cpu_int) if cpu == cpu_int else float(cpu)
            if invocation.resources.memory is not None:
                memory = parse_quantity(invocation.resources.memory)
                memory_int = memory.to_integral_value()
                ray_options["memory"] = (
                    int(memory_int) if memory == memory_int else float(memory)
                )
            if invocation.resources.gpu is not None:
                # Fractional GPUs not supported currently
                gpu = int(invocation.resources.gpu)
                ray_options["num_gpus"] = gpu

        ray_result = _make_ray_dag_node(
            client=client,
            ray_options=ray_options,
            ray_args=pos_args,
            ray_kwargs=kwargs,
            args_metadata=pos_args_metadata,
            kwargs_metadata=kwargs_metadata,
            n_outputs=user_task.output_metadata.n_outputs,
            project_dir=project_dir,
            user_fn_ref=user_task.fn_ref,
        )

        for output_id in invocation.output_ids:
            ray_futures[output_id] = ray_result

    # Gather futures for the last, fake task, and decide what args we need to unwrap.
    pos_args, pos_args_metadata = _gather_args(
        workflow_def.output_ids, workflow_def, ray_futures
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
        ray_args=pos_args,
        ray_kwargs={},
        args_metadata=pos_args_metadata,
        kwargs_metadata={},
        n_outputs=len(pos_args),
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
                ) from err
            else:
                raise err
        else:
            return result[0]

    return handle_data_aggregation_error.bind(last_future)
