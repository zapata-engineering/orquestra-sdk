################################################################################
# © Copyright 2023 - 2024 Zapata Computing Inc.
################################################################################
"""Translates IR workflow def into a Ray workflow."""
import os
import re
import time
import typing as t
import warnings
from functools import singledispatch
from pathlib import Path

import pydantic
from packaging import version
from typing_extensions import assert_never

from orquestra.sdk.packaging import get_installed_version

from .. import exceptions, secrets
from .._base import _exec_ctx, _git_url_utils, _graphs, _services, dispatch, serde
from .._base._env import (
    RAY_DOWNLOAD_GIT_IMPORTS_ENV,
    RAY_SET_CUSTOM_IMAGE_RESOURCES_ENV,
)
from .._base._logs import _markers
from .._base._regex import SEMVER_REGEX
from ..kubernetes.quantity import parse_quantity
from ..schema import ir, responses, workflow_run
from . import _client, _id_gen
from ._wf_metadata import InvUserMetadata, pydatic_to_json_dict

DEFAULT_IMAGE_TEMPLATE = "hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:{}"


def _get_default_image(template: str, sdk_version: str, num_gpus: t.Optional[int]):
    image = template.format(sdk_version)
    if num_gpus is not None and num_gpus > 0:
        image = f"{image}-cuda"
    return image


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
    """Extract the underlying user functions from a 'fn_ref'.

    Dereferences 'fn_ref', loads the module attribute, and extracts the underlying user
    function if it was a TaskDef.
    """
    obj: t.Any = dispatch.locate_fn_ref(fn_ref)

    # dsl.task() wraps a callable in a TaskDef object. We need to locate the
    # underlying user function, not the Task object.
    try:
        return obj._TaskDef__sdk_task_body
    except AttributeError:
        return obj


def _aggregate_outputs(*args):
    if len(args) == 1:
        return args[0]
    return args


class TaskResult(t.NamedTuple):
    packed: responses.WorkflowResult
    unpacked: t.Tuple[responses.WorkflowResult, ...]


class ArgumentUnwrapper:
    """Unwraps arguments (constants, secrets, artifacts) before passing to a task."""

    def __init__(
        self,
        user_fn: t.Callable,
        args_artifact_nodes: t.Mapping[int, ir.ArtifactNode],
        kwargs_artifact_nodes: t.Mapping[str, ir.ArtifactNode],
        deserialize: bool,
    ):
        """Initilaiser for the ArgumentUnwrapper.

        Args:
            user_fn: the task function to wrap.
            args_artifact_nodes: A map of positional argument index to workflow artifact
                node.
                This is to assist with unpacking artifact results.
                Not all positional arguments will be included, i.e. a constant or secret
            kwargs_artifact_nodes: A map of keyword argument name to workflow artifact
                node.
                This is to assist with unpacking artifact results.
                Not all positional arguments will be included, i.e. a constant or secret
            deserialize: If true, we will first deserialize the argument before passing
                to the task function.
                If false, the serialized argument is passed to the underlying task
                function.
                This is used to avoid the deserialization problem of having the Python
                dependencies for Pickles when aggregating the outputs of a workflow.
        """
        self._user_fn = user_fn
        self._args_artifact_nodes = args_artifact_nodes
        self._kwargs_artifact_nodes = kwargs_artifact_nodes
        self._deserialize = deserialize

    def _get_metadata(self, key: t.Union[int, str]) -> t.Optional[ir.ArtifactNode]:
        if isinstance(key, int):
            return self._args_artifact_nodes.get(key)
        elif isinstance(key, str):
            return self._kwargs_artifact_nodes.get(key)
        else:
            assert_never(key)

    def _unpack_argument(
        self,
        arg: t.Union[ir.ConstantNode, ir.SecretNode, TaskResult],
        meta_key: t.Union[int, str],
    ):
        if isinstance(arg, (ir.ConstantNodeJSON, ir.ConstantNodePickle)):
            return serde.deserialize(arg) if self._deserialize else arg
        elif isinstance(arg, ir.SecretNode):
            return (
                secrets.get(
                    arg.secret_name,
                    config_name=arg.secret_config,
                    workspace_id=arg.workspace_id,
                )
                if self._deserialize
                else arg
            )
        elif isinstance(arg, TaskResult):
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


SENTINEL = "dry_run task output"


def _generate_nop_function(output_metadata: ir.TaskOutputMetadata):
    def nop(*_, **__):
        if not output_metadata.is_subscriptable:
            return SENTINEL
        else:
            return (SENTINEL,) * output_metadata.n_outputs

    return nop


def _get_user_function(
    user_fn_ref, dry_run, output_metadata: t.Optional[ir.TaskOutputMetadata]
):
    # Ref is None only in case of the data aggregation step
    if user_fn_ref is None:
        return _aggregate_outputs
    elif not dry_run:
        return _locate_user_fn(user_fn_ref)
    else:
        assert output_metadata is not None
        return _generate_nop_function(output_metadata)


def _make_ray_dag_node(
    client: _client.RayClient,
    ray_options: t.Mapping,
    ray_args: t.Iterable[t.Any],
    ray_kwargs: t.Mapping[str, t.Any],
    args_artifact_nodes: t.Mapping,
    kwargs_artifact_nodes: t.Mapping,
    user_fn_ref: t.Optional[ir.FunctionRef],
    output_metadata: t.Optional[ir.TaskOutputMetadata],
    dry_run: bool,
) -> _client.FunctionNode:
    """Prepares a Ray task that fits a single ir.TaskInvocation.

    The result is a node in a Ray DAG.

    Args:
        client: Ray API facade.
        ray_options: dict passed to _client.RayClient.add_options().
        ray_args: constants or futures required to build the DAG.
        ray_kwargs: constants or futures required to build the DAG.
        args_artifact_nodes: a map of positional arg index to artifact node
            see ArgumentUnwrapper.
        kwargs_artifact_nodes: a map of keyword arg name to artifact node
            see ArgumentUnwrapper.
        user_fn_ref: function reference for a function to be executed by Ray.
            if None - executes data aggregation step.
        output_metadata: output metadata for the user task function.
            Keeps number of outputs and if the output is subscriptable.
        dry_run: Run the task without actually executing any user code.
            Useful for testing infrastructure, dependency imports, etc.
    """
    current_path = Path.cwd()

    @client.remote
    def _ray_remote(*inner_args, **inner_kwargs):
        with _exec_ctx.ray():
            # We need to emit task start marker log as soon as possible. Otherwise, we
            # risk an exception won't be visible to the user.
            #
            # TODO: make the IDs required and raise an error if they're not present.
            # https://zapatacomputing.atlassian.net/browse/ORQSDK-530
            wf_run_id, task_inv_id, _ = get_current_ids()
            with _markers.capture_logs(
                logs_dir=_services.redirected_logs_dir(),
                wf_run_id=wf_run_id,
                task_inv_id=task_inv_id,
            ):
                dispatch.ensure_sys_paths([str(current_path)])

                # True for all the steps except data aggregation
                serialization = user_fn_ref is not None

                # Try-except block covers _get_user_function and wrapped() because:
                # _get_user_function un-pickles the inlineImported functions - and this
                # operation can already cause exceptions
                # wrapped() calls user function, which catches all the exceptions
                # that happens within user code
                try:
                    user_fn = _get_user_function(user_fn_ref, dry_run, output_metadata)

                    wrapped = ArgumentUnwrapper(
                        user_fn=user_fn,
                        args_artifact_nodes=args_artifact_nodes,
                        kwargs_artifact_nodes=kwargs_artifact_nodes,
                        deserialize=serialization,
                    )

                    wrapped_return = wrapped(*inner_args, **inner_kwargs)
                except Exception as e:
                    assert wf_run_id is not None
                    assert task_inv_id is not None
                    _client.save_task_postrun_metadata(
                        wf_run_id,
                        task_inv_id,
                        {"end_time": time.time(), "failed": True},
                    )

                    raise exceptions.UserTaskFailedError(
                        f"User task with task invocation id:{task_inv_id} failed.",
                        wf_run_id if wf_run_id else "",
                        task_inv_id if task_inv_id else "",
                    ) from e

                packed: responses.WorkflowResult = (
                    serde.result_from_artifact(wrapped_return, ir.ArtifactFormat.AUTO)
                    if serialization
                    else wrapped_return
                )
                unpacked: t.Tuple[responses.WorkflowResult, ...]

                if output_metadata is not None and output_metadata.n_outputs > 1:
                    unpacked = tuple(
                        (
                            serde.result_from_artifact(
                                wrapped_return[i], ir.ArtifactFormat.AUTO
                            )
                            if serialization
                            else wrapped_return[i]
                        )
                        for i in range(output_metadata.n_outputs)
                    )
                else:
                    unpacked = (packed,)
                return TaskResult(
                    packed=packed,
                    unpacked=unpacked,
                )

    named_remote = client.add_options(_ray_remote, **ray_options)
    dag_node = named_remote.bind(*ray_args, **ray_kwargs)

    return dag_node


def _gen_task_run_id(wf_run_id: str, invocation: ir.TaskInvocation):
    """
    Loosely corresponds to the "unified ID" in the tagging design doc:
    https://zapatacomputing.atlassian.net/wiki/spaces/ORQSRUN/pages/479920161/Logging+Tagging.

    Assumed to be globally unique.

    Example value: "wf.multioutput_wf.91aa7aa@invocation-3-task-make-company-name.91e4b"
    """  # noqa: D205, D212
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


def _import_pip_env(
    ir_invocation: ir.TaskInvocation,
    wf: ir.WorkflowDef,
    imports_pip_string: t.Dict[ir.ImportId, t.List[str]],
) -> t.List[str]:
    """Gather a list of python imports required for the task.

    Args:
        ir_invocation: The task invocation to be executed in this environment.
        wf: The overall workflow definition.
        imports_pip_string: a mapping between the ID and the list of pip strings for a
            specific import

    Returns:
        A list consisting of the python imports declared in the task definition, and the
            current Orquestra SDK version. The latter is included to prevent tasks from
            executing with different SDK versions to the head node.
    """
    task_def = wf.tasks[ir_invocation.task_id]
    imports = [
        wf.imports[id_]
        for id_ in (
            task_def.source_import_id,
            *(task_def.dependency_import_ids or []),
        )
    ]

    current_sdk_version: str = get_installed_version("orquestra-sdk")

    sdk_dependency = None
    pip_list = [
        chunk
        for imp in imports
        for chunk in imports_pip_string[imp.id]
        if not (sdk_dependency := re.match(r"^orquestra-sdk([<|!|=|>|~].*)?$", chunk))
    ]

    # If the task definition includes the SDK, warn the user that this does nothing.
    if sdk_dependency:
        warnings.warn(
            f"The definition for task `{ir_invocation.task_id}` "
            f"declares `{sdk_dependency[0]}` as a dependency. "
            "The current SDK version "
            + (f"({current_sdk_version}) " if current_sdk_version else "")
            + "is automatically installed in task environments. "
            "The specified dependency will be ignored.",
            exceptions.OrquestraSDKVersionMismatchWarning,
        )

    # Don't add sdk dependency if submitting from a prerelease or dev version.
    parsed_sdk_version = version.parse(current_sdk_version)
    if not (parsed_sdk_version.is_devrelease or parsed_sdk_version.is_prerelease):
        pip_list += [f"orquestra-sdk=={current_sdk_version}"]

    return pip_list


def _normalise_prerelease_version(version: str) -> t.Optional[str]:
    """Remove prerelease version information from the version string."""
    match = re.match(SEMVER_REGEX, version)
    assert match, f"Version {version} did not parse as valid SemVer."
    vernums = [int(match.group("major")), int(match.group("minor"))]
    if match.group("patch"):
        vernums.append(int(match.group("patch")))

    # If the version is a prerelease, go back to the previous iteration.
    if match.group("prerelease"):
        vernums[-1] -= 1

    # Safety hatch - returning a version less than 0.1 is nonsensical.
    if vernums[0:2] == [0, 0]:
        return None

    return ".".join([str(vernum) for vernum in vernums])


def _gather_args(arg_ids, workflow_def, ray_futures):
    ray_args = []
    ray_args_artifact_nodes: t.Dict[int, t.Optional[ir.ArtifactNode]] = {}
    for i, arg_id in enumerate(arg_ids):
        ir_node = _arg_from_graph(arg_id, workflow_def)
        if isinstance(ir_node, ir.ArtifactNode):
            ray_args.append(ray_futures[arg_id])
            ray_args_artifact_nodes[i] = ir_node
        else:
            ray_args.append(ir_node)
            ray_args_artifact_nodes[i] = None

    return tuple(ray_args), ray_args_artifact_nodes


def _gather_kwargs(kwargs, workflow_def, ray_futures):
    ray_kwargs = {}
    ray_kwargs_artifact_nodes: t.Dict[str, t.Optional[ir.ArtifactNode]] = {}
    for name, kwarg_id in kwargs.items():
        ir_node = _arg_from_graph(kwarg_id, workflow_def)
        if isinstance(ir_node, ir.ArtifactNode):
            ray_kwargs[name] = ray_futures[kwarg_id]
            ray_kwargs_artifact_nodes[name] = ir_node
        else:
            ray_kwargs[name] = ir_node
            ray_kwargs_artifact_nodes[name] = None

    return ray_kwargs, ray_kwargs_artifact_nodes


def _ray_resources_for_custom_image(image_name: str) -> t.Mapping[str, float]:
    """
    Custom Ray resources we set to power running Orquestra tasks on custom Docker
    images.
    The values are coupled with Compute Engine server-side set up.
    """  # noqa: D205, D212
    # The format for custom image strings is described in the ADR:
    # https://zapatacomputing.atlassian.net/wiki/spaces/ORQSRUN/pages/688259073/2023-05-05+Ray+resources+syntax+for+custom+images
    return {f"image:{image_name}": 1}


def make_ray_dag(
    client: _client.RayClient,
    workflow_def: ir.WorkflowDef,
    workflow_run_id: workflow_run.WorkflowRunId,
    dry_run: bool,
):
    # a mapping of "artifact ID" <-> "the ray Future needed to get the value"
    ray_futures: t.Dict[ir.ArtifactNodeId, t.Any] = {}

    # this resolves all imports to their final "pip string" once,
    # instead of per-task invocation
    imports_pip_strings = {
        id_: _pip_string(imp) for id_, imp in workflow_def.imports.items()
    }

    for invocation in _graphs.iter_invocations_topologically(workflow_def):
        user_task = workflow_def.tasks[invocation.task_id]
        pos_args, pos_args_artifact_nodes = _gather_args(
            invocation.args_ids, workflow_def, ray_futures
        )
        kwargs, kwargs_artifact_nodes = _gather_kwargs(
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

        pip = _import_pip_env(invocation, workflow_def, imports_pip_strings)

        ray_options = {
            # We're using task invocation ID as the Ray "task ID" instead of task run ID
            # because it's easier to query this way. Use the "user_metadata" to get both
            # identifiers.
            "name": invocation.id,
            "metadata": pydatic_to_json_dict(inv_metadata),
            # If there are any python packages to install for step - set runtime env
            "runtime_env": (_client.RuntimeEnv(pip=pip) if len(pip) > 0 else None),
            "catch_exceptions": False,
            # We only want to execute workflow tasks once by default.
            # This is so there is only one task run ID per task, for scenarios where
            # this is used (like in MLflow). We allow setting this variable on
            # task-level for some particular edge-cases like memory leaks inside
            # 3rd party libraries - so in case of the OOMKilled worker it can be
            # restarted.
            # By default, Ray will only retry tasks that fail due to a "system error".
            # For example, if the worker process crashes or exits early.
            # Normal Python exceptions are NOT retried.
            "max_retries": user_task.max_retries if user_task.max_retries else 0,
        }

        # Non-custom task resources
        if invocation.resources is not None:
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
                gpu = int(float(invocation.resources.gpu))
                ray_options["num_gpus"] = gpu

        # Set custom image
        if os.getenv(RAY_SET_CUSTOM_IMAGE_RESOURCES_ENV) is not None:
            # This makes an assumption that only "new" IRs will get to this point
            assert workflow_def.metadata is not None, "Expected a >=0.45.0 IR"
            sdk_version = workflow_def.metadata.sdk_version.original

            # Custom "Ray resources" request. The entries need to correspond to the ones
            # used when starting the Ray cluster. See also:
            # https://docs.ray.io/en/latest/ray-core/scheduling/resources.html#custom-resources
            ray_options["resources"] = _ray_resources_for_custom_image(
                invocation.custom_image
                or user_task.custom_image
                or _get_default_image(
                    DEFAULT_IMAGE_TEMPLATE, sdk_version, ray_options.get("num_gpus")
                )
            )

        ray_result = _make_ray_dag_node(
            client=client,
            ray_options=ray_options,
            ray_args=pos_args,
            ray_kwargs=kwargs,
            args_artifact_nodes=pos_args_artifact_nodes,
            kwargs_artifact_nodes=kwargs_artifact_nodes,
            user_fn_ref=user_task.fn_ref,
            output_metadata=user_task.output_metadata,
            dry_run=dry_run,
        )

        for output_id in invocation.output_ids:
            ray_futures[output_id] = ray_result

    # Gather futures for the last, fake task, and decide what args we need to unwrap.
    pos_args, pos_args_artifact_nodes = _gather_args(
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
            # Set to avoid retrying when the worker crashes.
            # See the comment with the invocation's options for more details.
            "max_retries": 0,
            # Custom "Ray resources" request. We don't need any for the aggregation
            # step.
            "resources": None,
        },
        ray_args=pos_args,
        ray_kwargs={},
        args_artifact_nodes=pos_args_artifact_nodes,
        kwargs_artifact_nodes={},
        user_fn_ref=None,
        output_metadata=ir.TaskOutputMetadata(
            n_outputs=len(pos_args), is_subscriptable=False
        ),
        dry_run=False,
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


def get_current_ids() -> (
    t.Tuple[
        t.Optional[workflow_run.WorkflowRunId],
        t.Optional[ir.TaskInvocationId],
        t.Optional[workflow_run.TaskRunId],
    ]
):
    """Use Ray context to figure out the IDs of the currently running workflow and task.

    The returned TaskInvocationID and TaskRunID are None if we weren't able to get them
    from the current Ray context.
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
        user_meta = InvUserMetadata.model_validate(task_meta.get("user_metadata"))
    except pydantic.ValidationError:
        # This ray task wasn't annotated with InvUserMetadata. It happens when
        # `get_current_ids()` is used from a context that's not a regular Orquestra Task
        # run. One example is the one-off task that we use to construct Ray DAG inside a
        # Ray worker process.
        return wf_run_id, None, None

    return wf_run_id, user_meta.task_invocation_id, user_meta.task_run_id
