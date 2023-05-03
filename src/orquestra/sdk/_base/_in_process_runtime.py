################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import typing as t
import warnings
from datetime import datetime, timedelta, timezone

from orquestra.sdk import ProjectRef, exceptions
from orquestra.sdk._base import abc
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.responses import WorkflowResult
from orquestra.sdk.schema.workflow_run import RunStatus, State, TaskRun, WorkflowRun

from .. import secrets
from . import serde
from ._graphs import iter_invocations_topologically
from .dispatch import locate_fn_ref

WfRunId = str
ArtifactValue = t.Any
TaskOutputs = t.Tuple[ArtifactValue, ...]


def _make_completed_task_run(workflow_run_id, start_time, end_time, task_inv):
    return TaskRun(
        id=f"{workflow_run_id}-{task_inv}",
        invocation_id=task_inv,
        status=RunStatus(
            state=State.SUCCEEDED,
            start_time=start_time,
            end_time=end_time,
        ),
    )


def _get_args(
    consts: t.Dict[ir.ConstantNodeId, t.Any],
    artifact_store: t.Dict[ir.ArtifactNodeId, ArtifactValue],
    args_ids: t.List[ir.ArgumentId],
) -> t.List[t.Any]:
    args = []
    for arg_id in args_ids:
        try:
            args.append(artifact_store[arg_id])
        except KeyError:
            args.append(consts[arg_id])
    return args


def _get_kwargs(
    consts: t.Dict[ir.ConstantNodeId, t.Any],
    artifact_store: t.Dict[ir.ArtifactNodeId, ArtifactValue],
    kwargs_ids: t.Dict[ir.ParameterName, ir.ArgumentId],
) -> t.Dict[ir.ParameterName, ArtifactValue]:
    kwargs = {}
    for name, arg_id in kwargs_ids.items():
        try:
            kwargs[name] = artifact_store[arg_id]
        except KeyError:
            kwargs[name] = consts[arg_id]
    return kwargs


class InProcessRuntime(abc.RuntimeInterface):
    """
    Result of calling workflow function directly. Empty at first. Filled each
    time `create_workflow_run` is called.

    Implements orquestra.sdk._base.abc.RuntimeInterface methods.
    """

    def __init__(self):
        self._output_store: t.Dict[WfRunId, TaskOutputs] = {}
        self._artifact_store: t.Dict[
            WfRunId, t.Dict[ir.ArtifactNodeId, ArtifactValue]
        ] = {}
        self._workflow_def_store: t.Dict[WfRunId, ir.WorkflowDef] = {}
        self._start_time_store: t.Dict[WfRunId, datetime] = {}
        self._end_time_store: t.Dict[WfRunId, datetime] = {}

    def _gen_next_run_id(self, wf_def: ir.WorkflowDef):
        return f"{wf_def.name}-{len(self._output_store) + 1}"

    def create_workflow_run(
        self, workflow_def: ir.WorkflowDef, project: t.Optional[ProjectRef]
    ) -> WfRunId:
        if project:
            warnings.warn(
                "in_process runtime doesn't support project-scoped workflows. "
                "Project and workspace IDs will be ignored.",
                category=exceptions.UnsupportedRuntimeFeature,
            )

        run_id = self._gen_next_run_id(workflow_def)

        self._start_time_store[run_id] = datetime.now(timezone.utc)

        # We deserialize the constants in one go, instead of as needed
        consts: t.Dict[ir.ConstantNodeId, t.Any] = {
            id: serde.deserialize(node)
            for id, node in workflow_def.constant_nodes.items()
        }
        for id, secret in workflow_def.secret_nodes.items():
            consts[id] = secrets.get(
                secret.secret_name, config_name=secret.secret_config
            )
        # We'll store artifacts for this run here.
        self._artifact_store[run_id] = {}

        # We are going to iterate over the workflow graph and execute each task
        # invocation sequentially, after topologically sorting the graph
        for task_inv in iter_invocations_topologically(workflow_def):
            # We can get the task function, args and kwargs from the task invocation
            task_fn: t.Any = locate_fn_ref(workflow_def.tasks[task_inv.task_id].fn_ref)
            args = _get_args(consts, self._artifact_store[run_id], task_inv.args_ids)
            kwargs = _get_kwargs(
                consts, self._artifact_store[run_id], task_inv.kwargs_ids
            )

            # Next, the task is executed with the args/kwargs
            try:
                fn = task_fn._TaskDef__sdk_task_body
            except AttributeError:
                fn = task_fn
            fn_output = fn(*args, **kwargs)

            # Finally, we need to dereference the output IDs
            for artifact_id in task_inv.output_ids:
                artifact = workflow_def.artifact_nodes[artifact_id]
                if artifact.artifact_index is None:
                    self._artifact_store[run_id][artifact_id] = fn_output
                else:
                    self._artifact_store[run_id][artifact_id] = fn_output[
                        artifact.artifact_index
                    ]

        # Ordinary functions return `obj` or `tuple(obj, obj)`
        outputs = tuple(
            _get_args(consts, self._artifact_store[run_id], workflow_def.output_ids)
        )
        self._output_store[run_id] = outputs

        self._end_time_store[run_id] = datetime.now(timezone.utc)
        self._workflow_def_store[run_id] = workflow_def
        return run_id

    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WfRunId
    ) -> t.Tuple[WorkflowResult, ...]:
        return (
            *(
                serde.result_from_artifact(output, ir.ArtifactFormat.AUTO)
                for output in self._output_store[workflow_run_id]
            ),
        )

    def get_available_outputs(
        self, workflow_run_id: WfRunId
    ) -> t.Dict[ir.TaskInvocationId, WorkflowResult]:
        wf_def = self._workflow_def_store[workflow_run_id]

        inv_outputs: t.Dict[ir.TaskInvocationId, WorkflowResult] = {}
        for inv in wf_def.task_invocations.values():
            # Assumption there's always a non-unpacked artifact. We want to return
            # whatever shape was returned from the task function so we can use the
            # "packed" artifact. For more info on artifact unpacking, see
            # "orquestra.sdk._base._traversal".

            artifact_nodes = [wf_def.artifact_nodes[id] for id in inv.output_ids]
            packed_nodes = [n for n in artifact_nodes if n.artifact_index is None]
            assert (
                len(packed_nodes) == 1
            ), f"Task invocation should have exactly 1 packed output. {inv.id} has {len(packed_nodes)}: {packed_nodes}"  # noqa: E501
            packed_artifact = packed_nodes[0]

            task_result = self._artifact_store[workflow_run_id][packed_artifact.id]

            inv_outputs[inv.id] = serde.result_from_artifact(
                task_result, ir.ArtifactFormat.AUTO
            )

        return inv_outputs

    def get_workflow_run_status(self, workflow_run_id: WfRunId) -> WorkflowRun:
        if workflow_run_id not in self._output_store:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow with id {workflow_run_id} not found"
            )
        workflow_def = self._workflow_def_store[workflow_run_id]
        start_time = self._start_time_store[workflow_run_id]
        end_time = self._end_time_store[workflow_run_id]
        return WorkflowRun(
            id=workflow_run_id,
            workflow_def=workflow_def,
            task_runs=[
                _make_completed_task_run(
                    workflow_run_id, start_time, end_time, task_inv
                )
                for task_inv in workflow_def.task_invocations
            ],
            status=RunStatus(
                state=State.SUCCEEDED,
                start_time=start_time,
                end_time=end_time,
            ),
        )

    def stop_workflow_run(self, workflow_run_id: WfRunId):
        if workflow_run_id in self._output_store:
            # Noop. If a client happens to call this method the workflow is already
            # stopped, by definition of the InProcessRuntime. If the user is running
            # the workflow using the InProcessRuntime the only way to call this method
            # would be after the workflow has finished, or in a different process.
            # We don't do IPC for this runtime class.
            pass
        else:
            # We didn't see this workflow run.
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow with id {workflow_run_id} not found"
            )

    def list_workflow_runs(
        self,
        *,
        limit: t.Optional[int] = None,
        max_age: t.Optional[timedelta] = None,
        state: t.Union[State, t.List[State], None] = None,
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

        wf_runs = []
        # Each workflow run executed with the in-process runtime is stored within the
        # runtime object.
        # We can grab the workflow_run ID from one of the storage locations
        for wf_run_id in self._workflow_def_store.keys():
            # The in-process runtime doesn't store the "run status", so let's reuse the
            # get_workflow_run_status method
            wf_run = self.get_workflow_run_status(wf_run_id)

            # Let's filter the workflows at this point, instead of iterating over a list
            # multiple times
            if state_list is not None and wf_run.status.state not in state_list:
                continue
            if max_age is not None and (
                now - (wf_run.status.start_time or now) < max_age
            ):
                continue
            wf_runs.append(wf_run)

        # We have to wait until we have all the workflow runs before sorting
        if limit is not None:
            wf_runs = sorted(wf_runs, key=lambda run: run.status.start_time or now)[
                -limit:
            ]
        return wf_runs

    @classmethod
    def from_runtime_configuration(cls, *args, **kwargs):
        raise NotImplementedError(
            "This functionality isn't available for 'in_process' runtime"
        )

    def get_task_logs(self, *args, **kwargs):
        raise NotImplementedError(
            "This functionality isn't available for 'in_process' runtime"
        )

    def get_workflow_logs(self, *args, **kwargs):
        raise NotImplementedError(
            "This functionality isn't available for 'in_process' runtime"
        )
