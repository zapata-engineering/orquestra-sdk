################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import typing as t
from datetime import datetime, timezone

from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import RunStatus, State, TaskRun, WorkflowRun

from ..exceptions import WorkflowRunNotFoundError
from ._graphs import iter_invocations_topologically
from .dispatch import locate_fn_ref
from .serde import deserialize_constant

WfRunId = str


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
    artifact_store: t.Dict[ir.ArtifactNodeId, t.Any],
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
    artifact_store: t.Dict[ir.ArtifactNodeId, t.Any],
    kwargs_ids: t.Dict[ir.ParameterName, ir.ArgumentId],
) -> t.Dict[ir.ParameterName, t.Any]:
    kwargs = {}
    for name, arg_id in kwargs_ids.items():
        try:
            kwargs[name] = artifact_store[arg_id]
        except KeyError:
            kwargs[name] = consts[arg_id]
    return kwargs


class InProcessRuntime:
    _output_store: t.Dict[WfRunId, t.Any]
    """
    Result of calling workflow function directly. Empty at first. Filled each
    time `create_workflow_run` is called.

    Implements orquestra.sdk._base.abc.RuntimeInterface methods.
    """

    def __init__(self):
        self._output_store = {}
        self._workflow_def_store: t.Dict[WfRunId, ir.WorkflowDef] = {}
        self._start_time_store: t.Dict[WfRunId, datetime] = {}
        self._end_time_store: t.Dict[WfRunId, datetime] = {}

    def _gen_next_run_id(self, wf_def: ir.WorkflowDef):
        return f"{wf_def.name}-{len(self._output_store) + 1}"

    def create_workflow_run(self, workflow_def: ir.WorkflowDef) -> WfRunId:
        run_id = self._gen_next_run_id(workflow_def)

        self._start_time_store[run_id] = datetime.now(timezone.utc)

        # We deserialize the constants in one go, instead of as needed
        consts: t.Dict[ir.ConstantNodeId, t.Any] = {
            id: deserialize_constant(node)
            for id, node in workflow_def.constant_nodes.items()
        }
        # We have the workflow run's artifacts stored here
        artifact_store: t.Dict[ir.ArtifactNodeId, t.Any] = {}

        # We are going to iterate over the workflow graph and execute each task
        # invocation sequentially, after topologically sorting the graph
        for task_inv in iter_invocations_topologically(workflow_def):
            # We can get the task function, args and kwargs from the task invocation
            task_fn: t.Any = locate_fn_ref(workflow_def.tasks[task_inv.task_id].fn_ref)
            args = _get_args(consts, artifact_store, task_inv.args_ids)
            kwargs = _get_kwargs(consts, artifact_store, task_inv.kwargs_ids)

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
                    artifact_store[artifact_id] = fn_output
                else:
                    artifact_store[artifact_id] = fn_output[artifact.artifact_index]

        # Ordinary functions return `obj` or `tuple(obj, obj)`
        outputs = tuple(_get_args(consts, artifact_store, workflow_def.output_ids))
        self._output_store[run_id] = outputs[0] if len(outputs) == 1 else outputs

        self._end_time_store[run_id] = datetime.now(timezone.utc)
        self._workflow_def_store[run_id] = workflow_def
        return run_id

    def get_workflow_run_outputs(self, workflow_run_id: WfRunId) -> t.Sequence[t.Any]:
        return self._output_store[workflow_run_id]

    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WfRunId
    ) -> t.Sequence[t.Any]:
        return self.get_workflow_run_outputs(workflow_run_id)

    def get_available_outputs(self, workflow_run_id: WfRunId) -> t.Dict[str, t.Any]:
        workflow_run_outputs = self.get_workflow_run_outputs(workflow_run_id)
        output_task_ids = self._workflow_def_store[workflow_run_id].output_ids
        return dict(zip(output_task_ids, workflow_run_outputs))

    def get_full_logs(self, _) -> t.Dict[str, t.List[str]]:
        return {}

    def get_workflow_run_status(self, workflow_run_id: WfRunId) -> WorkflowRun:
        if workflow_run_id not in self._output_store:
            raise WorkflowRunNotFoundError(
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
            raise WorkflowRunNotFoundError(
                f"Workflow with id {workflow_run_id} not found"
            )
