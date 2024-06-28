################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import typing as t
from collections import namedtuple
from itertools import chain

from orquestra.workflow_shared import serde
from orquestra.workflow_shared.abc import ArtifactValue, RuntimeInterface
from orquestra.workflow_shared.exceptions import (
    TaskRunNotFound,
    WorkflowRunIDNotFoundError,
)
from orquestra.workflow_shared.exec_ctx import ExecContext, get_current_exec_context
from orquestra.workflow_shared.logs import LogOutput
from orquestra.workflow_shared.schema import ir
from orquestra.workflow_shared.schema.responses import WorkflowResult
from orquestra.workflow_shared.schema.workflow_run import State
from orquestra.workflow_shared.schema.workflow_run import TaskRun as TaskRunModel
from orquestra.workflow_shared.schema.workflow_run import TaskRunId, WorkflowRunId
from orquestra.workflow_shared.serde import deserialize_constant


class TaskRun:
    """Represents execution of a single task."""

    class _InputUnavailable:
        def __str__(self):
            return "Input for a task is Unavailable"

        def __repr__(self):
            return "InputUnavailable()"

    INPUT_UNAVAILABLE = _InputUnavailable()

    Inputs = namedtuple("Inputs", "args kwargs")

    def __init__(
        self,
        task_run_id: TaskRunId,
        task_invocation_id: ir.TaskInvocationId,
        workflow_run_id: WorkflowRunId,
        runtime: RuntimeInterface,
        wf_def: ir.WorkflowDef,
    ):
        """This object isn't intended to be directly initialized.

        Instead, please use ``WorkflowRun.get_tasks()``.
        """
        self._task_run_id = task_run_id
        self._task_invocation_id = task_invocation_id
        self._runtime = runtime
        self._wf_def = wf_def
        self._workflow_run_id = workflow_run_id

        # get the fn_ref from wf_def
        invocation = wf_def.task_invocations[self.task_invocation_id]
        task_def = wf_def.tasks[invocation.task_id]
        fn_ref = task_def.fn_ref
        self._fn_name = fn_ref.function_name

        # inline function ref doesn't have module
        self._module: t.Optional[str]
        if isinstance(fn_ref, ir.ModuleFunctionRef):
            self._module = fn_ref.module
        else:
            self._module = None

    @property
    def task_run_id(self) -> TaskRunId:
        return self._task_run_id

    @property
    def task_invocation_id(self) -> ir.TaskInvocationId:
        return self._task_invocation_id

    @property
    def fn_name(self) -> str:
        return self._fn_name

    @property
    def workflow_run_id(self) -> WorkflowRunId:
        return self._workflow_run_id

    @property
    def module(self) -> t.Optional[str]:
        return self._module

    def __str__(self):
        return (
            f"TaskRun {self.task_run_id}, mod={self.module}, fn={self.fn_name}, "
            f"wf_run_id={self.workflow_run_id}, task_inv_id={self.task_invocation_id}"
        )

    def get_status(self) -> State:
        """Fetch current status from the runtime."""
        wf_run_model = self._runtime.get_workflow_run_status(self.workflow_run_id)
        task_run_model = next(
            run
            for run in wf_run_model.task_runs
            if run.invocation_id == self.task_invocation_id
        )
        return task_run_model.status.state

    def get_logs(self) -> LogOutput:
        return self._runtime.get_task_logs(
            wf_run_id=self.workflow_run_id, task_inv_id=self.task_invocation_id
        )

    def get_outputs(self) -> t.Any:
        """Get values calculated by this task run.

        Returns whatever the task function returned, regardless of
        ``@task(n_outputs=...)``.

        Raises:
            TaskRunNotFound: if the task wasn't completed yet, or the ID is invalid.
        """
        # NOTE: this is a possible place for improvement. If future runtime APIs support
        # getting a subset of artifacts, we should use them here.
        workflow_artifacts = self._runtime.get_available_outputs(self.workflow_run_id)

        try:
            task_outputs = workflow_artifacts[self.task_invocation_id]
        except KeyError as e:
            raise TaskRunNotFound(
                f"Output for task `{self.task_invocation_id}` in "
                f"`{self.workflow_run_id}` wasn't found. "
                "It may have failed or not be completed yet."
            ) from e

        return serde.deserialize(task_outputs)

    def _find_invocation_by_output_id(self, output: ir.ArgumentId) -> ir.TaskInvocation:
        """Locates the invocation object responsible for returning output with given ID.

        It is based on the assumption that output IDs are unique, and one output
        is returned by a single task invocation (but one invocation can produce
        multiple outputs).
        """
        return next(
            inv
            for inv in self._wf_def.task_invocations.values()
            if output in inv.output_ids
        )

    def _find_value_by_id(
        self,
        arg_id: ir.ArgumentId,
        available_outputs: t.Mapping[ir.TaskInvocationId, WorkflowResult],
    ) -> ArtifactValue:
        """Find and deserialize input artifact value based on the artifact/argument ID.

        Uses values from available_outputs, or deserializes the constant embedded in
        the workflow def.
        """
        if arg_id in self._wf_def.constant_nodes:
            value = deserialize_constant(self._wf_def.constant_nodes[arg_id])
            return value
        elif arg_id in self._wf_def.secret_nodes:
            return self._wf_def.secret_nodes[arg_id]

        producer_inv = self._find_invocation_by_output_id(arg_id)
        output_index = self._wf_def.artifact_nodes[arg_id].artifact_index
        try:
            artifact_node = available_outputs[producer_inv.id]
        except KeyError:
            # Parent invocation ID not in available outputs => parent invocation
            # wasn't completed yet.
            return self.INPUT_UNAVAILABLE

        parent_output_vals = serde.deserialize(artifact_node)

        # A task function can return multiple values. We're only interested in one
        # of them.
        #
        # Assumption 1: RuntimeInterface.get_available_outputs() returns tuples even
        # for tasks with single output.
        #
        # Assumption 2: either a task wasn't completed yet and we don't have any
        # outputs (handled above) or we have access to all outputs of this task.
        # There shouldn't be a situation where we have access to a subset of a given
        # task's outputs.
        if output_index is None:
            return parent_output_vals
        else:
            return parent_output_vals[output_index]

    def get_inputs(self) -> Inputs:
        """Get values of inputs (parameters) of a task run.

        Returns:
            Input namedTuple with Input.args and .kwargs parameters.
        """
        all_inv_outputs = self._runtime.get_available_outputs(self.workflow_run_id)

        task_invocation = self._wf_def.task_invocations[self.task_invocation_id]
        args = [
            self._find_value_by_id(arg_id, all_inv_outputs)
            for arg_id in task_invocation.args_ids
        ]
        kwargs = {
            param_name: self._find_value_by_id(kwarg_id, all_inv_outputs)
            for param_name, kwarg_id in task_invocation.kwargs_ids.items()
        }

        return self.Inputs(args, kwargs)

    def get_parents(self) -> t.Set["TaskRun"]:
        """Get 'parent' tasks whose return values are taken as input parameters."""
        # Note: theoretically there might be a mismatch between task invocations and
        # task runs. Task invocations come from the static workflow definition. Task
        # runs only exist if a given task invocation was scheduled for execution. If a
        # given invocation is waiting to be run, there might be no corresponding task
        # run.
        #
        # However, this method is about getting parents. We can assume that if a task
        # run A exists, its always parents exist as well; otherwise invocation A
        # wouldn't be scheduled and task run A wouldn't exist in the first place.

        # 1. Get parent invocation IDs
        task_invocation = self._wf_def.task_invocations[self.task_invocation_id]
        parent_inv_ids = set()
        # for every arg in the function
        for arg_id in chain(
            task_invocation.args_ids, task_invocation.kwargs_ids.values()
        ):
            # If it's constant, there is no parent task that produces it
            if arg_id in self._wf_def.constant_nodes:
                continue
            # find invocation that produces it
            parent_inv = self._find_invocation_by_output_id(arg_id)
            parent_inv_ids.add(parent_inv.id)

        # 2. Get parent task run models
        wf_run_model = self._runtime.get_workflow_run_status(self.workflow_run_id)
        parent_run_models: t.Sequence[TaskRunModel] = [
            run for run in wf_run_model.task_runs if run.invocation_id in parent_inv_ids
        ]

        # 3. Wrap in convenience objects
        parents = [
            TaskRun(
                task_run_id=model.id,
                task_invocation_id=model.invocation_id,
                workflow_run_id=self.workflow_run_id,
                runtime=self._runtime,
                wf_def=self._wf_def,
            )
            for model in parent_run_models
        ]

        return set(parents)


class CurrentRunIDs(t.NamedTuple):
    workflow_run_id: WorkflowRunId
    task_invocation_id: t.Optional[ir.TaskInvocationId]
    task_run_id: t.Optional[TaskRunId]


def _get_ray_backend_ids() -> CurrentRunIDs:
    """Get the workflow run, task invocation, and task run IDs from Ray.

    Raises:
        ModuleNotFoundError: when Ray isn't installed.
        WorkflowRunIDNotFoundError: When the workflow run ID can't be recovered.

    Returns:
        The IDs associated with the current run, in a named tuple. See: CurrentRunIDs
    """
    # Deferred import in case Ray isn't installed
    from orquestra.workflow_runtime import get_current_ids

    (
        wf_run_id,
        task_inv_id,
        task_run_id,
    ) = get_current_ids()

    if wf_run_id is None:
        raise WorkflowRunIDNotFoundError("Could not recover Workflow Run ID")

    return CurrentRunIDs(wf_run_id, task_inv_id, task_run_id)


def _get_in_process_backend_ids() -> CurrentRunIDs:
    """Get the backend IDs from the In-process runtime.

    Raises:
        WorkflowRunIDNotFoundError: When the workflow run ID can't be recovered.

    Returns:
        The workflow run, task invocation, and task run IDs associated with the current
            run, in a named tuple. See: CurrentRunIDs
    """
    from ..._base._in_process_runtime import get_current_in_process_ids

    ids = get_current_in_process_ids()

    if ids is None:
        raise WorkflowRunIDNotFoundError(
            "current_run_ids global was imported with value None."
        )
    if ids[0] is None:
        raise WorkflowRunIDNotFoundError("Could not recover Workflow Run ID")

    return CurrentRunIDs(*ids)


def current_run_ids() -> CurrentRunIDs:
    """Get the backend IDs related to current execution context.

    Workflow run ID is a globally unique identifier generated whenever a workflow is
    submitted for running. Single workflow definition can be run multiple times
    resulting in multiple workflow run IDs. Analog of PID for a standard program.

    Task invocation ID is related to using a task in your workflow definition,
    analogous to a function invocation in a standard program. Scoped to a workflow
    definition. Isn't globally unique. If you run the same workflow definition multiple
    times, you'll end up with the same task invocation IDs across runs.

    Task run ID is a globally unique identifier of executing an invocation exactly once.

    This function is intended to be used within the task code in the following way::

        @sdk.task
        def t():
            wf_run_id, task_inv_id, task_run_id =  sdk.current_run_ids()
            ...

    Returns:
        The workflow run, task invocation, and task run IDs associated with the current
            run, in a named tuple. See: CurrentRunIDs

    Raises:
        WorkflowRunIDNotFoundError: When the workflow run ID cannot be recovered.
        NotImplementedError: When the execution context is not one of the covered cases.
    """
    context = get_current_exec_context()

    if context == ExecContext.RAY:
        return _get_ray_backend_ids()
    elif context == ExecContext.DIRECT:
        return _get_in_process_backend_ids()

    raise NotImplementedError(
        f"Got unexpected global context {context}. Please report this as a bug."
    )
