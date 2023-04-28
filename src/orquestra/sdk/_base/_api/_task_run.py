################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import typing as t
from collections import namedtuple
from itertools import chain

from orquestra.sdk._base import serde
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.responses import WorkflowResult
from orquestra.sdk.schema.workflow_run import State, TaskInvocationId
from orquestra.sdk.schema.workflow_run import TaskRun as TaskRunModel
from orquestra.sdk.schema.workflow_run import TaskRunId, WorkflowRunId

from ...exceptions import TaskRunNotFound
from ..abc import ArtifactValue, RuntimeInterface
from ..serde import deserialize_constant


class TaskRun:
    """
    Represents execution of a single task.
    """

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
        task_invocation_id: TaskInvocationId,
        workflow_run_id: WorkflowRunId,
        runtime: RuntimeInterface,
        wf_def: ir.WorkflowDef,
    ):
        """
        This object isn't intended to be directly initialized. Instead, please use
        `WorkflowRun.get_tasks()`.
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
    def task_invocation_id(self) -> TaskInvocationId:
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
        """
        Fetch current status from the runtime
        """
        wf_run_model = self._runtime.get_workflow_run_status(self.workflow_run_id)
        task_run_model = next(
            run
            for run in wf_run_model.task_runs
            if run.invocation_id == self.task_invocation_id
        )
        return task_run_model.status.state

    def get_logs(self) -> t.List[str]:
        return self._runtime.get_task_logs(
            wf_run_id=self.workflow_run_id, task_inv_id=self.task_invocation_id
        )

    def get_outputs(self) -> t.Any:
        """
        Get values calculated by this task run.

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
        """
        Helper method that locates the invocation object responsible for returning
        output with given ID
        It is based on the assumption that output IDs are unique, and one output
        is returned by a single task invocation (but one invocation can produce
        multiple outputs)
        """
        return next(
            inv
            for inv in self._wf_def.task_invocations.values()
            if output in inv.output_ids
        )

    def _find_value_by_id(
        self,
        arg_id: ir.ArgumentId,
        available_outputs: t.Mapping[TaskInvocationId, WorkflowResult],
    ) -> ArtifactValue:
        """
        Helper method that finds and deserializes input artifact value based on the
        artifact/argument ID. Uses values from available_outputs, or deserializes the
        constant embedded in the workflow def.
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
        """
        Get values of inputs (parameters) of a task run

        Returns:  Input namedTuple with Input.args and .kwargs parameters.
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
        """
        Get parent tasks - tasks whose return values are taken as input parameters
        """
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
