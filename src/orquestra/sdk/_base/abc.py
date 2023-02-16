################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
"""
Interfaces exposed by orquestra-sdk.

`abc` might be a misnomer because we have protocols here, but it's close-enough
to let the leader know to expect interfaces here.

This module shouldn't contain any implementation, only interface definitions.
"""

import typing as t
from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path

from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.ir import TaskInvocationId, WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import (
    State,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
)


class LogReader(t.Protocol):
    """
    A component that reads logs produced by tasks and workflows.
    """

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:
        """
        Reads all available logs, specific to a single task invocation/run.

        Returns:
            Log lines printed when running this task invocation. If the task didn't
            produce any logs this should be an empty list.
        """
        ...

    def get_workflow_logs(
        self, wf_run_id: WorkflowRunId
    ) -> t.Dict[TaskInvocationId, t.List[str]]:
        """
        Reads all available logs printed during execution of this workflow run.

        Returns:
            A mapping with task logs. Each key-value pair corresponds to one task
            invocation.
            - key: task invocation ID (see
                orquestra.sdk._base.ir.WorkflowDef.task_invocations)
            - value: log lines from running this task invocation
        """
        ...


# A typealias that hints where we expect raw artifact values.
ArtifactValue = t.Any


class RuntimeInterface(ABC):
    """
    The main abstraction for managing Orquestra workflows. Allows swapping the
    implementations related to local vs remote runs.
    """

    @classmethod
    @abstractmethod
    def from_runtime_configuration(
        cls, project_dir: Path, config: RuntimeConfiguration, verbose: bool
    ) -> "RuntimeInterface":
        """Returns an initilaised version of the class from a "Runtime options" JSON"""
        raise NotImplementedError()

    @abstractmethod
    def create_workflow_run(
        self,
        workflow_def: WorkflowDef,
    ) -> WorkflowRunId:
        """Schedules a workflow definition for execution"""
        raise NotImplementedError()

    @abstractmethod
    def get_all_workflow_runs_status(self) -> t.List[WorkflowRun]:
        """Gets the status of all workflow runs."""
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """Gets the status of a workflow run"""
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[ArtifactValue]:
        """Returns the output artifacts of a workflow run

        For example, for this workflow:

            @sdk.workflow
            def my_wf():
                return [my_task(), another_task()]

        this method will return an iterable that yields the results from my_task and
        another_task().

        This method blocks until the workflow is completed
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[ArtifactValue]:
        """Non-blocking version of get_workflow_run_outputs.

        This method raises exceptions if the workflow output artifacts are not available

        Raises:
            WorkflowRunNotSucceeded if the workflow has not yet finished
        """
        raise NotImplementedError()

    @abstractmethod
    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Dict[TaskInvocationId, t.Tuple[ArtifactValue, ...]]:
        """Returns all available outputs for a workflow

        This method returns all available artifacts. When the workflow fails it returns
        artifacts only for the steps that did success. Might raise an exception if
        runtime doesn't support getting artifacts from in-progress workflow.

        Either we have access to all outputs of a given task, or none. In other words,
        the number of values in the tuple should always match the number of output IDs
        in the corresponding task invocation.

        Careful: This method does NOT return status of a workflow. Verify it beforehand
        to make sure if workflow failed/succeeded/is running. You might get incomplete
        results

        Returns:
            A mapping with an entry for each task run in the workflow. The key is the
                task's invocation ID. The value is a n-tuple, where n is the number of
                task's outputs. If task has 1 output, this will be a 1-tuple.
        """
        raise NotImplementedError()

    @abstractmethod
    def stop_workflow_run(self, workflow_run_id: WorkflowRunId) -> None:
        """Stops a workflow run.

        Raises:
        WorkflowRunCanNotBeTerminated if workflow run is cannot be terminated.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_workflow_runs(
        self,
        *,
        limit: t.Optional[int] = None,
        max_age: t.Optional[timedelta] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
    ) -> t.Sequence[WorkflowRunMinimal]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            status: Only return runs of runs with the specified status.
        Returns:
                A list of the workflow runs
        """
        raise NotImplementedError()

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:
        """
        See LogReader.get_task_logs()
        """
        raise NotImplementedError()

    def get_workflow_logs(
        self, wf_run_id: WorkflowRunId
    ) -> t.Dict[TaskInvocationId, t.List[str]]:
        """
        See LogReader.get_task_logs()
        """
        raise NotImplementedError()


class WorkflowRepo(ABC):
    """
    This is the interface for accessing workflow runs that have been submitted

    The results and status is still delegated to the Runtime
    """

    @abstractmethod
    def save_workflow_run(self, workflow_run: StoredWorkflowRun):
        """
        Save a workflow run

        Arguments:
            workflow_run: the workflow run with run ID, stored config, and WorkflowDef

        Raises:
            RuntimeError if the workflow ID has already been used
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run(self, workflow_run_id: WorkflowRunId) -> StoredWorkflowRun:
        """
        Retrieve a workflow run based on ID

        Arguments:
            workflow_run_id: the run ID to load

        Raises:
            NotFoundError if the workflow run is not found

        Returns:
            the workflow run with run ID, stored config, and WorkflowDef
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_runs_list(
        self, prefix: t.Optional[str], config_name: t.Optional[str]
    ) -> t.List[StoredWorkflowRun]:
        """
        Retrieve all workflow runs matching one or more conditions. If no conditions
        are set, returns all workflow runs.

        Arguments:
            prefix (Optional): Only return workflow runs whose IDs start with the
            prefix.
            config_name (Optional): Only return workflow runs that use the
            specified configuration name.

        Returns:
            A list of workflow runs for a given config. Includes: run ID, stored
            config, and WorkflowDef
        """
        raise NotImplementedError()
