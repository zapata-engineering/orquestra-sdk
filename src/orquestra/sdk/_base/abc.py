################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
################################################################################
"""Interfaces exposed by orquestra-sdk.

`abc` might be a misnomer because we have protocols here, but it's close-enough
to let the leader know to expect interfaces here.

This module shouldn't contain any implementation, only interface definitions.
"""

import typing as t
from abc import ABC, abstractmethod
from datetime import timedelta

from ..exceptions import WorkspacesNotSupportedError
from ..schema.ir import TaskInvocationId, WorkflowDef
from ..schema.responses import WorkflowResult
from ..schema.workflow_run import (
    State,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
    WorkflowRunSummary,
    WorkspaceId,
)
from ._logs._interfaces import LogOutput, LogReader, WorkflowLogs
from ._spaces._structs import Project, ProjectRef, Workspace

# A typealias that hints where we expect raw artifact values.
ArtifactValue = t.Any


class RuntimeInterface(ABC, LogReader):
    """The main abstraction for managing Orquestra workflows.

    Allows swapping the implementations related to local vs remote runs.
    """

    @abstractmethod
    def create_workflow_run(
        self, workflow_def: WorkflowDef, project: t.Optional[ProjectRef], dry_run: bool
    ) -> WorkflowRunId:
        """Schedules a workflow definition for execution.

        Args:
            workflow_def: IR definition of workflow to be executed.
            project: project in which workflow is going to be executed
                used currently only on CE runtime.
                When omitted, WF will be scheduled at default project
            dry_run: Run the workflow without actually executing any task code.
                Useful for testing infrastructure, dependency imports, etc.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """Gets the status of a workflow run."""
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[WorkflowResult]:
        """Non-blocking version of get_workflow_run_outputs.

        This method raises exceptions if the workflow output artifacts are not
        available.

        Args:
            workflow_run_id: ID identifying the workflow run.

        Raises:
            WorkflowRunNotSucceeded: if the workflow has not yet finished.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Dict[TaskInvocationId, WorkflowResult]:
        """Returns all available outputs for a workflow run.

        This method returns all available artifacts.
        When the workflow fails it returns artifacts only for the steps that did
        success.
        Might raise an exception if runtime doesn't support getting artifacts from
        in-progress workflow.

        Either we have access to all outputs of a given task, or none.
        If a given task invocation didn't succeed yet, there shouldn't be an entry in
        the returned dict.

        This method should return all output values for a task even if some of them
        aren't used in the workflow function.
        Reasons:

        * Users might be interested in the computed value after running, even though
          the workflow didn't make an explicit use of it.
        * Position in the task output tuple is significant. We can't just drop some of
          the elements because this would shift indices.

        Careful: This method does NOT return status of a workflow.
        Verify it beforehand to make sure if workflow failed/succeeded/is running.
        You might get incomplete results.

        Args:
            workflow_run_id: ID identifying the workflow run.


        Returns:
            A mapping with an entry for each task run in the workflow.
                The key is the task's invocation ID.
                The value is whatever the task function returned, independent of the
                ``@task(n_outputs=...)`` value.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_output(
        self, workflow_run_id: WorkflowRunId, task_invocation_id: TaskInvocationId
    ) -> WorkflowResult:
        """Returns single output for a workflow run.

        This method returns single artifact for given task.
        When the task failed or was not yet completed, this method throws an exception

        Careful: This method does NOT return status of a task.
        Verify it beforehand to make sure if task failed/succeeded/is running.
        You might get an exception

        Args:
            workflow_run_id: ID identifying the workflow run.
            task_invocation_id: ID identifying single task run

        Returns:
            Whatever the task function returned, independent of the
            ``@task(n_outputs=...)`` value.
        """
        raise NotImplementedError()

    @abstractmethod
    def stop_workflow_run(
        self, workflow_run_id: WorkflowRunId, *, force: t.Optional[bool] = None
    ) -> None:
        """Stops a workflow run.

        Args:
            workflow_run_id: ID identifying the workflow run.
            force: Asks the runtime to terminate the workflow without waiting for the
                workflow to gracefully exit.
                By default, this behavior is up to the runtime, but can be overridden
                with True/False.

        Raises:
            WorkflowRunCanNotBeTerminated: if workflow run is cannot be terminated.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_workflow_runs(
        self,
        *,
        limit: t.Optional[int] = None,
        max_age: t.Optional[timedelta] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
        workspace: t.Optional[WorkspaceId] = None,
    ) -> t.Sequence[WorkflowRunMinimal]:
        """List the workflow runs, with some filters.

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace. Supported only
                on CE.

        Returns:
                A list of the workflow runs.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_workflow_run_summaries(
        self,
        *,
        limit: t.Optional[int] = None,
        max_age: t.Optional[timedelta] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
        workspace: t.Optional[WorkspaceId] = None,
    ) -> t.List[WorkflowRunSummary]:
        """List summaries of the workflow runs, with some filters.

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace.

        Raises:
            UnauthorizedError: if the remote cluster rejects the token.

        Returns:
            A list of the workflow run summaries.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_project(self, wf_run_id: WorkflowRunId):
        """Returns project and workspace IDs of given workflow."""
        raise NotImplementedError()

    def list_workspaces(self) -> t.Sequence[Workspace]:
        """List workspaces available to a user. Works only on CE."""
        raise WorkspacesNotSupportedError()

    def list_projects(self, workspace_id: str) -> t.Sequence[Project]:
        """List workspaces available to a user. Works only on CE."""
        raise WorkspacesNotSupportedError()

    @abstractmethod
    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> LogOutput:
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_logs(self, wf_run_id: WorkflowRunId) -> WorkflowLogs:
        raise NotImplementedError()
