################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
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

from ..exceptions import WorkspacesNotSupportedError
from ..schema.configs import RuntimeConfiguration
from ..schema.ir import TaskInvocationId, WorkflowDef
from ..schema.local_database import StoredWorkflowRun
from ..schema.responses import WorkflowResult
from ..schema.workflow_run import (
    ProjectId,
    State,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
    WorkspaceId,
)
from ._logs._interfaces import LogReader, WorkflowLogs
from ._spaces._structs import Project, ProjectRef, Workspace

# A typealias that hints where we expect raw artifact values.
ArtifactValue = t.Any


class RuntimeInterface(ABC, LogReader):
    """
    The main abstraction for managing Orquestra workflows. Allows swapping the
    implementations related to local vs remote runs.
    """

    @abstractmethod
    def create_workflow_run(
        self, workflow_def: WorkflowDef, project: t.Optional[ProjectRef]
    ) -> WorkflowRunId:
        """Schedules a workflow definition for execution

        Args:
            workflow_def: IR definition of workflow to be executed
            project: project in which workflow is going to be executed
                used currently only on CE runtime.
                When omitted, WF will be scheduled at default project
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """Gets the status of a workflow run"""
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[WorkflowResult]:
        """Non-blocking version of get_workflow_run_outputs.

        This method raises exceptions if the workflow output artifacts are not available

        Raises:
            WorkflowRunNotSucceeded if the workflow has not yet finished
        """
        raise NotImplementedError()

    @abstractmethod
    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Dict[TaskInvocationId, WorkflowResult]:
        """Returns all available outputs for a workflow

        This method returns all available artifacts. When the workflow fails it returns
        artifacts only for the steps that did success. Might raise an exception if
        runtime doesn't support getting artifacts from in-progress workflow.

        Either we have access to all outputs of a given task, or none. If a given task
        invocation didn't succeed yet, there shouldn't be an entry in the returned dict.

        This method should return all output values for a task even if some of them
        aren't used in the workflow function. Reasons:
        - Users might be interested in the computed value after running, even though
          the workflow didn't make an explicit use of it.
        - Position in the task output tuple is significant. We can't just drop some of
          the elements because this would shift indices.

        Careful: This method does NOT return status of a workflow. Verify it beforehand
        to make sure if workflow failed/succeeded/is running. You might get incomplete
        results

        Returns:
            A mapping with an entry for each task run in the workflow. The key is the
                task's invocation ID. The value is whatever the task function returned,
                independent of the ``@task(n_outputs=...)`` value.
        """
        raise NotImplementedError()

    @abstractmethod
    def stop_workflow_run(
        self, workflow_run_id: WorkflowRunId, *, force: t.Optional[bool] = None
    ) -> None:
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
        workspace: t.Optional[WorkspaceId] = None,
        project: t.Optional[ProjectId] = None
    ) -> t.Sequence[WorkflowRunMinimal]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace. Supported only
                on CE.
        Returns:
                A list of the workflow runs
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_project(self, wf_run_id: WorkflowRunId):
        """
        Returns project and workspace IDs of given workflow
        """
        raise NotImplementedError()

    def list_workspaces(self) -> t.Sequence[Workspace]:
        """
        List workspaces available to a user. Works only on CE
        """
        raise WorkspacesNotSupportedError()

    def list_projects(self, workspace_id: str) -> t.Sequence[Project]:
        """
        List workspaces available to a user. Works only on CE
        """
        raise WorkspacesNotSupportedError()

    @abstractmethod
    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_logs(self, wf_run_id: WorkflowRunId) -> WorkflowLogs:
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
