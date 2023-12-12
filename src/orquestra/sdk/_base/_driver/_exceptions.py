################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""Exception types related to the Workflow Driver API."""
from typing import List, Optional

import requests

from ._models import (
    TaskInvocationID,
    WorkflowDefID,
    WorkflowRunArtifactID,
    WorkflowRunID,
    WorkflowRunResultID,
)


class InvalidTokenError(Exception):
    """
    Raised when the communication with the external Config Service couldn't be made
    because of an invalid token.
    """  # noqa: D205, D212, D415

    pass


class InvalidWorkflowDef(Exception):
    """Raised when an invalid Workflow Definition is sent to the Workflow Driver."""

    def __init__(self, message: str, detail: str):
        self.message = message
        self.detail = detail
        super().__init__(message, detail)


class InvalidWorkflowRunRequest(Exception):
    """Raised when an invalid Workflow Run request is sent to the Workflow Driver."""

    def __init__(self, message: str, detail: str):
        self.message = message
        self.detail = detail
        super().__init__(message, detail)


class UnsupportedSDKVersion(InvalidWorkflowRunRequest):
    """
    Raised when an unsupported Workflow SDK version is used to submit a workflow run.
    """  # noqa: D200, D212

    def __init__(
        self, submitted_version: Optional[str], supported_versions: Optional[List[str]]
    ):
        self.submitted_version = submitted_version
        self.supported_versions = supported_versions
        super().__init__(
            f"Unsupported Workflow SDK version: {submitted_version}",
            f"Supported Workflow SDK versions: {supported_versions}",
        )


class InvalidWorkflowDefID(Exception):
    """Raised when an invalid Workflow Definition ID is sent to the Workflow Driver.

    Workflow Definition IDs are expected to be UUIDs, if an ID is sent that cannot be
    parsed as a UUID, this exception will be raised.
    """

    def __init__(self, workflow_def_id: WorkflowDefID):
        self.workflow_def_id = workflow_def_id
        super().__init__(workflow_def_id)


class InvalidWorkflowRunID(Exception):
    """Raised when an invalid Workflow Run ID is sent to the Workflow Driver."""

    def __init__(self, workflow_run_id: WorkflowRunID):
        self.workflow_run_id = workflow_run_id
        super().__init__(workflow_run_id)


class InvalidWorkflowRunArtifactID(Exception):
    """Raised when an invalid Workflow Run Artifact ID is sent to the Workflow Driver.

    Workflow Run Artifact IDs are expected to be UUIDs, if an ID is sent that cannot be
    parsed as a UUID, this exception will be raised.
    """

    def __init__(self, workflow_run_artifact_id: WorkflowRunArtifactID):
        self.workflow_run_artifact_id = workflow_run_artifact_id
        super().__init__(workflow_run_artifact_id)


class InvalidWorkflowRunResultID(Exception):
    """Raised when an invalid Workflow Run Result ID is sent to the Workflow Driver.

    Workflow Run Result IDs are expected to be UUIDs, if an ID is sent that cannot be
    parsed as a UUID, this exception will be raised.
    """

    def __init__(self, workflow_run_result_id: WorkflowRunResultID):
        self.workflow_run_result_id = workflow_run_result_id
        super().__init__(workflow_run_result_id)


class ForbiddenError(Exception):
    """Raised when the user did not have permission to access a specific resource."""


class UnknownHTTPError(Exception):
    """Raised when there's an error we don't handle otherwise."""

    def __init__(self, response: requests.Response):
        self.response = response
        super().__init__(response)


class WorkflowDefNotFound(Exception):
    """Raised when a Workflow Definition cannot be found."""

    def __init__(self, workflow_def_id: WorkflowDefID):
        self.workflow_def_id = workflow_def_id
        super().__init__(workflow_def_id)


class WorkflowRunNotFound(Exception):
    """Raised when a Workflow Run cannot be found."""

    def __init__(self, workflow_run_id: WorkflowRunID):
        self.workflow_run_id = workflow_run_id
        super().__init__(workflow_run_id)


class WorkflowRunLogsNotFound(Exception):
    """Raised when a Workflow Run's Logs cannot be found."""

    def __init__(self, workflow_run_id: WorkflowRunID):
        self.workflow_run_id = workflow_run_id
        super().__init__(workflow_run_id)


class TaskRunLogsNotFound(Exception):
    """Raised when a Task Run's Logs cannot be found."""

    def __init__(
        self, workflow_run_id: WorkflowRunID, task_invocation_id: TaskInvocationID
    ):
        self.workflow_run_id = workflow_run_id
        self.task_invocation_id = task_invocation_id
        super().__init__(workflow_run_id, task_invocation_id)


class WorkflowRunLogsNotReadable(Exception):
    """Raised when a Workflow Run's Logs exist, but cannot be decoded."""

    def __init__(
        self,
        workflow_run_id: WorkflowRunID,
        task_invocation_id: Optional[TaskInvocationID],
    ):
        self.workflow_run_id = workflow_run_id
        self.task_invocation_id = task_invocation_id
        super().__init__(workflow_run_id, task_invocation_id)


class WorkflowRunArtifactNotFound(Exception):
    """Raised when a Workflow Run Artifact cannot be found."""

    def __init__(self, workflow_run_artifact_id: WorkflowRunArtifactID):
        self.workflow_run_artifact_id = workflow_run_artifact_id
        super().__init__(workflow_run_artifact_id)


class WorkflowRunResultNotFound(Exception):
    """Raised when a Workflow Run Result cannot be found."""

    def __init__(self, workflow_run_result_id: WorkflowRunResultID):
        self.workflow_run_result_id = workflow_run_result_id
        super().__init__(workflow_run_result_id)


class InvalidWorkspaceZRI(Exception):
    """Raised when workspace ZRI is invalid."""

    def __init__(self, zri: str):
        self.zri = zri
        super().__init__(zri)
