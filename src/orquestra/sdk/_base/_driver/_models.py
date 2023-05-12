################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Internal models for the workflow driver API
"""
from datetime import datetime
from enum import Enum
from typing import Generic, List, Mapping, Optional, TypeVar

import pydantic
from pydantic.generics import GenericModel

from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.workflow_run import (
    ProjectId,
    RunStatus,
    State,
    TaskRun,
    WorkflowRun,
    WorkflowRunMinimal,
    WorkspaceId,
)

WorkflowDefID = str
WorkflowRunID = str
TaskRunID = str
TaskInvocationID = str
WorkflowRunArtifactID = str
WorkflowRunResultID = str


# --- Generic responses and pagination ---

DataT = TypeVar("DataT")
MetaT = TypeVar("MetaT")


class Pagination(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/259481b9240547bccf4fa40df4e92bf6c617a25f/openapi/src/schemas/MetaSuccessPaginated.yaml
    """

    nextPageToken: str


class Response(GenericModel, Generic[DataT, MetaT]):
    """
    A generic to help with the structure of driver responses
    """

    data: DataT
    meta: Optional[MetaT]


class MetaEmpty(pydantic.BaseModel):
    pass


class Error(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/2b3534/openapi/src/schemas/Error.yaml
    """

    code: Optional[int]
    message: str
    detail: str


# --- Workflow Definitions ---


class CreateWorkflowDefResponse(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/2b3534/openapi/src/responses/CreateWorkflowDefinitionResponse.yaml
    """

    id: WorkflowDefID


class GetWorkflowDefResponse(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/7b472546225d0a87be3694ddaa330db4ddcad3c1/openapi/src/schemas/WorkflowDefinition.yaml
    """

    id: WorkflowDefID
    created: datetime
    owner: str
    workflow: WorkflowDef


class ListWorkflowDefsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/cdb667ef6d1053876250daff27e19fb50374c0d4/openapi/src/resources/workflow-definitions.yaml#L8
    """

    pageSize: Optional[int]
    pageToken: Optional[str]


class CreateWorkflowDefsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/dc8a2a37d92324f099afefc048f6486a5061850f/openapi/src/resources/workflow-definitions.yaml#L39
    """

    workspaceId: Optional[str]
    projectId: Optional[str]


ListWorkflowDefsResponse = List[GetWorkflowDefResponse]

# --- Workflow Runs ---


class StateResponse(str, Enum):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/RunStatus.yaml#L7
    """

    WAITING = "WAITING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"


class RunStatusResponse(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/RunStatus.yaml#L1
    """

    state: StateResponse
    startTime: Optional[datetime]
    endTime: Optional[datetime]

    def to_ir(self) -> RunStatus:
        return RunStatus(
            state=State(self.state),
            start_time=self.startTime,
            end_time=self.endTime,
        )


class TaskRunResponse(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L17
    """

    id: TaskRunID
    invocationId: TaskInvocationID
    status: Optional[RunStatusResponse]

    def to_ir(self) -> TaskRun:
        if self.status is None:
            status = RunStatus(
                state=State.WAITING,
                start_time=None,
                end_time=None,
            )
        else:
            status = self.status.to_ir()
        return TaskRun(
            id=self.id,
            invocation_id=self.invocationId,
            status=status,
        )


class MinimalWorkflowRunResponse(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L1
    """

    id: WorkflowRunID
    definitionId: WorkflowDefID

    def to_ir(self, workflow_def: WorkflowDef) -> WorkflowRunMinimal:
        return WorkflowRunMinimal(
            id=self.id,
            workflow_def=workflow_def,
        )


class WorkflowRunResponse(MinimalWorkflowRunResponse):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L1
    """

    owner: str
    status: RunStatusResponse
    taskRuns: List[TaskRunResponse]

    def to_ir(self, workflow_def: WorkflowDef) -> WorkflowRun:
        return WorkflowRun(
            id=self.id,
            status=self.status.to_ir(),
            task_runs=[t.to_ir() for t in self.taskRuns],
            workflow_def=workflow_def,
        )


class Resources(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/580c8d8835b1cccd085ea716c514038e85eb28d7/openapi/src/schemas/Resources.yaml
    """

    # If this schema is changed, the documentation in
    # docs/guides/ce-resource-management.rst should also be updated.

    nodes: Optional[int]
    cpu: Optional[str] = pydantic.Field(
        regex=r"^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
    )
    memory: Optional[str] = pydantic.Field(
        regex=r"^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
    )
    gpu: Optional[str] = pydantic.Field(regex="^[01]+$")


class CreateWorkflowRunRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/schemas/CreateWorkflowRunRequest.yaml
    """

    workflowDefinitionID: WorkflowDefID
    resources: Resources


class CreateWorkflowRunResponse(pydantic.BaseModel):
    """
    Based on source code:
        https://github.com/zapatacomputing/workflow-driver/blob/2e999a76019e8f8de8082409daddf7789dc2f430/pkg/server/server.go#L376
    """

    id: WorkflowRunID


class ListWorkflowRunsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/workflow-runs.yaml#L9
    """

    workflowDefinitionID: Optional[WorkflowDefID]
    pageSize: Optional[int]
    pageToken: Optional[str]
    workspaceId: Optional[WorkspaceId]
    projectId: Optional[ProjectId]


ListWorkflowRunsResponse = List[MinimalWorkflowRunResponse]


class GetWorkflowRunResponse(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/workflow-run.yaml#L17
    """

    data: WorkflowRunResponse


# --- Workflow Artifacts ---


class GetWorkflowRunArtifactsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/artifacts.yaml#L10
    """

    workflowRunId: WorkflowRunID


GetWorkflowRunArtifactsResponse = Mapping[TaskRunID, List[WorkflowRunArtifactID]]

# --- Workflow Results ---


class GetWorkflowRunResultsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/run-results.yaml#L10
    """

    workflowRunId: WorkflowRunID


GetWorkflowRunResultsResponse = List[WorkflowRunResultID]


# --- Logs ---


class GetWorkflowRunLogsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/workflow-run-logs.yaml
    """

    workflowRunId: WorkflowRunID


class GetTaskRunLogsRequest(pydantic.BaseModel):
    """
    Implements:
        https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/task-run-logs.yaml#L8
    """

    taskRunId: TaskRunID


class CommonResourceMeta(pydantic.BaseModel):
    type: str
    displayName: str
    description: str
    owner: str
    createdBy: str
    createdAt: str
    lastAccessed: str
    lastUpdated: str
    tags: List[str]
    status: str


class ResourceIdentifier(pydantic.BaseModel):
    tenantId: str
    resourceGroupId: str
    id: str


class WorkspaceDetail(CommonResourceMeta, ResourceIdentifier):
    logo: str
    namespace: str


class ProjectDetail(CommonResourceMeta, ResourceIdentifier):
    logo: str
    image: str
    profileName: str


ListWorkspacesResponse = List[WorkspaceDetail]
ListProjectResponse = List[ProjectDetail]
