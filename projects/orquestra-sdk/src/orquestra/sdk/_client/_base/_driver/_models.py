################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Internal models for the workflow driver API."""
from enum import Enum, IntEnum
from typing import (
    Generic,
    List,
    Literal,
    Mapping,
    NamedTuple,
    NewType,
    Optional,
    TypeVar,
    Union,
)

import pydantic
from orquestra.workflow_shared.dates import Instant
from orquestra.workflow_shared.orqdantic import BaseModel
from orquestra.workflow_shared.schema.ir import WorkflowDef
from orquestra.workflow_shared.schema.workflow_run import (
    ProjectId,
    RunStatus,
    State,
    TaskRun,
    WorkflowRun,
    WorkflowRunMinimal,
    WorkflowRunSummary,
    WorkspaceId,
)
from typing_extensions import Annotated

WorkflowDefID = str
WorkflowRunID = str
TaskRunID = str
TaskInvocationID = str
WorkflowRunArtifactID = str
WorkflowRunResultID = str


# --- Generic responses and pagination ---

DataT = TypeVar("DataT")
MetaT = TypeVar("MetaT")


class Pagination(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/259481b9240547bccf4fa40df4e92bf6c617a25f/openapi/src/schemas/MetaSuccessPaginated.yaml.
    """  # noqa: D205, D212

    nextPageToken: str


class Response(BaseModel, Generic[DataT, MetaT]):
    """A generic to help with the structure of driver responses."""

    data: DataT
    meta: Optional[MetaT] = None


class MetaEmpty(BaseModel):
    pass


class ErrorCode(IntEnum):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/59eb3985151a813c69fec5a4777e8ed5c9a1f718/openapi/src/resources/workflow-runs.yaml#L59.
    """  # noqa: D205, D212

    REQUEST_BODY_INVALID_JSON = 1
    WORKFLOW_DEF_ID_MISSING = 2
    RESOURCE_REQUEST_INVALID = 3
    SDK_VERSION_UNSUPPORTED = 4
    WORKFLOW_DEF_ID_NOT_UUID = 5
    WORKFLOW_DEF_NOT_FOUND = 6


class Error(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/2b3534/openapi/src/schemas/Error.yaml.
    """  # noqa: D205, D212

    code: Optional[int] = None
    message: str
    detail: str


# --- Workflow Definitions ---


class CreateWorkflowDefResponse(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/2b3534/openapi/src/responses/CreateWorkflowDefinitionResponse.yaml.
    """  # noqa: D205, D212

    id: WorkflowDefID


class GetWorkflowDefResponse(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/cb61512e9f3da24addd933c7259aa4584ab04e4f/openapi/src/schemas/WorkflowDefinition.yaml.
    """  # noqa: D205, D212

    id: WorkflowDefID
    created: Instant
    owner: str
    workflow: WorkflowDef
    workspaceId: WorkspaceId
    project: ProjectId
    sdkVersion: str


class ListWorkflowDefsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/cdb667ef6d1053876250daff27e19fb50374c0d4/openapi/src/resources/workflow-definitions.yaml#L8.
    """  # noqa: D205, D212

    pageSize: Optional[int] = None
    pageToken: Optional[str] = None


class CreateWorkflowDefsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/dc8a2a37d92324f099afefc048f6486a5061850f/openapi/src/resources/workflow-definitions.yaml#L39.
    """  # noqa: D205, D212

    workspaceId: Optional[str] = None
    projectId: Optional[str] = None


ListWorkflowDefsResponse = List[GetWorkflowDefResponse]

# --- Workflow Runs ---


class StateResponse(str, Enum):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/RunStatus.yaml#L7.
    """  # noqa: D205, D212

    WAITING = "WAITING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, value):
        return cls.UNKNOWN


class RunStatusResponse(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/RunStatus.yaml#L1.
    """  # noqa: D205, D212

    state: StateResponse
    startTime: Optional[Instant] = None
    endTime: Optional[Instant] = None

    def to_ir(self) -> RunStatus:
        return RunStatus(
            state=State(self.state),
            start_time=self.startTime,
            end_time=self.endTime,
        )


class TaskRunResponse(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L17.
    """  # noqa: D205, D212

    id: TaskRunID
    invocationId: TaskInvocationID
    status: Optional[RunStatusResponse] = None

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


class MinimalWorkflowRunResponse(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L1.
    """  # noqa: D205, D212

    id: WorkflowRunID
    definitionId: WorkflowDefID

    def to_ir(self, workflow_def: WorkflowDef) -> WorkflowRunMinimal:
        return WorkflowRunMinimal(
            id=self.id,
            workflow_def=workflow_def,
        )


class WorkflowRunSummaryResponse(BaseModel):
    """Contains all of the information needed to give a basic overview of the workflow.

    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L1
    """

    id: WorkflowRunID
    definitionId: WorkflowDefID
    status: RunStatusResponse
    createTime: str
    owner: str
    totalTasks: int
    completedTasks: int
    dryRun: bool

    def to_ir(self) -> WorkflowRunSummary:
        return WorkflowRunSummary(
            id=self.id,
            status=self.status.to_ir(),
            owner=self.owner,
            total_task_runs=self.totalTasks,
            completed_task_runs=self.completedTasks,
            dry_run=self.dryRun,
        )


class WorkflowRunResponse(MinimalWorkflowRunResponse):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/schemas/WorkflowRun.yaml#L1.
    """  # noqa: D205, D212

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


class Resources(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/580c8d8835b1cccd085ea716c514038e85eb28d7/openapi/src/schemas/Resources.yaml.
    """  # noqa: D205, D212

    # If this schema is changed, the documentation in
    # docs/guides/ce-resource-management.rst should also be updated.

    nodes: Optional[int] = None
    cpu: Optional[str] = pydantic.Field(
        pattern=r"^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
    )
    memory: Optional[str] = pydantic.Field(
        pattern=r"^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
    )
    gpu: Optional[str] = pydantic.Field(pattern="^[01]+$")


class HeadNodeResources(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/ac1e97ea00fc3526c93187a1da02170bff45b74f/openapi/src/schemas/HeadNodeResources.yaml.
    """  # noqa: D205, D212

    cpu: Optional[str] = pydantic.Field(
        pattern=r"^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
    )
    memory: Optional[str] = pydantic.Field(
        pattern=r"^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
    )


class CreateWorkflowRunRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/ac1e97ea00fc3526c93187a1da02170bff45b74f/openapi/src/schemas/CreateWorkflowRunRequest.yaml.
    """  # noqa: D205, D212

    workflowDefinitionID: WorkflowDefID
    resources: Resources
    dryRun: bool
    headNodeResources: Optional[HeadNodeResources] = None


class CreateWorkflowRunResponse(BaseModel):
    """Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/2e999a76019e8f8de8082409daddf7789dc2f430/pkg/server/server.go#L376.
    """  # noqa: D205, D212

    id: WorkflowRunID


class ListWorkflowRunsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/c52013c0f4df066159fc32ad38d489b3eaff5850/openapi/src/resources/workflow-runs.yaml#L14.
    """  # noqa: D205, D212

    workflowDefinitionID: Optional[WorkflowDefID] = None
    pageSize: Optional[int] = None
    pageToken: Optional[str] = None
    workspaceId: Optional[WorkspaceId] = None
    projectId: Optional[ProjectId] = None
    maxAge: Optional[int] = None
    state: Optional[str] = None


ListWorkflowRunsResponse = List[MinimalWorkflowRunResponse]

ListWorkflowRunSummariesResponse = List[WorkflowRunSummaryResponse]


class GetWorkflowRunResponse(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/workflow-run.yaml#L17.
    """  # noqa: D205, D212

    data: WorkflowRunResponse


class TerminateWorkflowRunRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/873437f8157226c451220306a6ce90c80e8c8f9e/openapi/src/resources/workflow-run-terminate.yaml#L12.
    """  # noqa: D205, D212

    force: Optional[bool] = None


# --- Workflow Artifacts ---


class GetWorkflowRunArtifactsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/artifacts.yaml#L10.
    """  # noqa: D205, D212

    workflowRunId: WorkflowRunID


GetWorkflowRunArtifactsResponse = Mapping[TaskRunID, List[WorkflowRunArtifactID]]

# --- Workflow Results ---


class GetWorkflowRunResultsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/run-results.yaml#L10.
    """  # noqa: D205, D212

    workflowRunId: WorkflowRunID


GetWorkflowRunResultsResponse = List[WorkflowRunResultID]


# --- Logs ---


class GetWorkflowRunLogsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/34eba4253b56266772795a8a59d6ec7edf88c65a/openapi/src/resources/workflow-run-logs.yaml.
    """  # noqa: D205, D212

    workflowRunId: WorkflowRunID


class GetTaskRunLogsRequest(BaseModel):
    """
    Implements:
    https://github.com/zapatacomputing/workflow-driver/blob/c7685a579eca1f9cb3eb27e2a8c2a9757a3cd021/openapi/src/resources/task-run-logs.yaml.
    """  # noqa: D205, D212

    workflowRunId: WorkflowRunID
    taskInvocationId: TaskInvocationID


class CommonResourceMeta(BaseModel):
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


class ResourceIdentifier(BaseModel):
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


# --- Logs ---

# CE endpoints for logs return a compressed archive. Inside, there's a single file. In
# it, there's a sequence of Fluent Bit "events" split into newline-separated "sections".
# Each "section" is JSON-decodable into a list of "events".
#
# Each "event" is a pair of [timestamp, message].
#
# The timestamp is a float unix epoch timestamp of the time when the log line was
# *indexed* by the Platform-side log service. This is different from the time when the
# log line was emitted.
#
# The message contains a single line from a file produced by Ray + some metadata about
# the log source.
#
# The CE API returns a single archived file called "step-logs". After unarchiving, this
# file contains newline-separated chunks. Each chunk is a JSON-encoded list of events.


LogFilename = NewType("LogFilename", str)
RayFilename = NewType("RayFilename", str)


class WorkflowLogMessage(BaseModel):
    """Represents a single line indexed by the server side log service.

    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/972aaa3ca75780a52d01872bc294be419a761209/openapi/src/resources/workflow-run-logs.yaml#L25.

    The name is borrowed from Fluent Bit nomenclature:
    https://docs.fluentbit.io/manual/concepts/key-concepts#event-format.
    """

    log: str
    """
    Single line content.
    """

    ray_filename: RayFilename
    """
    Server-side file path of the indexed file.
    """

    tag: str
    """
    An identifier in the form of "workflow.logs.ray.<workflow run ID>".
    """


class WorkflowLogEvent(NamedTuple):
    """A pair of ``[timestamp, message]``.

    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/972aaa3ca75780a52d01872bc294be419a761209/openapi/src/resources/workflow-run-logs.yaml#L18
    """

    timestamp: float
    """
    Unix timestamp in seconds with fraction for the moment when a log line is exported
    from Ray system to Orquestra.
    It does not necessarily correspond to the particular time that the message is
    logged by Ray runtime.
    """

    message: WorkflowLogMessage
    """A single indexed log line."""


WorkflowLogSection = List[WorkflowLogEvent]


class TaskLogMessage(BaseModel):
    """Represents a single line indexed by the server side log service.

    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/f55bd3d5203a42ee42fc2405cd44cfaa94993f4a/openapi/src/resources/task-run-logs.yaml#L16

    The name is borrowed from Fluent Bit nomenclature:
    https://docs.fluentbit.io/manual/concepts/key-concepts#event-format.
    """

    log: str
    """Single line content."""

    log_filename: LogFilename
    """Server-side file path of the indexed file."""

    tag: str
    """An identifier in the form of:

    "workflow.logs.ray.<workflow run ID>.<task invocation ID>"
    """


class TaskLogEvent(NamedTuple):
    """A pair of ``[timestamp, task log message]``.

    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/c7685a579eca1f9cb3eb27e2a8c2a9757a3cd021/openapi/src/resources/task-run-logs.yaml#L16
    """

    timestamp: float
    """
    Unix timestamp in seconds with fraction for the moment when a log line is exported
    from Ray system to Orquestra.
    It does not necessarily correspond to the particular time that the message is
    logged by Ray runtime.
    """

    message: TaskLogMessage
    """
    A single indexed log line.
    """


TaskLogSection = List[TaskLogEvent]


# --- System Logs ---


class SystemLogSourceType(str, Enum):
    """Types of sources that can emit system logs."""

    RAY_HEAD_NODE = "RAY_HEAD_NODE"
    RAY_WORKER_NODE = "RAY_WORKER_NODE"
    K8S_EVENT = "K8S_EVENT"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, *args, **kwargs):
        return cls.UNKNOWN


class K8sEventLog(BaseModel):
    """A system-level log line produced by a K8S event."""

    tag: str

    log: dict
    """
    The keys in this dictionary are determined by Kubernetes.
    """

    source_type: Literal[SystemLogSourceType.K8S_EVENT] = SystemLogSourceType.K8S_EVENT


class RayHeadNodeEventLog(BaseModel):
    """A system-level log line produced by a Ray head node event."""

    tag: str

    log: str

    source_type: Literal[
        SystemLogSourceType.RAY_HEAD_NODE
    ] = SystemLogSourceType.RAY_HEAD_NODE


class RayWorkerNodeEventLog(BaseModel):
    """A system-level log line produced by a Ray head node event."""

    tag: str

    log: str

    source_type: Literal[
        SystemLogSourceType.RAY_WORKER_NODE
    ] = SystemLogSourceType.RAY_WORKER_NODE


class UnknownEventLog(BaseModel):
    """Fallback option - the event type is unknown, so display the message as a str."""

    tag: str

    log: str

    source_type: Literal[SystemLogSourceType.UNKNOWN] = SystemLogSourceType.UNKNOWN


SysLog = Annotated[
    Union[K8sEventLog, RayHeadNodeEventLog, RayWorkerNodeEventLog, UnknownEventLog],
    pydantic.Field(discriminator="source_type"),
]


class SysMessage(NamedTuple):
    """A pair of ``[timestamp, syslog]``.

    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/92d9ff32189c580fd0a2ff6eec03cc977fd01502/openapi/src/resources/workflow-run-system-logs.yaml#L2
    """

    timestamp: float
    """
    Unix timestamp in seconds with fraction for the moment when a log line is exported
    from system to Orquestra.
    It does not necessarily correspond to the particular time that the message is
    logged by the system.
    """

    message: SysLog


SysSection = List[SysMessage]
