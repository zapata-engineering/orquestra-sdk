################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Models for responses from the CLI.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t

from pydantic import BaseModel, Field
from typing_extensions import Annotated

from .configs import RuntimeConfiguration
from .ir import (
    ArtifactFormat,
    Import,
    ImportId,
    TaskDef,
    TaskDefId,
    TaskInvocationId,
    WorkflowDef,
)
from .workflow_run import WorkflowRun, WorkflowRunOnlyID


class ResponseFormat(enum.Enum):
    PLAIN_TEXT = "text"
    JSON = "json"
    DEFAULT = PLAIN_TEXT


class ResponseStatusCode(enum.Enum):
    UNKNOWN_ERROR = -1
    OK = 0
    PERMISSION_ERROR = 1
    NOT_FOUND = 2
    NOT_A_DIRECTORY = 3
    PROJECT_EXISTS = 4
    INVALID_PROJECT = 5
    INVALID_TASK_DEF = 6
    INVALID_WORKFLOW_DEF = 7
    INVALID_WORKFLOW_DEFS_SYNTAX = 8
    INVALID_WORKFLOW_RUN = 9
    WORKFLOW_RUN_NOT_FOUND = 10
    CONNECTION_ERROR = 11
    UNAUTHORIZED = 12
    SERVICES_ERROR = 13
    INVALID_CLI_COMMAND_ERROR = 14
    USER_CANCELLED = 15


class ResponseMetadata(BaseModel):
    success: bool
    code: ResponseStatusCode
    message: str


class ErrorResponse(BaseModel):
    meta: ResponseMetadata


class GetWorkflowDefResponse(BaseModel):
    meta: ResponseMetadata
    workflow_defs: t.List[WorkflowDef]


class GetTaskDefResponse(BaseModel):
    meta: ResponseMetadata
    task_defs: t.Dict[TaskDefId, TaskDef]
    imports: t.Dict[ImportId, Import]


class SubmitWorkflowDefResponse(BaseModel):
    meta: ResponseMetadata
    workflow_runs: t.List[WorkflowRunOnlyID]


class GetWorkflowRunResponse(BaseModel):
    meta: ResponseMetadata
    workflow_runs: t.List[WorkflowRun]


class ListWorkflowRunsResponse(BaseModel):
    meta: ResponseMetadata
    workflow_runs: t.List[WorkflowRun]
    filters: dict


class StopWorkflowRunResponse(BaseModel):
    meta: ResponseMetadata


class SetDefaultConfig(BaseModel):
    meta: ResponseMetadata
    default_config_name: str


class GetDefaultConfig(BaseModel):
    meta: ResponseMetadata
    default_config_name: str


class SetTokenResponse(BaseModel):
    """
    Used by 'orq set token'.
    """

    meta: ResponseMetadata
    # Config content after the action.
    result_config: RuntimeConfiguration


class CreateConfigResponse(BaseModel):
    """
    Used by `orq create-config`
    """

    meta: ResponseMetadata
    written_config: RuntimeConfiguration


class ServicesNotRunningResponse(BaseModel):
    meta: ResponseMetadata


class ServicesStartedResponse(BaseModel):
    meta: ResponseMetadata


class ServicesStoppedResponse(BaseModel):
    meta: ResponseMetadata


class ServicesStatusResponse(BaseModel):
    meta: ResponseMetadata

    # True if a local Ray cluster is running in the background.
    ray_running: bool

    # True if a fluentbit service is running in the background.
    fluentbit_running: bool


class JSONResult(BaseModel):
    # Output value dumped to a flat JSON string.
    value: str
    serialization_format: t.Literal[ArtifactFormat.JSON] = ArtifactFormat.JSON


class PickleResult(BaseModel):
    # Output value dumped to a pickle byte string, encoded as base64, and split into
    # chunks. Chunking is required because some JSON parsers have limitation on max
    # string field length.
    chunks: t.List[str]
    serialization_format: t.Literal[
        ArtifactFormat.ENCODED_PICKLE
    ] = ArtifactFormat.ENCODED_PICKLE


WorkflowResult = Annotated[
    t.Union[JSONResult, PickleResult], Field(discriminator="serialization_format")
]


class GetWorkflowRunResultsResponse(BaseModel):
    meta: ResponseMetadata
    workflow_run_id: str
    workflow_results: t.List[WorkflowResult]


class GetArtifactsResponse(BaseModel):
    meta: ResponseMetadata

    artifacts: t.Dict[TaskInvocationId, t.Any]
    """Artifact values returned from tasks. Each key-value pair in this dict
    corresponds to a single task invocation inside a workflow. The dicts value
    is a plain artifact value, as returned from the task. If a task returns
    multiple values, the dicts value is a tuple."""


class GetLogsResponse(BaseModel):
    meta: ResponseMetadata
    logs: t.List[t.Union[str, BaseModel]]


class ServiceResponse(BaseModel):
    name: str
    is_running: bool
    info: t.Optional[str]
