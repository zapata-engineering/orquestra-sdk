################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Models for responses from the CLI.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t

from pydantic import Field
from typing_extensions import Annotated

from .._base._storage import OrquestraBaseModel
from .ir import ArtifactFormat


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


class ResponseMetadata(OrquestraBaseModel):
    success: bool
    code: ResponseStatusCode
    message: str


class JSONResult(OrquestraBaseModel):
    # Output value dumped to a flat JSON string.
    value: str
    serialization_format: t.Literal[ArtifactFormat.JSON] = ArtifactFormat.JSON


class PickleResult(OrquestraBaseModel):
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


class ComputeEngineWorkflowResult(OrquestraBaseModel):
    results: t.Tuple[WorkflowResult, ...]
    type: t.Literal["ComputeEngineWorkflowResult"] = "ComputeEngineWorkflowResult"


class ServiceResponse(OrquestraBaseModel):
    name: str
    is_running: bool
    info: t.Optional[str]
