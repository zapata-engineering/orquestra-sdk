################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Workflow Run model.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t

from pydantic import BaseModel

from orquestra.sdk._base._dates import Instant
from orquestra.sdk.schema.ir import TaskInvocationId, WorkflowDef

WorkflowRunId = str
TaskRunId = str
WorkspaceId = str
ProjectId = str


class State(enum.Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, _):
        return cls.UNKNOWN


class RunStatus(BaseModel):
    state: State
    start_time: t.Optional[Instant]
    end_time: t.Optional[Instant]


class TaskRun(BaseModel):
    id: TaskRunId
    invocation_id: TaskInvocationId
    status: RunStatus
    message: t.Optional[str] = None


class WorkflowRunOnlyID(BaseModel):
    """
    A WorkflowRun that only contains the ID
    """

    id: WorkflowRunId


class WorkflowRunMinimal(WorkflowRunOnlyID):
    """
    The minimal amount of information to create a WorkflowRun in the public API
    """

    workflow_def: WorkflowDef


class WorkflowRun(WorkflowRunMinimal):
    """
    A full workflow run with TaskRuns and WorkflowRun status
    """

    task_runs: t.List[TaskRun]
    status: RunStatus
