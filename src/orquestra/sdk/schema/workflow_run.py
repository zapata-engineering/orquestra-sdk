################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Workflow Run model.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t
from datetime import datetime

from pydantic import BaseModel

from orquestra.sdk.schema.ir import TaskInvocationId, WorkflowDef

WorkflowRunId = str
TaskRunId = str


class State(enum.Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"
    ERROR = "ERROR"


class RunStatus(BaseModel):
    state: State
    start_time: t.Optional[datetime]
    end_time: t.Optional[datetime]


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
