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


class WorkflowRun(BaseModel):
    id: WorkflowRunId
    workflow_def: t.Optional[WorkflowDef]
    task_runs: t.List[TaskRun]
    status: RunStatus


class WorkflowRunMinimal(BaseModel):
    id: WorkflowRunId
