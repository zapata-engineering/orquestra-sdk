################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Workflow Run model.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t
import warnings

from orquestra.sdk._base._dates import Instant
from orquestra.sdk.schema.ir import TaskInvocationId, WorkflowDef

from .._base._storage import BaseModel

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
    def _missing_(cls, value):
        return cls.UNKNOWN

    def is_completed(self) -> bool:
        """Check whether the state indicates that execution of the wf run has ended.

        Returns:
            True if the execution has ended for any reason, False if the workflow run
                is waiting or running.
        """
        if self == self.UNKNOWN:
            warnings.warn("Cannot determine the workflow run state.")

        return self in (
            self.SUCCEEDED,
            self.TERMINATED,
            self.FAILED,
            self.ERROR,
            self.KILLED,
        )


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
    """A WorkflowRun that only contains the ID."""

    id: WorkflowRunId


class WorkflowRunMinimal(WorkflowRunOnlyID):
    """The minimal amount of information to create a WorkflowRun in the public API."""

    workflow_def: WorkflowDef


class WorkflowRun(WorkflowRunMinimal):
    """A full workflow run with TaskRuns and WorkflowRun status."""

    task_runs: t.List[TaskRun]
    status: RunStatus
    message: t.Optional[str] = None


class WorkflowRunSummary(WorkflowRunOnlyID):
    """A summary overview of a workflow run."""

    status: RunStatus
    owner: t.Optional[str]
    """
    The email address of the account that submitted this workflow run.

    For local runs, this field is not populated.
    """
    total_task_runs: int
    completed_task_runs: int
    dry_run: t.Optional[bool]

    @staticmethod
    def from_workflow_run(wf: WorkflowRun) -> "WorkflowRunSummary":
        return WorkflowRunSummary(
            id=wf.id,
            status=wf.status,
            owner=None,
            total_task_runs=len(wf.workflow_def.task_invocations),
            completed_task_runs=sum(
                1 for tr in wf.task_runs if tr.status.state == State.SUCCEEDED
            ),
            dry_run=None,
        )
