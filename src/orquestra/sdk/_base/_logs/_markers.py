################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Specialized log lines we emit in Orquestra logs.


``TaskStartMarker`` and ``TaskEndMarker`` are record types that hold the information we
emit in the log markers. They're dataclasses and not pydantic models because we're
not just using plain JSON: we're using prefix markers.
"""
import json
import re
import sys
import traceback
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from .. import _dates

ORQ_MARKER_PREFIX = "ORQ-MARKER:"
ORQ_MARKER_PATTERN = re.compile(re.escape(ORQ_MARKER_PREFIX) + r"(.+)")


@dataclass(frozen=True)
class TaskStartMarker:
    event = "task_start"
    wf_run_id: WorkflowRunId
    task_inv_id: TaskInvocationId
    timestamp: _dates.Instant

    @property
    def line(self) -> str:
        event = {
            "event": self.event,
            "timestamp": _dates.local_isoformat(self.timestamp),
            "wf_run_id": self.wf_run_id,
            "task_inv_id": self.task_inv_id,
        }
        return f"{ORQ_MARKER_PREFIX}{json.dumps(event)}"


@dataclass(frozen=True)
class TaskEndMarker:
    event = "task_end"
    wf_run_id: t.Optional[WorkflowRunId]
    task_inv_id: t.Optional[TaskInvocationId]
    timestamp: _dates.Instant

    @property
    def line(self) -> str:
        event = {
            "event": self.event,
            "timestamp": _dates.local_isoformat(self.timestamp),
            **({"wf_run_id": self.wf_run_id} if self.wf_run_id else {}),
            **({"task_inv_id": self.task_inv_id} if self.task_inv_id else {}),
        }
        return f"{ORQ_MARKER_PREFIX}{json.dumps(event)}"


Marker = t.Union[TaskStartMarker, TaskEndMarker]


def parse_line(line: str) -> t.Optional[Marker]:
    """
    Attemps to interpret a single log line as a marker.

    Returns:
        - Deserialized marker object with the marker event's content.
        - None if the line doesn't match the marker format.
    """
    if not line.startswith(ORQ_MARKER_PREFIX):
        return None

    if (match := ORQ_MARKER_PATTERN.match(line)) is None:
        return None

    event_dict = json.loads(match.group(1))
    try:
        if event_dict["event"] == TaskStartMarker.event:
            return TaskStartMarker(
                wf_run_id=event_dict["wf_run_id"],
                task_inv_id=event_dict["task_inv_id"],
                timestamp=_dates.from_isoformat(event_dict["timestamp"]),
            )
        elif event_dict["event"] == TaskEndMarker.event:
            return TaskEndMarker(
                wf_run_id=event_dict.get("wf_run_id"),
                task_inv_id=event_dict.get("task_inv_id"),
                timestamp=_dates.from_isoformat(event_dict["timestamp"]),
            )
        else:
            return None
    except KeyError:
        return None


def print_start(wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
    """
    Emits "task start" marker to stdout and stderr.

    Required for task-log correlation.
    """
    now = _dates.now()
    marker = TaskStartMarker(
        wf_run_id=wf_run_id, task_inv_id=task_inv_id, timestamp=now
    )
    print(marker.line)
    print(marker.line, file=sys.stderr)


def print_end(wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
    """
    Emits "task end" marker to stdout and stderr.

    Required for task-log correlation.
    """
    now = _dates.now()
    marker = TaskEndMarker(wf_run_id=wf_run_id, task_inv_id=task_inv_id, timestamp=now)
    print(marker.line)
    print(marker.line, file=sys.stderr)


@contextmanager
def printed_task_markers(
    wf_run_id: t.Optional[WorkflowRunId], task_inv_id: t.Optional[TaskInvocationId]
):
    """
    Emits "task start" and "task end" markers before and after the yielded block.
    Logs exceptions to stderr and rethrows.
    """
    # TODO: make the arguments non-optionals.
    # https://zapatacomputing.atlassian.net/browse/ORQSDK-530

    if wf_run_id and task_inv_id:
        print_start(wf_run_id, task_inv_id)

    try:
        yield
    except Exception as e:
        traceback.print_exception(type(e), e, e.__traceback__)
        raise
    finally:
        if wf_run_id and task_inv_id:
            print_end(wf_run_id, task_inv_id)
