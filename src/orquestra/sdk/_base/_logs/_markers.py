################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Specialized log lines we emit in Orquestra logs.


``TaskStartMarker`` and ``TaskEndMarker`` are record types that were previously
used to hold the information we emit in the log markers.
They're dataclasses and not pydantic models because we're not just using plain
JSON: we're using prefix markers.
"""
import json
import re
import sys
import traceback
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from .. import _dates

ORQ_MARKER_PREFIX = "ORQ-MARKER:"
ORQ_MARKER_PATTERN = re.compile(re.escape(ORQ_MARKER_PREFIX) + r"(.+)")


UNKNOWN_WF_RUN_ID = "unknown-wf-run-id"
UNKNOWN_TASK_INV_ID = "unknown-task-inv-id"


@contextmanager
def redirected_io(
    logs_dir: Path,
    wf_run_id: t.Optional[WorkflowRunId],
    task_inv_id: t.Optional[TaskInvocationId],
):
    wf_run_id = wf_run_id or UNKNOWN_WF_RUN_ID
    task_inv_id = task_inv_id or UNKNOWN_TASK_INV_ID

    # wurlitzer doesn't support Windows.
    # Instead, we turn this into a no-op
    # We need to yield to match the generator interface.
    if sys.platform.startswith("win32"):
        yield
        return

    # We need to defer this import until after we're sure Windows cannot
    # reach it.
    # wurlitzer does not have type annotations
    import wurlitzer  # type: ignore

    log_path = logs_dir / "wf" / wf_run_id / "task"
    out_path = log_path / f"{task_inv_id}.out"
    err_path = log_path / f"{task_inv_id}.err"

    log_path.mkdir(parents=True, exist_ok=True)

    with open(out_path, "a") as out_f, open(err_path, "a") as err_f:
        with wurlitzer.pipes(stdout=out_f, stderr=err_f):
            try:
                yield
            except Exception as e:
                traceback.print_exception(type(e), e, e.__traceback__)
                raise


@dataclass(frozen=True)
class TaskStartMarker:
    """
    Deprecated: Newer workflows do not use markers.
    """

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
    """
    Deprecated: Newer workflows do not use markers.
    """

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
    Deprecated: Newer workflows do not use markers.

    Attempts to interpret a single log line as a marker.

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
