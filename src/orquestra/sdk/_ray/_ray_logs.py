################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""
Class to get logs from Ray for particular Workflow, both historical and live.
"""
import json
import typing as t

# from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pydantic

from orquestra.sdk._base._logs import _regrouping
from orquestra.sdk._base._logs._interfaces import WorkflowLogs
from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import TaskRunId, WorkflowRunId

from . import _client


class WFLog(pydantic.BaseModel):
    """
    Log line produced inside a task run. Parsed. Coupled with the logging format set in
    ``orquestra.sdk._base._log_adater``.
    """

    # Timezone-aware date+time.
    timestamp: datetime
    # Log level name, consistent with Python logging.
    level: str
    filename: str
    # User-specified message string.
    message: str
    # ID of the workflow that was run when this line was produced.
    wf_run_id: t.Optional[WorkflowRunId]
    # ID of the task invocation (node inside the workflow def graph).
    task_inv_id: t.Optional[TaskInvocationId]
    # ID of the task that was run when this line was produced.
    task_run_id: t.Optional[TaskRunId]


def _parse_obj_or_none(model_class, json_dict):
    try:
        return model_class.parse_obj(json_dict)
    except pydantic.error_wrappers.ValidationError:
        return None


def parse_log_line(raw_line: bytes) -> t.Optional[WFLog]:
    line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
    if line.startswith(_client.LogPrefixActorName) or line.startswith(
        _client.LogPrefixTaskName
    ):
        return None

    try:
        json_obj = json.loads(line)
    except (ValueError, TypeError):
        return None

    # Fixes log lines that are valid JSON literals but not objects
    if not isinstance(json_obj, dict):
        return None

    return _parse_obj_or_none(WFLog, json_obj)


def _iter_logs_paths(ray_temp: Path) -> t.Iterator[Path]:
    seen_paths: t.MutableSet[Path] = set()
    for file_path in ray_temp.glob("session_*/logs/*"):
        real_path = file_path.resolve()
        if real_path in seen_paths:
            continue

        yield real_path

        seen_paths.add(real_path)


def iter_user_log_paths(ray_temp: Path) -> t.Iterator[Path]:
    return filter(_regrouping.is_worker, _iter_logs_paths(ray_temp))


def iter_env_log_paths(ray_temp: Path) -> t.Iterator[Path]:
    return filter(_regrouping.is_env_setup, _iter_logs_paths(ray_temp))


def _iter_log_lines(paths: t.Iterable[Path]) -> t.Iterator[bytes]:
    for path in paths:
        with path.open("rb") as f:
            yield from f


class DirectRayReader:
    """
    Directly reads log files produced by Ray.
    Implements the ``LogReader`` interface.

    Requires ``ray_temp`` to be consistent with the path passed when initializing the
    Ray cluster. For example, if the cluster was started with::

        ray start --head  \
                --temp-dir="~/.orquestra/ray" \
                --storage="~/.orquestra/ray_storage"

    the ``ray_temp`` needs to be ``~/.orquestra/ray``.
    """

    def __init__(self, ray_temp: Path):
        """
        Args:
            ray_temp: directory where Ray keeps its data, like ``~/.orquestra/ray``.
        """
        self._ray_temp = ray_temp

    def _get_user_log_lines(self) -> t.Sequence[WFLog]:
        log_paths = iter_user_log_paths(self._ray_temp)
        log_line_bytes = _iter_log_lines(log_paths)

        return [
            parsed_log
            for log_line in log_line_bytes
            if (parsed_log := parse_log_line(raw_line=log_line)) is not None
        ]

    def _get_env_setup_lines(self) -> t.Sequence[str]:
        log_paths = iter_env_log_paths(self._ray_temp)
        log_line_bytes = _iter_log_lines(log_paths)

        return [line.decode() for line in log_line_bytes]

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:
        user_logs = self._get_user_log_lines()

        task_logs = [
            log
            for log in user_logs
            if log.wf_run_id == wf_run_id and log.task_inv_id == task_inv_id
        ]
        return [log.json() for log in task_logs]

    def get_workflow_logs(self, wf_run_id: WorkflowRunId) -> WorkflowLogs:
        user_logs = self._get_user_log_lines()

        logs_dict: t.Dict[TaskInvocationId, t.List[str]] = {}
        for log in user_logs:
            if log.wf_run_id != wf_run_id:
                continue

            if log.task_inv_id is None:
                continue

            logs_dict.setdefault(log.task_inv_id, []).append(log.json())

        env_setup = self._get_env_setup_lines()

        return WorkflowLogs(
            per_task=logs_dict,
            env_setup=env_setup,
            system=[],
            other=[],
        )
