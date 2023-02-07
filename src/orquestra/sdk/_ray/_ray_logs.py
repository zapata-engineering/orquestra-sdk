################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Class to get logs from Ray for particular Workflow, both historical and live.
"""
import glob
import json
import os
import typing as t
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pydantic

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import TaskRunId, WorkflowRunId

from . import _client


@dataclass
class _LogFileInfo:
    """
    Keeps track where we stopped reading a log file the last time we looked at it.

    Mutated every time we read a log file.
    """

    # Absolute filepath without symlinks.
    filepath: str
    size_when_last_opened: int
    # Last read index.
    file_position: int


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


class _RayLogs:
    """
    Directly reads log files produced by Ray, bypassing the fluent-bit service. Returns
    log lines without parsing the content.

    Requires ``ray_temp`` to be consistent with the path passed when initializing the
    Ray cluster. For example, if the cluster was started with::

        ray start --head  \
                --temp-dir="~/.orquestra/ray" \
                --storage="~/.orquestra/ray_storage"

    the ``ray_temp`` needs to be ``~/.orquestra/ray``.
    """

    def __init__(self, ray_temp: Path):
        self.ray_temp = ray_temp

        self.log_filenames: t.MutableSet[str] = set()
        self.log_file_infos: t.MutableSequence[_LogFileInfo] = []

        self.logs_by_wf: t.MutableMapping[WorkflowRunId, t.MutableSequence[WFLog]] = {}
        self.logs_by_task: t.MutableMapping[TaskRunId, t.MutableSequence[WFLog]] = {}

    def _update_log_filenames(self):
        path_glob = self.ray_temp / "session**" / "logs" / "worker*[.out|.err]"
        log_file_paths = glob.glob(str(path_glob))
        for file_path in log_file_paths:
            real_path = os.path.realpath(file_path)
            if os.path.isfile(file_path) and real_path not in self.log_filenames:
                self.log_filenames.add(real_path)
                self.log_file_infos.append(
                    _LogFileInfo(
                        filepath=real_path,
                        size_when_last_opened=0,
                        file_position=0,
                    )
                )

    def read_new_logs(self):
        """
        Reads log files. Returns all lines produced since last call of this method.

        Updates `self`'s file infos and parsed log objects.

        Yields:
            Newly parsed log lines that haven't been seen before, as bytes. Each yield
            relates to one line, as a bytes.
        """
        self._update_log_filenames()

        log_lines: t.List[bytes] = []

        for file_info in self.log_file_infos:
            file_size = os.path.getsize(file_info.filepath)
            if file_size > file_info.size_when_last_opened:
                file_info.size_when_last_opened = file_size

                with open(file_info.filepath, "rb") as f:
                    f.seek(file_info.file_position)

                    yield from f

                    file_info.file_position = f.tell()

        return log_lines


class DirectRayReader:
    """
    Adapter that wraps RayLogs in the orquestra.sdk._base.abc.LogReader interface.
    """

    def __init__(self, ray_temp: Path):
        """
        Args:
            ray_temp: directory where Ray keeps its data, like ``~/.orquestra/ray``.
        """
        self._ray_temp = ray_temp

    def _get_parsed_logs(self) -> t.Iterable[WFLog]:
        wrapped = _RayLogs(ray_temp=self._ray_temp)
        log_line_bytes = wrapped.read_new_logs()

        return [
            parsed_log
            for log_line in log_line_bytes
            if (parsed_log := parse_log_line(raw_line=log_line)) is not None
        ]

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:

        parsed_logs = self._get_parsed_logs()

        task_logs = [
            log
            for log in parsed_logs
            if log.wf_run_id == wf_run_id and log.task_inv_id == task_inv_id
        ]
        return [log.json() for log in task_logs]

    def get_workflow_logs(
        self, wf_run_id: WorkflowRunId
    ) -> t.Dict[TaskInvocationId, t.List[str]]:

        parsed_logs = self._get_parsed_logs()

        logs_dict: t.Dict[TaskInvocationId, t.List[str]] = {}
        for log in parsed_logs:
            if log.wf_run_id != wf_run_id:
                continue

            if log.task_inv_id is None:
                continue

            logs_dict.setdefault(log.task_inv_id, []).append(log.json())

        return logs_dict
