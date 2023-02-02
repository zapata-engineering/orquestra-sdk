################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Class to get logs from Ray for particular Workflow, both historical and live.
"""
import glob
import json
import os
import time
import typing as t
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pydantic

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
    # ID of the task that was run when this line was produced.
    task_run_id: t.Optional[TaskRunId]


def _parse_obj_or_none(model_class, json_dict):
    try:
        return model_class.parse_obj(json_dict)
    except pydantic.error_wrappers.ValidationError:
        return None


def parse_log_line(
    raw_line: bytes, searched_id: t.Union[WorkflowRunId, TaskRunId, None]
) -> t.Optional[WFLog]:
    line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
    if line.startswith(_client.LogPrefixActorName) or line.startswith(
        _client.LogPrefixTaskName
    ):
        return None

    try:
        json_dict = json.loads(line)
    except (ValueError, TypeError):
        return None

    if searched_id is not None:
        if (
            json_dict.get("wf_run_id") == searched_id
            or json_dict.get("task_run_id") == searched_id
        ):
            return _parse_obj_or_none(WFLog, json_dict)
        else:
            return None
    else:
        return _parse_obj_or_none(WFLog, json_dict)


class _RayLogs:
    """
    Directly reads log files produced by Ray, bypassing the fluent-bit service.

    Requires ``ray_temp`` to be consistent with the path passed when initializing the
    Ray cluster. For example, if the cluster was started with::

        ray start --head  \
                --temp-dir="~/.orquestra/ray" \
                --storage="~/.orquestra/ray_storage"

    the ``ray_temp`` needs to be ``~/.orquestra/ray``.
    """

    def __init__(self, ray_temp: Path, workflow_or_task_run_id: t.Optional[str] = None):
        self.ray_temp = ray_temp
        self.workflow_or_task_run_id = workflow_or_task_run_id
        self.log_filenames: t.MutableSet[str] = set()
        self.log_file_infos: t.MutableSequence[_LogFileInfo] = []

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

    def _read_log_files(self) -> t.Sequence[WFLog]:
        logs: t.List[WFLog] = []

        self._update_log_filenames()

        for file_info in self.log_file_infos:
            file_size = os.path.getsize(file_info.filepath)
            if file_size > file_info.size_when_last_opened:
                file_info.size_when_last_opened = file_size

                with open(file_info.filepath, "rb") as f:
                    f.seek(file_info.file_position)
                    for next_line in f:
                        parsed_log = parse_log_line(
                            next_line, searched_id=self.workflow_or_task_run_id
                        )
                        if parsed_log is not None:
                            logs.append(parsed_log)
                    file_info.file_position = f.tell()

        return logs

    def iter_logs(self):
        while True:
            parsed_logs = self._read_log_files()
            yield [log.json() for log in parsed_logs]
            time.sleep(2)

    def get_full_logs(self):
        parsed_logs = self._read_log_files()
        formatted = [log.json() for log in parsed_logs]
        # This is bad. The key should be a task invocation ID. To be fixed in the Jira
        # ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-676
        return {"logs": formatted}


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

    def get_full_logs(self, run_id: t.Optional[str] = None) -> t.Dict[str, t.List[str]]:
        wrapped = _RayLogs(ray_temp=self._ray_temp, workflow_or_task_run_id=run_id)
        return wrapped.get_full_logs()

    def iter_logs(self, run_id: t.Optional[str] = None) -> t.Iterator[t.Sequence[str]]:
        wrapped = _RayLogs(ray_temp=self._ray_temp, workflow_or_task_run_id=run_id)
        yield from wrapped.iter_logs()
