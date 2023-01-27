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
from pathlib import Path

from . import _client

LINE_FORMAT: str = "{log.timestamp}\t{log.level} {log.filename} -- {log.message.logs}\t[{log.message.id}]"  # noqa E501


@dataclass
class _LogFileInfo:
    filename: str
    size_when_last_opened: int
    file_position: int


@dataclass
class LogMessage:
    id: str
    logs: str


@dataclass
class StructuredLog:
    timestamp: str
    level: str
    filename: str
    message: LogMessage


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
            if os.path.isfile(file_path) and file_path not in self.log_filenames:
                self.log_filenames.add(file_path)
                self.log_file_infos.append(
                    _LogFileInfo(
                        filename=file_path,
                        size_when_last_opened=0,
                        file_position=0,
                    )
                )

    def _load_structured_log_record(self, record: str):
        data = json.loads(record)
        return StructuredLog(
            timestamp=data["timestamp"],
            level=data["level"],
            filename=data["filename"],
            message=LogMessage(data["message"]["run_id"], data["message"]["logs"]),
        )

    def _load_unstructured_log_record(self, record: str):
        ts_level_filename, logs = record.split(" -- ")
        timestamp, level_filename = ts_level_filename.split("\t")
        level, filename = level_filename.split(" ", 1)
        try:
            log_message, id = logs.split("\t")
        except ValueError:
            log_message = logs
            id = ""

        return StructuredLog(
            timestamp=timestamp,
            level=level,
            filename=filename,
            message=LogMessage(id=id.strip(" []"), logs=log_message),
        )

    def _refine_log_line(self, line):
        line = line.decode("utf-8", "replace").rstrip("\r\n")
        if line.startswith(_client.LogPrefixActorName) or line.startswith(
            _client.LogPrefixTaskName
        ):
            return
        elif (
            self.workflow_or_task_run_id is None
        ) or self.workflow_or_task_run_id in line:
            if line.startswith("{"):
                log = self._load_structured_log_record(line)
            else:
                log = self._load_unstructured_log_record(line)
            return LINE_FORMAT.format(log=log)

    def _read_log_files(self):
        lines = []

        self._update_log_filenames()

        for file_info in self.log_file_infos:
            file_size = os.path.getsize(file_info.filename)
            if file_size > file_info.size_when_last_opened:
                file_info.size_when_last_opened = file_size

                with open(file_info.filename, "rb") as f:
                    f.seek(file_info.file_position)
                    for next_line in f:
                        next_line = self._refine_log_line(next_line)
                        if next_line:
                            lines.append(next_line)
                    file_info.file_position = f.tell()

        return lines

    def iter_logs(self):
        while True:
            yield self._read_log_files()
            time.sleep(2)

    def get_full_logs(self):
        # This is bad. The key should be a task invocation ID. To be fixed in the Jira
        # ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-570
        return {"logs": self._read_log_files()}


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
