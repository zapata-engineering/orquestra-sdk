################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Class to query logs from Ray local runtime.
"""
import json
import pathlib
import typing as t

import pydantic

from orquestra.sdk.schema import workflow_run


class LogMessage(pydantic.BaseModel):
    run_id: str
    logs: str


class StructuredLog(pydantic.BaseModel):
    timestamp: str
    level: str
    filename: str
    message: LogMessage


RAY_WORKER_RUN_ID = "ray.worker"


class LogQueryService:
    def __init__(
        self,
        run_id: t.Optional[workflow_run.WorkflowRunId],
        logs_dir: pathlib.Path,
    ):
        self.run_id = run_id or RAY_WORKER_RUN_ID
        self.logs_dir = logs_dir
        self.logs_path = logs_dir / self.run_id

    def _load_structured_log_record(self, record):
        "Load log message from JSON"
        return StructuredLog(**json.loads(record))

    def _load_unstructured_log_record(self, record):
        "Load log message from raw Ray's log line."
        try:
            ts_level_filename, logs = record.split(" -- ")
            timestamp, level_filename = ts_level_filename.split("\t")
            level, filename = level_filename.split(" ", 1)

            return StructuredLog(
                timestamp=timestamp,
                level=level,
                filename=filename,
                message=LogMessage(run_id=self.run_id, logs=logs),
            )

        except ValueError:
            return

    def _if_json(self, data):
        """Check if string is a valid JSON"""
        try:
            _ = json.loads(data)
        except (json.JSONDecodeError, ValueError, TypeError):
            return False
        return True

    def get_full_structured_logs(self):
        full_structured_logs = []
        with open(self.logs_path, "r") as f:
            for line in f:
                data = json.loads(line.replace(f"{self.run_id}: ", ""))
                timestamp_ms, log_record = data
                if self._if_json(log_record["log"]) is True:
                    model = self._load_structured_log_record(log_record["log"])
                else:
                    model = self._load_unstructured_log_record(log_record["log"])
                full_structured_logs.append(model)

        return list(filter(None, full_structured_logs))

    def get_message_logs(self):
        structured_logs = self.get_full_structured_logs()
        return [model.message.logs for model in structured_logs]

    def get_full_logs(self):
        return {self.run_id: self.get_full_structured_logs()}


class FluentbitReader:
    """
    Adapter that wraps LogQueryService in the orquestra.sdk._base.abc.LogReader
    interface.
    """

    def __init__(self, logs_dir: pathlib.Path):
        """
        Args:
            logs_dir: directory that contains log files to be read, e.g.
                ``~/.orquestra/log``.
        """
        self._logs_dir = logs_dir

    def get_full_logs(self, run_id: t.Optional[str] = None) -> t.Dict[str, t.List[str]]:
        if run_id is None:
            raise ValueError(
                "Reading logs for all runs isn't supported for FluentBit yet"
            )

        service = LogQueryService(run_id=run_id, logs_dir=self._logs_dir)
        return service.get_full_logs()

    def iter_logs(self, run_id: t.Optional[str] = None) -> t.Iterator[t.Sequence[str]]:
        raise NotImplementedError()
