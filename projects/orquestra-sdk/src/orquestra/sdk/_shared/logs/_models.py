################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Models for managing logs, used internally."""
from enum import Enum
from pathlib import Path
from typing import Dict, List, Sequence


class LogStreamType(Enum):
    STDOUT = ".out"
    STDERR = ".err"

    @classmethod
    def _missing_(cls, value):
        return cls.STDOUT

    @classmethod
    def by_file(cls, file: Path):
        return cls(file.suffix)


class LogAccumulator:
    def __init__(self):
        self._logs: Dict[LogStreamType, List[str]] = {}

    def add_lines_from_file(self, file: Path):
        stream = LogStreamType.by_file(file)
        logs_batch = file.read_text().splitlines()
        self.add_lines_by_stream(stream, logs_batch)

    def add_line_by_stream(self, stream: LogStreamType, log_line: str):
        self._logs.setdefault(stream, []).append(log_line)

    def add_lines_by_stream(self, stream: LogStreamType, log_lines: Sequence[str]):
        self._logs.setdefault(stream, []).extend(log_lines)

    @property
    def out(self):
        return self._logs.get(LogStreamType.STDOUT, [])

    @property
    def err(self):
        return self._logs.get(LogStreamType.STDERR, [])
