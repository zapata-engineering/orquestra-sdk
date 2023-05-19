################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import subprocess
import sys
import typing as t

import pytest

from orquestra.sdk._ray import _ray_logs


class TestMakeLogger:
    @pytest.mark.skip
    class TestIntegration:
        """
        It's very difficult to verify how we arrange loggers and formatters because
        pytest gets into way. These tests verify the boundary of our system -
        ``_make_logger()`` in a subprocess.

        The ``_log_adapter`` module produces logs that need to be readable by
        ``_ray_logs`` module. This is why we use ``_ray_logs`` data structures for
        assertions.

        System overview::

            _make_logger() -> stdout -> _ray_logs.parse_log_line()

        """

        @staticmethod
        def _run_script(script: t.Sequence[str]) -> subprocess.CompletedProcess:
            # We wanna pass this as a single line to 'python -c my;script;lines'
            test_script_joined = ";".join(script)

            proc = subprocess.run(
                [sys.executable, "-c", test_script_joined], capture_output=True
            )

            return proc

        def test_both_ids(self):
            # Given
            test_script = [
                "from orquestra.sdk._base import _log_adapter",
                'logger = _log_adapter._make_logger(wf_run_id="wf.1", task_inv_id="inv2", task_run_id="task_run_3")',  # noqa: E501
                'logger.info("hello!")',
            ]

            # When
            proc = self._run_script(test_script)

            # Then
            # Expect logs printed to stderr
            assert proc.stdout == b""
            assert proc.stderr != b""

            lines = proc.stderr.splitlines()
            assert len(lines) == 1

            record = _ray_logs.parse_log_line(lines[0])
            assert record is not None

            # Expect timezone-aware dates
            assert record.timestamp.tzinfo is not None

            assert record.level == "INFO"
            assert record.message == "hello!"
            assert record.wf_run_id == "wf.1"
            assert record.task_inv_id == "inv2"
            assert record.task_run_id == "task_run_3"

        def test_no_ids(self):
            # Given
            test_script = [
                "from orquestra.sdk._base import _log_adapter",
                "logger = _log_adapter._make_logger(wf_run_id=None, task_inv_id=None, task_run_id=None)",  # noqa: E501
                'logger.info("hello!")',
            ]

            # When
            proc = self._run_script(test_script)

            # Then

            # Expect logs printed to stderr
            assert proc.stdout == b""
            assert proc.stderr != b""

            lines = proc.stderr.splitlines()
            assert len(lines) == 1

            record = _ray_logs.parse_log_line(lines[0])
            assert record is not None

            # Expect timezone-aware dates
            assert record.timestamp.tzinfo is not None

            assert record.level == "INFO"
            assert record.message == "hello!"
            assert record.wf_run_id is None
            assert record.task_inv_id is None
            assert record.task_run_id is None
