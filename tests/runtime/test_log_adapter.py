################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import subprocess
import sys
import typing as t

import pytest

from orquestra.sdk._base import _log_adapter


class TestMakeLogger:
    @pytest.mark.slow
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

        def test_no_ids(self):
            # Given
            test_script = [
                "from orquestra.sdk._base import _log_adapter",
                "logger = _log_adapter.make_logger()",  # noqa: E501
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

    class TestUnit:
        @staticmethod
        def test_wfprint(capsys):
            # Given
            message = "foo"

            # When
            with pytest.warns(DeprecationWarning):
                _log_adapter.wfprint(message)

            # Then
            captured = capsys.readouterr()
            assert captured.out == ""
            assert message in captured.err

        @staticmethod
        def test_workflow_logger(capsys):
            # Given
            message = "foo"
            with pytest.warns(DeprecationWarning):
                logger = _log_adapter.workflow_logger()

            # When
            logger.info(message)

            # Then
            captured = capsys.readouterr()
            assert captured.out == ""
            assert message in captured.err
