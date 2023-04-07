################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for ``orquestra.sdk._base.cli._corq._cli_logs``.
"""
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.slow()
class TestConfigureVerbosenessIfNeeded:
    class TestIntegration:
        """
        Python's ``logging`` is notoriously difficult to reason about, so unit tests
        with mocks don't make much sense. Testing logging configuration with ``pytest``
        isn't straightforward either because ``pytest`` intercepts log entries. This is
        why we test the logging config via subprocess & stdout.
        """

        @staticmethod
        @pytest.fixture
        def script_path():
            return Path(__file__).parent / "data" / "simulate_cli_logging_stuff.py"

        @staticmethod
        def test_no_env_var(script_path: Path):
            # When
            proc_result = subprocess.run(
                [sys.executable, str(script_path)], capture_output=True
            )

            # Then
            proc_result.check_returncode()
            stdout = proc_result.stdout.decode()
            stderr = proc_result.stderr.decode()
            assert stdout == ""
            assert stderr == ""

        @staticmethod
        def test_verbose_flag(script_path: Path):
            # When
            proc_result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                env={"ORQ_VERBOSE": "1"},
            )

            # Then
            proc_result.check_returncode()
            stdout = proc_result.stdout.decode()
            stderr = proc_result.stderr.decode()
            assert stdout == ""
            assert stderr == (
                "DEBUG:root:root logger debug message\n"
                "DEBUG:__main__:module logger debug message\n"
            )
