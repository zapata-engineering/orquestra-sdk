################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._driver._log_parsing.
"""
from pathlib import Path

import pytest

from orquestra.sdk._base._driver import _log_parsing


DATA_PATH = Path(__file__).parent / "data" / "get_wf_logs_response.tar.gz"


@pytest.fixture
def response_content():
    return DATA_PATH.read_bytes()


class TestParseLogsArchive:
    @staticmethod
    def test_recorded_response(response_content: bytes):
        # When
        parsed_logs = _log_parsing.parse_logs_archive(response_content)

        # Then
        # Workers
        assert len(parsed_logs.workers) == 6

        worker = parsed_logs.workers[0]
        assert worker.output_kind == _log_parsing.OutputKind.err
        assert (
            worker.worker_id
            == "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a"
        )
        assert worker.job_id == "01000000"
        assert len(worker.lines) == 2

        worker = parsed_logs.workers[1]
        assert worker.output_kind == _log_parsing.OutputKind.out
        assert (
            worker.worker_id
            == "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a"
        )
        assert worker.job_id == "01000000"
        assert len(worker.lines) == 2

        worker = parsed_logs.workers[2]
        assert worker.output_kind == _log_parsing.OutputKind.err
        assert (
            worker.worker_id
            == "ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2"
        )
        assert worker.job_id == "01000000"
        assert len(worker.lines) == 21

        # Envs
        assert len(parsed_logs.envs) == 1

        env = parsed_logs.envs[0]
        assert env.job_id == "01000000"
        assert len(env.lines) == 174
