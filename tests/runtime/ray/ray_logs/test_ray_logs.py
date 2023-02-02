################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for RayLogs.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from orquestra.sdk._ray import _ray_logs
from orquestra.sdk.schema.workflow_run import WorkflowRunId

DATA_PATH = Path(__file__).parent / "data"


TIMEZONE = timezone(timedelta(seconds=3600))
TIMESTAMP = datetime(2023, 2, 1, 16, 4, 59, tzinfo=TIMEZONE)
INFO_LOG = _ray_logs.WFLog(
    timestamp=TIMESTAMP,
    level="INFO",
    filename="_log_adapter.py:138",
    message="hello there!",
    wf_run_id="wf.orquestra_basic_demo.e82cbfa",
    task_run_id="invocation-0-task-generate-data",
)
ERROR_LOG = _ray_logs.WFLog(
    timestamp=TIMESTAMP,
    level="ERROR",
    filename="_dag.py:194",
    message='Traceback (most recent call last):\n  File "/Users/alex/Code/zapata/evangelism-workflows/vendor/orquestra-workflow-sdk/src/orquestra/sdk/_ray/_dag.py", line 191, in _ray_remote\n    return wrapped(*inner_args, **inner_kwargs)\n  File "/Users/alex/Code/zapata/evangelism-workflows/vendor/orquestra-workflow-sdk/src/orquestra/sdk/_ray/_dag.py", line 146, in __call__\n    return self._fn(*unpacked_args, **unpacked_kwargs)\n  File "/Users/alex/Code/zapata/evangelism-workflows/demos/basic/tasks.py", line 39, in generate_data\n    foo = 1 / 0\nZeroDivisionError: division by zero\n',  # noqa: E501
    wf_run_id="wf.orquestra_basic_demo.e82cbfa",
    task_run_id="invocation-0-task-generate-data",
)


class TestParseLogLine:
    @staticmethod
    @pytest.mark.parametrize(
        "log_file, expected_parsed",
        [
            pytest.param(DATA_PATH / "worker1.err", [], id="wf_manager_stderr"),
            pytest.param(DATA_PATH / "worker1.out", [], id="wf_manager_stdout"),
            pytest.param(DATA_PATH / "worker2.err", [], id="idle_worker_stderr"),
            pytest.param(DATA_PATH / "worker2.err", [], id="idle_worker_stdout"),
            pytest.param(
                DATA_PATH / "worker3.err",
                [INFO_LOG, ERROR_LOG],
                id="busy_worker_stderr",
            ),
            pytest.param(DATA_PATH / "worker3.out", [], id="busy_worker_stdout"),
        ],
    )
    def test_parsing_real_files(log_file: Path, expected_parsed):
        # Given
        input_lines = log_file.read_bytes().splitlines(keepends=True)
        wf_run_id: WorkflowRunId = "wf.orquestra_basic_demo.e82cbfa"

        # When
        parsed = []
        for input_line in input_lines:
            parsed_log = _ray_logs.parse_log_line(input_line, searched_id=wf_run_id)
            if parsed_log is not None:
                parsed.append(parsed_log)

        # Then
        assert parsed == expected_parsed

    @staticmethod
    @pytest.mark.parametrize(
        "line,expected",
        [
            pytest.param("", None, id="empty_line"),
            pytest.param("{}", None, id="empty_json"),
            pytest.param("{{}}", None, id="malformed_json"),
            pytest.param(
                "2023-01-31 12:44:48,991	INFO workflow_executor.py:86 -- Workflow job [id=wf.orquestra_basic_demo.48c3618] started.",  # noqa: E501
                None,
                id="3rd_party_log_with_queried_id",
            ),
            pytest.param(
                '{"timestamp": "2023-02-01T16:04:59+0100", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf", "task_run_id": "wf@inv"}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id="wf",
                    task_run_id="wf@inv",
                ),
                id="valid_log_both_ids",
            ),
            pytest.param(
                '{"timestamp": "2023-02-01T16:04:59+0100", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": null, "task_run_id": null}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id=None,
                    task_run_id=None,
                ),
                id="no_run_ids",
            ),
        ],
    )
    def test_no_filter(line: str, expected):
        # Given
        raw_line = line.encode()

        # When
        parsed = _ray_logs.parse_log_line(raw_line, searched_id=None)

        # Then
        assert parsed == expected

    @staticmethod
    @pytest.mark.parametrize(
        "line,expected",
        [
            pytest.param(
                '{"timestamp": "2023-02-01T16:04:59+0100", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf", "task_run_id": "wf@inv"}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id="wf",
                    task_run_id="wf@inv",
                ),
                id="both_ids",
            ),
            pytest.param(
                '{"timestamp": "2023-02-01T16:04:59+0100", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf", "task_run_id": null}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id="wf",
                    task_run_id=None,
                ),
                id="only_wf_run_id",
            ),
            pytest.param(
                '{"timestamp": "2023-02-01 12:59:26,568", "level": "INFO", "filename": "_log_adapter.py:131", "message": "hello there!", "wf_run_id": "other", "task_run_id": "wf@inv"}',  # noqa: E501
                None,
                id="different_wf_run_id",
            ),
        ],
    )
    def test_search_by_wf_run_id(line: str, expected):
        # Given
        raw_line = line.encode()

        # When
        parsed = _ray_logs.parse_log_line(raw_line, searched_id="wf")

        # Then
        assert parsed == expected
