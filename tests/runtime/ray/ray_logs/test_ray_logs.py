################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for RayLogs.
"""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra.sdk._ray import _ray_logs

INFO_LOG = _ray_logs.WFLog(
    timestamp=datetime(2023, 2, 9, 11, 26, 7, 98413, tzinfo=timezone.utc),
    level="INFO",
    filename="_log_adapter.py:184",
    message="hello there!",
    wf_run_id="wf.orquestra_basic_demo.3fcba90",
    task_inv_id="invocation-0-task-generate-data",
    task_run_id="wf.orquestra_basic_demo.3fcba90@invocation-0-task-generate-data.d0751",
)

SAMPLE_TIMESTAMP = datetime(2023, 2, 9, 11, 26, 7, 99382, tzinfo=timezone.utc)
ERROR_LOG = _ray_logs.WFLog(
    timestamp=SAMPLE_TIMESTAMP,
    level="ERROR",
    filename="_dag.py:196",
    message='Traceback (most recent call last):\n  File "/Users/alex/Code/zapata/evangelism-workflows/vendor/orquestra-workflow-sdk/src/orquestra/sdk/_ray/_dag.py", line 193, in _ray_remote\n    return wrapped(*inner_args, **inner_kwargs)\n  File "/Users/alex/Code/zapata/evangelism-workflows/vendor/orquestra-workflow-sdk/src/orquestra/sdk/_ray/_dag.py", line 148, in __call__\n    return self._fn(*unpacked_args, **unpacked_kwargs)\n  File "/Users/alex/Code/zapata/evangelism-workflows/demos/basic/tasks.py", line 42, in generate_data\n    foo = 1 / 0\nZeroDivisionError: division by zero\n',  # noqa: E501
    wf_run_id="wf.orquestra_basic_demo.3fcba90",
    task_inv_id="invocation-0-task-generate-data",
    task_run_id="wf.orquestra_basic_demo.3fcba90@invocation-0-task-generate-data.d0751",
)

DATA_PATH = Path(__file__).parent / "data"
TEST_RAY_TEMP_PATH = DATA_PATH / "ray_temp"
WORKER_LOGS_PATH = (
    TEST_RAY_TEMP_PATH / "session_2023-02-09_12-23-55_156174_25782" / "logs"
)


class TestParseLogLine:
    @staticmethod
    @pytest.mark.parametrize(
        "log_file, expected_parsed",
        [
            pytest.param(WORKER_LOGS_PATH / "worker1.err", [], id="wf_manager_stderr"),
            pytest.param(WORKER_LOGS_PATH / "worker1.out", [], id="wf_manager_stdout"),
            pytest.param(WORKER_LOGS_PATH / "worker2.err", [], id="idle_worker_stderr"),
            pytest.param(WORKER_LOGS_PATH / "worker2.err", [], id="idle_worker_stdout"),
            pytest.param(
                WORKER_LOGS_PATH / "worker3.err",
                [INFO_LOG, ERROR_LOG],
                id="busy_worker_stderr",
            ),
            pytest.param(WORKER_LOGS_PATH / "worker3.out", [], id="busy_worker_stdout"),
        ],
    )
    def test_parsing_real_files(log_file: Path, expected_parsed):
        # Given
        input_lines = log_file.read_bytes().splitlines(keepends=True)

        # When
        parsed = []
        for input_line in input_lines:
            parsed_log = _ray_logs.parse_log_line(input_line)
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
            pytest.param("20.37", None, id="json_scalar"),
            pytest.param("{{}}", None, id="malformed_json"),
            pytest.param(
                "2023-01-31 12:44:48,991	INFO workflow_executor.py:86 -- Workflow job [id=wf.orquestra_basic_demo.48c3618] started.",  # noqa: E501
                None,
                id="3rd_party_log_with_queried_id",
            ),
            pytest.param(
                '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf", "task_run_id": "wf@inv", "task_inv_id": "inv"}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=SAMPLE_TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id="wf",
                    task_inv_id="inv",
                    task_run_id="wf@inv",
                ),
                id="valid_log_all_ids",
            ),
            pytest.param(
                '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf", "task_run_id": "wf@inv", "task_inv_id": null}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=SAMPLE_TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id="wf",
                    task_run_id="wf@inv",
                    task_inv_id=None,
                ),
                id="valid_log_no_inv",
            ),
            pytest.param(
                '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf.1", "task_run_id": null, "task_inv_id": null}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=SAMPLE_TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id="wf.1",
                    task_inv_id=None,
                    task_run_id=None,
                ),
                id="valid_log_only_wf_run_id",
            ),
            pytest.param(
                '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": null, "task_run_id": null, "task_inv_id": null}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=SAMPLE_TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id=None,
                    task_inv_id=None,
                    task_run_id=None,
                ),
                id="valid_log_all_null_ids",
            ),
            pytest.param(
                '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!"}',  # noqa: E501
                _ray_logs.WFLog(
                    timestamp=SAMPLE_TIMESTAMP,
                    level="INFO",
                    filename="_log_adapter.py:138",
                    message="hello there!",
                    wf_run_id=None,
                    task_inv_id=None,
                    task_run_id=None,
                ),
                id="valid_log_no_id_fields",
            ),
        ],
    )
    def test_examples(line: str, expected):
        # Given
        raw_line = line.encode()

        # When
        parsed = _ray_logs.parse_log_line(raw_line)

        # Then
        assert parsed == expected


class TestIterLogPaths:
    """
    Test boundary::
        [FS]->[_iter_log_paths()]
    """

    @staticmethod
    def test_with_real_files():
        # Given
        ray_temp = TEST_RAY_TEMP_PATH

        # When
        paths_iter = _ray_logs._iter_log_paths(ray_temp)

        # Then
        paths = list(paths_iter)
        # There are logs for 3 workers with separate files for stdout and stderr.
        # There's also a directory symlink (session_latest) but it shouldn't cause us to
        # read the same logs twice.
        assert len(paths) == 6
        assert len([p for p in paths if "worker" in p.stem]) == 6
        assert len([p for p in paths if p.suffix == ".err"]) == 3
        assert len([p for p in paths if p.suffix == ".out"]) == 3


class TestIterLogLines:
    """
    Test boundary::
        [_iter_log_paths()]->[_iter_log_lines()]
                       [FS]->[_iter_log_lines()]
    """

    @staticmethod
    def test_lines_content(monkeypatch):
        # Given
        fake_paths = (p for p in WORKER_LOGS_PATH.iterdir() if p.is_file())
        monkeypatch.setattr(_ray_logs, "_iter_log_paths", fake_paths)

        # When
        lines_iter = _ray_logs._iter_log_lines(fake_paths)

        # Then
        lines = list(lines_iter)
        assert len(lines) == 70

        # Look for known sample content. We need to strip newlines to make the test
        # OS-independent
        stripped_lines = [line.strip() for line in lines]
        assert b":task_name:create_ray_workflow" in stripped_lines
        assert b"ZeroDivisionError: division by zero" in stripped_lines


class TestDirectRayReader:
    class TestGetTaskLogs:
        """
        Test boundary::
            [_iter_log_lines()]->[DirectRayReader.get_task_logs()]
        """

        @staticmethod
        @pytest.mark.parametrize(
            "wf_run_id,task_inv_id,parsed_logs,expected",
            [
                pytest.param("wf.1", "inv1", [], [], id="no_parsed_logs"),
                pytest.param(
                    "wf.1",
                    "inv1",
                    [
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="hello there!",
                            wf_run_id="wf.2",
                            task_inv_id="inv1",
                            task_run_id="wf.2@inv1",
                        ),
                    ],
                    [],
                    id="invalid_wf_run",
                ),
                pytest.param(
                    "wf.1",
                    "inv1",
                    [
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="hello there!",
                            wf_run_id="wf.2",
                            task_inv_id="inv2",
                            task_run_id="wf.1@inv2",
                        ),
                    ],
                    [],
                    id="invalid_task_inv",
                ),
                pytest.param(
                    "wf.1",
                    "inv1",
                    [
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="hello there!",
                            wf_run_id="wf.1",
                            task_inv_id="inv1",
                            task_run_id="wf.1@inv1",
                        ),
                    ],
                    [
                        '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf.1", "task_inv_id": "inv1", "task_run_id": "wf.1@inv1"}',  # noqa: E501
                    ],
                    id="matching_ids",
                ),
            ],
        )
        def test_examples(monkeypatch, wf_run_id, task_inv_id, parsed_logs, expected):
            # Given
            ray_temp = Path("shouldnt/matter")
            reader = _ray_logs.DirectRayReader(ray_temp=ray_temp)
            monkeypatch.setattr(
                reader, "_get_parsed_logs", Mock(return_value=parsed_logs)
            )

            # When
            result_logs = reader.get_task_logs(
                wf_run_id=wf_run_id, task_inv_id=task_inv_id
            )

            # Then
            assert result_logs == expected

    class TestGetWorkflowLogs:
        """
        Test boundary::
            [_get_parsed_logs()]->[get_workflow_logs()]
        """

        @staticmethod
        @pytest.mark.parametrize(
            "wf_run_id,parsed_logs,expected",
            [
                pytest.param("wf.1", [], {}, id="no_parsed_logs"),
                pytest.param(
                    "wf.1",
                    [
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="hello there!",
                            wf_run_id="wf.1",
                            task_inv_id="inv1",
                            task_run_id="wf.1@inv1",
                        ),
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="general kenobi!",
                            wf_run_id="wf.1",
                            task_inv_id="inv1",
                            task_run_id="wf.1@inv1",
                        ),
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="and another one",
                            wf_run_id="wf.1",
                            task_inv_id="inv2",
                            task_run_id="wf.1@inv2",
                        ),
                    ],
                    {
                        "inv1": [
                            '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "hello there!", "wf_run_id": "wf.1", "task_inv_id": "inv1", "task_run_id": "wf.1@inv1"}',  # noqa: E501
                            '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "general kenobi!", "wf_run_id": "wf.1", "task_inv_id": "inv1", "task_run_id": "wf.1@inv1"}',  # noqa: E501
                        ],
                        "inv2": [
                            '{"timestamp": "2023-02-09T11:26:07.099382+00:00", "level": "INFO", "filename": "_log_adapter.py:138", "message": "and another one", "wf_run_id": "wf.1", "task_inv_id": "inv2", "task_run_id": "wf.1@inv2"}',  # noqa: E501
                        ],
                    },
                    id="matching_wf_run",
                ),
                pytest.param(
                    "wf.1",
                    [
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="hello there!",
                            wf_run_id="wf.1",
                            task_inv_id=None,
                            task_run_id=None,
                        )
                    ],
                    {},
                    id="no_task_ids",
                ),
                pytest.param(
                    "wf.1",
                    [
                        _ray_logs.WFLog(
                            timestamp=SAMPLE_TIMESTAMP,
                            level="INFO",
                            filename="_log_adapter.py:138",
                            message="hello there!",
                            wf_run_id="wf.2",
                            task_inv_id="inv2",
                            task_run_id="wf.2@inv2",
                        )
                    ],
                    {},
                    id="other_wf_run",
                ),
            ],
        )
        def test_examples(monkeypatch, wf_run_id, parsed_logs, expected):
            # Given
            ray_temp = Path("shouldnt/matter")
            reader = _ray_logs.DirectRayReader(ray_temp=ray_temp)
            monkeypatch.setattr(
                reader, "_get_parsed_logs", Mock(return_value=parsed_logs)
            )

            # When
            result_logs = reader.get_workflow_logs(wf_run_id=wf_run_id)

            # Then
            assert result_logs == expected
