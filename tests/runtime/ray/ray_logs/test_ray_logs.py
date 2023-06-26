################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for RayLogs.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from orquestra.sdk._base._dates import Instant
from orquestra.sdk._ray import _ray_logs

DATA_DIR = Path(__file__).parent / "data"
TEST_RAY_TEMP = DATA_DIR / "ray_temp"


SAMPLE_TIMESTAMP = Instant(datetime(2023, 2, 9, 11, 26, 7, 99382, tzinfo=timezone.utc))


class TestIterUserLogPaths:
    """
    Unit tests for ``iter_user_log_paths``.
    Test boundary::
        [FS]->[paths iterator]
    """

    @staticmethod
    def test_with_real_files():
        # Given
        ray_temp = TEST_RAY_TEMP

        # When
        paths_iter = _ray_logs.iter_user_log_paths(ray_temp)

        # Then
        paths = list(paths_iter)
        # There are logs for 5 workers with separate files for stdout and stderr.
        # There's also a directory symlink (session_latest) but it shouldn't cause us to
        # read the same logs twice.
        assert len(paths) == 10
        assert len([p for p in paths if "worker" in p.stem]) == 10
        assert len([p for p in paths if p.suffix == ".err"]) == 5
        assert len([p for p in paths if p.suffix == ".out"]) == 5


class TestIterEnvLogPaths:
    """
    Unit tests for ``iter_env_log_paths``.
    Test boundary::
        [FS]->[paths iterator]
    """

    @staticmethod
    def test_with_real_files():
        # Given
        ray_temp = TEST_RAY_TEMP

        # When
        paths_iter = _ray_logs.iter_env_log_paths(ray_temp)

        # Then
        paths = list(paths_iter)
        # There's only one file for the env setup because we've run only one Ray job.
        assert len(paths) == 1
        assert len([p for p in paths if "runtime_env_setup" in p.stem]) == 1


class TestParseUserLogLine:
    """
    Unit tests for ``parse_user_log_line()``.
    Test boundary::
        [JSON string]->[WFLog object]
    """

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
                json.dumps(
                    {
                        "timestamp": "2023-02-09T11:26:07.099382+00:00",
                        "level": "INFO",
                        "filename": "_log_adapter.py:138",
                        "message": "hello there!",
                        "wf_run_id": "wf",
                        "task_run_id": "wf@inv",
                        "task_inv_id": "inv",
                    }
                ),
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
                json.dumps(
                    {
                        "timestamp": "2023-02-09T11:26:07.099382+00:00",
                        "level": "INFO",
                        "filename": "_log_adapter.py:138",
                        "message": "hello there!",
                        "wf_run_id": "wf",
                        "task_run_id": "wf@inv",
                        "task_inv_id": None,
                    }
                ),
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
                json.dumps(
                    {
                        "timestamp": "2023-02-09T11:26:07.099382+00:00",
                        "level": "INFO",
                        "filename": "_log_adapter.py:138",
                        "message": "hello there!",
                        "wf_run_id": "wf.1",
                        "task_run_id": None,
                        "task_inv_id": None,
                    }
                ),
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
                json.dumps(
                    {
                        "timestamp": "2023-02-09T11:26:07.099382+00:00",
                        "level": "INFO",
                        "filename": "_log_adapter.py:138",
                        "message": "hello there!",
                        "wf_run_id": None,
                        "task_run_id": None,
                        "task_inv_id": None,
                    }
                ),
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
                json.dumps(
                    {
                        "timestamp": "2023-02-09T11:26:07.099382+00:00",
                        "level": "INFO",
                        "filename": "_log_adapter.py:138",
                        "message": "hello there!",
                    }
                ),
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


def _existing_wf_run_id():
    # Assumption: this is the run ID of the workflow that produced the logs.
    return "wf.wf_using_python_imports.8a4d9e7"


class TestDirectRayReader:
    """
    Unit tests for ``DirectRayReader``.
    Test boundary::
        [recorded ray log files]->[DirectRayReader methods]
    """

    class TestGetWorkflowLogs:
        class TestPerTask:
            @staticmethod
            def test_happy_path():
                # Given
                reader = _ray_logs.DirectRayReader(ray_temp=TEST_RAY_TEMP)

                # When
                logs = reader.get_workflow_logs(_existing_wf_run_id())

                # Then
                assert logs.per_task == {
                    "invocation-1-task-add-with-log": [
                        json.dumps(
                            {
                                "timestamp": "2023-06-12T12:46:55.381839+00:00",
                                "level": "INFO",
                                "filename": "_example_wfs.py:283",
                                "message": "hello, there!",
                                "wf_run_id": "wf.wf_using_python_imports.8a4d9e7",
                                "task_inv_id": "invocation-1-task-add-with-log",
                                "task_run_id": (
                                    "wf.wf_using_python_imports.8a4d9e7"
                                    "@invocation-1-task-add-with-log.f7e22"
                                ),
                            }
                        ),
                    ]
                }

            @staticmethod
            def test_invalid_id():
                # Given
                reader = _ray_logs.DirectRayReader(ray_temp=TEST_RAY_TEMP)
                wf_run_id = "doesn't-exist"

                # When
                logs = reader.get_workflow_logs(wf_run_id)

                # Then
                assert logs.per_task == {}

        @staticmethod
        @pytest.mark.parametrize(
            "wf_run_id",
            [
                pytest.param(_existing_wf_run_id(), id="valid_id"),
                pytest.param("doesnt-exist", id="invalid_id"),
            ],
        )
        def test_env_setup(wf_run_id: str):
            # Given
            reader = _ray_logs.DirectRayReader(ray_temp=TEST_RAY_TEMP)

            # When
            logs = reader.get_workflow_logs(wf_run_id)

            # Then
            for tell_tale in [
                "Cloning virtualenv",
                "'pip', 'install'",
                "Installing python requirements",
            ]:
                assert len([line for line in logs.env_setup if tell_tale in line]) > 0

    class TestGetTaskLogs:
        @staticmethod
        def test_happy_path():
            # Given
            reader = _ray_logs.DirectRayReader(ray_temp=TEST_RAY_TEMP)
            task_inv_id = "invocation-1-task-add-with-log"

            # When
            logs = reader.get_task_logs(
                wf_run_id=_existing_wf_run_id(), task_inv_id=task_inv_id
            )

            # Then
            assert logs == [
                json.dumps(
                    {
                        "timestamp": "2023-06-12T12:46:55.381839+00:00",
                        "level": "INFO",
                        "filename": "_example_wfs.py:283",
                        "message": "hello, there!",
                        "wf_run_id": "wf.wf_using_python_imports.8a4d9e7",
                        "task_inv_id": "invocation-1-task-add-with-log",
                        "task_run_id": (
                            "wf.wf_using_python_imports.8a4d9e7"
                            "@invocation-1-task-add-with-log.f7e22"
                        ),
                    }
                ),
            ]

        @staticmethod
        @pytest.mark.parametrize(
            "wf_run_id,task_inv_id",
            [
                (
                    pytest.param("nope", id="invalid_id"),
                    pytest.param("invocation-1-task-add-with-log", id="valid_id"),
                ),
                (
                    pytest.param(_existing_wf_run_id(), id="valid_id"),
                    pytest.param("nope", id="invalid_id"),
                ),
                (
                    pytest.param("nope", id="invalid_id"),
                    pytest.param("nope", id="invalid_id"),
                ),
            ],
        )
        def test_invalid_ids(wf_run_id, task_inv_id):
            # Given
            reader = _ray_logs.DirectRayReader(ray_temp=TEST_RAY_TEMP)
            wf_run_id = "doesnt-exist"
            task_inv_id = "invocation-1-task-add-with-log"

            # When
            logs = reader.get_task_logs(wf_run_id, task_inv_id)

            # Then
            assert logs == []
