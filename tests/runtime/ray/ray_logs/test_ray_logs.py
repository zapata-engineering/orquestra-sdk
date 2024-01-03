################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for RayLogs.
"""
import typing as t
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra.sdk import LogOutput
from orquestra.sdk._client import _dates
from orquestra.sdk._client._logs import _markers
from orquestra.sdk._ray import _ray_logs

DATA_DIR = Path(__file__).parent / "data"
TEST_RAY_TEMP = DATA_DIR / "legacy_logs" / "ray_temp"
TEST_REDIRECTED_LOG_DIR = DATA_DIR / "redirected_logs" / "logs"


SAMPLE_TIMESTAMP = _dates.utc_from_comps(2023, 2, 9, 11, 26, 7, 99382)


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


def _existing_wf_run_id():
    # Assumption: this is the run ID of the workflow that produced the logs.
    return "wf.wf_using_python_imports.1472d35"


def _make_worker_file(tmp_path: Path, lines: t.Sequence[str]) -> Path:
    path = tmp_path / "worker.stdout"
    with path.open("w") as f:
        for line in lines:
            f.write(line + "\n")
    return path


class TestIterTaskLogs:
    @staticmethod
    @pytest.fixture
    def start_marker():
        wf_run_id = "wf1"
        inv_id = "inv1"
        timestamp = _dates.from_isoformat("2005-04-25T20:37:00+00:00")
        return _markers.TaskStartMarker(wf_run_id, inv_id, timestamp)

    @staticmethod
    @pytest.fixture
    def end_marker():
        wf_run_id = "wf1"
        inv_id = "inv1"
        timestamp = _dates.from_isoformat("2005-04-25T20:37:01+00:00")
        return _markers.TaskEndMarker(wf_run_id, inv_id, timestamp)

    @staticmethod
    @pytest.fixture
    def start_marker2():
        wf_run_id = "wf1"
        inv_id = "inv2"
        timestamp = _dates.from_isoformat("2005-04-25T20:38:00+00:00")
        return _markers.TaskStartMarker(wf_run_id, inv_id, timestamp)

    @staticmethod
    @pytest.fixture
    def end_marker2():
        wf_run_id = "wf1"
        inv_id = "inv2"
        timestamp = _dates.from_isoformat("2005-04-25T20:38:01+00:00")
        return _markers.TaskEndMarker(wf_run_id, inv_id, timestamp)

    @staticmethod
    def test_empty_file(tmp_path):
        # Given
        path = _make_worker_file(tmp_path, lines=[])

        # When
        yields = list(_ray_logs.iter_task_logs(path))

        # Then
        assert yields == []

    class TestSingleTask:
        @staticmethod
        def test_happy_path(tmp_path: Path, start_marker, end_marker):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise",
                    start_marker.line,
                    "hello!",
                    end_marker.line,
                    "ray-noise",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello!"],
                    start_marker.wf_run_id,
                    start_marker.task_inv_id,
                )
            ]

        @staticmethod
        def test_missing_start(tmp_path: Path, end_marker):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise",
                    "hello!",
                    end_marker.line,
                    "ray-noise",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == []

        @staticmethod
        def test_missing_end(tmp_path: Path, start_marker):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise1",
                    start_marker.line,
                    "hello!",
                    "ray-noise2",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello!", "ray-noise2"],
                    start_marker.wf_run_id,
                    start_marker.task_inv_id,
                )
            ]

    class TestMultipleTasks:
        @staticmethod
        def test_happy_path(
            tmp_path: Path, start_marker, end_marker, start_marker2, end_marker2
        ):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise1",
                    start_marker.line,
                    "hello1!",
                    end_marker.line,
                    "ray-noise2",
                    start_marker2.line,
                    "hello2!",
                    end_marker2.line,
                    "ray-noise3",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello1!"],
                    start_marker.wf_run_id,
                    start_marker.task_inv_id,
                ),
                (
                    ["hello2!"],
                    start_marker2.wf_run_id,
                    start_marker2.task_inv_id,
                ),
            ]

        @staticmethod
        def test_missing_leading_start(
            tmp_path: Path, end_marker, start_marker2, end_marker2
        ):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise1",
                    # missing start marker
                    "hello1!",
                    end_marker.line,
                    "ray-noise2",
                    start_marker2.line,
                    "hello2!",
                    end_marker2.line,
                    "ray-noise3",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello2!"],
                    start_marker2.wf_run_id,
                    start_marker2.task_inv_id,
                ),
            ]

        @staticmethod
        def test_missing_leading_end(
            tmp_path: Path, start_marker, start_marker2, end_marker2
        ):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise1",
                    start_marker.line,
                    "hello1!",
                    # missing end marker
                    "ray-noise2",
                    start_marker2.line,
                    "hello2!",
                    end_marker2.line,
                    "ray-noise3",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello1!", "ray-noise2"],
                    start_marker.wf_run_id,
                    start_marker.task_inv_id,
                ),
                (
                    ["hello2!"],
                    start_marker2.wf_run_id,
                    start_marker2.task_inv_id,
                ),
            ]

        @staticmethod
        def test_missing_trailing_start(
            tmp_path: Path, start_marker, end_marker, end_marker2
        ):
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise1",
                    start_marker.line,
                    "hello1!",
                    end_marker.line,
                    "ray-noise2",
                    # missing start marker
                    "hello2!",
                    end_marker2.line,
                    "ray-noise3",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello1!"],
                    start_marker.wf_run_id,
                    start_marker.task_inv_id,
                ),
            ]

        @staticmethod
        def test_missing_trailing_end(
            tmp_path: Path, start_marker, end_marker, start_marker2
        ):
            # TODO
            # Given
            path = _make_worker_file(
                tmp_path,
                lines=[
                    "ray-noise1",
                    start_marker.line,
                    "hello1!",
                    end_marker.line,
                    "ray-noise2",
                    start_marker2.line,
                    "hello2!",
                    # missing end marker
                    "ray-noise3",
                ],
            )

            # When
            yields = list(_ray_logs.iter_task_logs(path))

            # Then
            assert yields == [
                (
                    ["hello1!"],
                    start_marker.wf_run_id,
                    start_marker.task_inv_id,
                ),
                (
                    ["hello2!", "ray-noise3"],
                    start_marker2.wf_run_id,
                    start_marker2.task_inv_id,
                ),
            ]


class TestDirectLogReader:
    """
    Unit tests for ``DirectLogReader``.
    Test boundary::
        [recorded ray log files]->[DirectLogReader methods]
    """

    @staticmethod
    @pytest.fixture
    def reader():
        return _ray_logs.DirectLogReader(ray_temp=TEST_RAY_TEMP)

    class TestGetWorkflowLogs:
        class TestLegacyPerTask:
            @staticmethod
            def test_happy_path(reader: _ray_logs.DirectLogReader):
                # When
                logs = reader.get_workflow_logs(_existing_wf_run_id())

                # Then
                assert logs.per_task == {
                    "invocation-0-task-task-with-python-imports": LogOutput(
                        out=[], err=[]
                    ),
                    "invocation-1-task-add-with-log": LogOutput(
                        out=["hello, there!"], err=[]
                    ),
                }

            @staticmethod
            def test_invalid_id(reader: _ray_logs.DirectLogReader):
                wf_run_id = "doesn't-exist"

                # When
                logs = reader.get_workflow_logs(wf_run_id)

                # Then
                assert logs.per_task == {}

        @pytest.mark.parametrize(
            "wf_run_id",
            [
                pytest.param(_existing_wf_run_id(), id="valid_id"),
                pytest.param("doesnt-exist", id="invalid_id"),
            ],
        )
        class TestNonTaskLogTypes:
            @staticmethod
            def test_env_setup(wf_run_id: str, reader: _ray_logs.DirectLogReader):
                # When
                logs = reader.get_workflow_logs(wf_run_id)

                # Then
                for tell_tale in [
                    "Cloning virtualenv",
                    "'pip', 'install'",
                    "Installing python requirements",
                ]:
                    assert (
                        len([line for line in logs.env_setup.out if tell_tale in line])
                        > 0
                    )

            @staticmethod
            def test_system(wf_run_id: str, reader: _ray_logs.DirectLogReader):
                # When
                logs = reader.get_workflow_logs(wf_run_id)

                # Then
                assert logs.system == LogOutput(
                    out=[],
                    err=[
                        f"WARNING: we don't parse system logs for the local runtime. The log files can be found in the directory '{TEST_RAY_TEMP}'"  # noqa: E501
                    ],
                )

            @staticmethod
            def test_other(wf_run_id: str, reader: _ray_logs.DirectLogReader):
                # When
                logs = reader.get_workflow_logs(wf_run_id)

                # Then
                assert logs.other == LogOutput(
                    out=[],
                    err=[
                        f"WARNING: we don't parse uncategorized logs for the local runtime. The log files can be found in the directory '{TEST_RAY_TEMP}'"  # noqa: E501
                    ],
                )

    class TestGetLegacyTaskLogs:
        @staticmethod
        def test_happy_path(reader: _ray_logs.DirectLogReader):
            # Given
            task_inv_id = "invocation-1-task-add-with-log"

            # When
            logs = reader.get_task_logs(
                wf_run_id=_existing_wf_run_id(), task_inv_id=task_inv_id
            )

            # Then
            assert logs == LogOutput(out=["hello, there!"], err=[])

        @staticmethod
        @pytest.mark.parametrize(
            "wf_run_id,task_inv_id",
            [
                pytest.param(
                    "nope",
                    "invocation-1-task-add-with-log",
                    id="invalid_wf_id_valid_task_id",
                ),
                pytest.param(
                    _existing_wf_run_id(), "nope", id="valid_wf_id_invalid_task_id"
                ),
                pytest.param("nope", "nope", id="both_invalid_id"),
            ],
        )
        def test_invalid_ids(
            wf_run_id: str, task_inv_id: str, reader: _ray_logs.DirectLogReader
        ):
            # When
            logs = reader.get_task_logs(wf_run_id, task_inv_id)

            # Then
            assert logs == LogOutput(out=[], err=[])

    class TestRedirectedLogs:
        @staticmethod
        @pytest.fixture(autouse=True)
        def patch_log_location(monkeypatch: pytest.MonkeyPatch):
            monkeypatch.setattr(
                _ray_logs,
                "redirected_logs_dir",
                Mock(return_value=TEST_REDIRECTED_LOG_DIR),
            )

        @staticmethod
        @pytest.fixture
        def valid_wf_run_id():
            return "wf-abcde-r000"

        @staticmethod
        @pytest.fixture
        def valid_task_inv_id():
            return "invocation-1-task-add-with-log"

        class TestTaskLogs:
            @staticmethod
            def test_happy_path(
                reader: _ray_logs.DirectLogReader,
                valid_wf_run_id: str,
                valid_task_inv_id: str,
            ):
                # When
                logs = reader.get_task_logs(valid_wf_run_id, valid_task_inv_id)

                # Then
                assert logs == LogOutput(out=["hello, there!"], err=[])

            @staticmethod
            def test_invalid_task_id_is_empty(
                reader: _ray_logs.DirectLogReader, valid_wf_run_id: str
            ):
                # Given
                task_inv_id = "<task inv sentinel>"

                # When
                logs = reader.get_task_logs(valid_wf_run_id, task_inv_id)

                # Then
                assert logs == LogOutput(out=[], err=[])

            @staticmethod
            def test_invalid_wf_id_calls_legacy(
                monkeypatch: pytest.MonkeyPatch,
                reader: _ray_logs.DirectLogReader,
                valid_task_inv_id: str,
            ):
                # Given
                wf_run_id = "<wf run sentinel>"
                legacy_logs = Mock()
                monkeypatch.setattr(reader, "_get_legacy_task_logs", legacy_logs)

                # When
                _ = reader.get_task_logs(wf_run_id, valid_task_inv_id)

                # Then
                legacy_logs.assert_called_with(wf_run_id, [valid_task_inv_id])

        class TestWorkflowLogs:
            @staticmethod
            def test_happy_path(
                reader: _ray_logs.DirectLogReader, valid_wf_run_id: str
            ):
                # When
                logs = reader.get_workflow_logs(valid_wf_run_id)

                # Then
                assert logs.per_task == {
                    "invocation-0-task-task-with-python-imports": LogOutput(
                        out=[], err=[]
                    ),
                    "invocation-1-task-add-with-log": LogOutput(
                        out=["hello, there!"], err=[]
                    ),
                }

            @staticmethod
            def test_invalid_wf_id_calls_legacy(
                monkeypatch: pytest.MonkeyPatch, reader: _ray_logs.DirectLogReader
            ):
                # Given
                wf_run_id = "<wf run sentinel>"
                legacy_logs = Mock()
                monkeypatch.setattr(reader, "_get_legacy_task_logs", legacy_logs)

                # When
                _ = reader.get_workflow_logs(wf_run_id)

                # Then
                legacy_logs.assert_called_with(wf_run_id)
