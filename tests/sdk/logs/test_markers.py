################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for ``orquestra.sdk._client._logs._markers``.
"""

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import create_autospec

import pytest

from orquestra.sdk._client import _dates
from orquestra.sdk._client._logs import _markers

INSTANT = _dates.from_isoformat("2005-04-25T20:37:00+00:00")


@pytest.fixture
def wf_run_id():
    return "wf.test.aaabbb"


@pytest.fixture
def task_inv_id():
    return "invocation-X.task"


@pytest.fixture
def message():
    return "<log message>"


@pytest.fixture
def log_dir():
    with TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.mark.parametrize(
    "platform, redirected",
    (
        ("win32", False),
        ("darwin", True),
        ("linux", True),
    ),
)
def test_platform_correct_log_implementation(
    monkeypatch: pytest.MonkeyPatch,
    log_dir: Path,
    wf_run_id: str,
    task_inv_id: str,
    platform: str,
    redirected: bool,
):
    redirected_logs = create_autospec(_markers.redirected_io)
    marker_logs = create_autospec(_markers.printed_task_markers)
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(_markers, "redirected_io", redirected_logs)
    monkeypatch.setattr(_markers, "printed_task_markers", marker_logs)

    with _markers.capture_logs(log_dir, wf_run_id, task_inv_id):
        pass

    if redirected:
        redirected_logs.assert_called_with(log_dir, wf_run_id, task_inv_id)
        marker_logs.assert_not_called()
    else:
        redirected_logs.assert_not_called()
        marker_logs.assert_called_with(wf_run_id, task_inv_id)


@pytest.mark.skipif(
    sys.platform.startswith("win32"), reason="Wurlitzer doesn't support Windows"
)
class TestLogRedirection:
    def test_stdout_redirected(
        self,
        capsys: pytest.CaptureFixture,
        log_dir: Path,
        wf_run_id: str,
        task_inv_id: str,
        message: str,
    ):
        with capsys.disabled():
            with _markers.redirected_io(log_dir, wf_run_id, task_inv_id):
                print(message)

        final_log_path = log_dir / "wf" / wf_run_id / "task" / f"{task_inv_id}.XXX"
        stdout_logs = final_log_path.with_suffix(".out").read_text()
        stderr_logs = final_log_path.with_suffix(".err").read_text()

        assert message in stdout_logs
        assert message not in stderr_logs

    def test_stderr_redirected(
        self,
        capsys: pytest.CaptureFixture,
        log_dir: Path,
        wf_run_id: str,
        task_inv_id: str,
        message: str,
    ):
        with capsys.disabled():
            with _markers.redirected_io(log_dir, wf_run_id, task_inv_id):
                print(message, file=sys.stderr)

        final_log_path = log_dir / "wf" / wf_run_id / "task" / f"{task_inv_id}.XXX"
        stdout_logs = final_log_path.with_suffix(".out").read_text()
        stderr_logs = final_log_path.with_suffix(".err").read_text()

        assert message not in stdout_logs
        assert message in stderr_logs

    def test_exception_redirected(
        self,
        capsys: pytest.CaptureFixture,
        log_dir: Path,
        wf_run_id: str,
        task_inv_id: str,
        message: str,
    ):
        with pytest.raises(Exception), capsys.disabled():
            with _markers.redirected_io(log_dir, wf_run_id, task_inv_id):
                raise Exception(message)

        final_log_path = log_dir / "wf" / wf_run_id / "task" / f"{task_inv_id}.XXX"
        stdout_logs = final_log_path.with_suffix(".out").read_text()
        stderr_logs = final_log_path.with_suffix(".err").read_text()

        assert message not in stdout_logs
        assert message in stderr_logs

    def test_log_directories_created(
        self, log_dir: Path, wf_run_id: str, task_inv_id: str
    ):
        with _markers.redirected_io(log_dir, wf_run_id, task_inv_id):
            pass
        assert (log_dir / "wf" / wf_run_id / "task").exists()

    def test_subprocess(
        self, log_dir: Path, wf_run_id: str, task_inv_id: str, message: str
    ):
        with _markers.redirected_io(log_dir, wf_run_id, task_inv_id):
            subprocess.run(["echo", f"{message}"])

        final_log_path = log_dir / "wf" / wf_run_id / "task" / f"{task_inv_id}.XXX"
        stdout_logs = final_log_path.with_suffix(".out").read_text()
        stderr_logs = final_log_path.with_suffix(".err").read_text()

        assert message in stdout_logs
        assert message not in stderr_logs


class TestParseLine:
    @staticmethod
    @pytest.mark.parametrize(
        "marker",
        [
            _markers.TaskStartMarker(
                wf_run_id="wf1", task_inv_id="inv1", timestamp=INSTANT
            ),
            _markers.TaskEndMarker(
                wf_run_id="wf1", task_inv_id="inv1", timestamp=INSTANT
            ),
            _markers.TaskEndMarker(
                wf_run_id="wf1", task_inv_id=None, timestamp=INSTANT
            ),
            _markers.TaskEndMarker(
                wf_run_id=None, task_inv_id="inv1", timestamp=INSTANT
            ),
            _markers.TaskEndMarker(wf_run_id=None, task_inv_id=None, timestamp=INSTANT),
        ],
    )
    def test_valid(marker: _markers.Marker):
        # Given
        line = marker.line

        # When
        parsed = _markers.parse_line(line)

        # Then
        assert parsed == marker

    @staticmethod
    @pytest.mark.parametrize(
        "line",
        [
            pytest.param("", id="empty-line"),
            pytest.param("ORQ-MARKER:", id="only-prefix"),
            pytest.param("ORQ-MARKER:{}", id="empty-event"),
            pytest.param(
                f'ORQ-MARKER:{json.dumps({"event": "task_start"})}',
                id="malformed-start-event",
            ),
            pytest.param(
                f'ORQ-MARKER:{json.dumps({"event": "other"})}',
                id="unsupported-event-type",
            ),
            pytest.param(":job_id:01000000", id="ray-job-marker"),
            pytest.param("actor_name:WorkflowManagementActor", id="ray-actor-marker"),
            pytest.param(
                "023-06-12 14:46:54,274	INFO workflow_executor.py:86 -- Workflow job [id=wf.wf_using_python_imports.8a4d9e7] started.",  # noqa: E501
                id="ray-workflow-marker",
            ),
            pytest.param(
                "023-06-12 14:46:55,385	INFO workflow_executor.py:284 -- Task status [SUCCESSFUL]	[wf.wf_using_python_imports.8a4d9e7@invocation-1-task-add-with-log]",  # noqa: E501
                id="ray-task-marker",
            ),
        ],
    )
    def test_invalid(line):
        # When
        parsed = _markers.parse_line(line)

        # Then
        assert parsed is None


class TestPrintedTaskMarkers:
    @staticmethod
    def test_happy_flow(capsys):
        # Given
        wf_run_id = "wf1"
        task_inv_id = "inv1"
        message = "hello!"

        # When
        with _markers.printed_task_markers(
            wf_run_id=wf_run_id,
            task_inv_id=task_inv_id,
        ):
            print(message)
            print(message, file=sys.stderr)

        # Then
        captured = capsys.readouterr()
        for stream in [captured.out, captured.err]:
            lines = stream.splitlines()
            assert len(lines) == 3

            assert isinstance(_markers.parse_line(lines[0]), _markers.TaskStartMarker)
            assert lines[1] == message
            assert isinstance(_markers.parse_line(lines[2]), _markers.TaskEndMarker)

    @staticmethod
    def test_exception(capsys):
        # Given
        wf_run_id = "wf1"
        task_inv_id = "inv1"
        message = "uh oh!"

        # When
        with pytest.raises(ValueError):
            with _markers.printed_task_markers(
                wf_run_id=wf_run_id,
                task_inv_id=task_inv_id,
            ):
                raise ValueError(message)

        # Then
        captured = capsys.readouterr()
        out_lines = captured.out.splitlines()
        assert len(out_lines) == 2

        assert isinstance(_markers.parse_line(out_lines[0]), _markers.TaskStartMarker)
        assert isinstance(_markers.parse_line(out_lines[1]), _markers.TaskEndMarker)

        err_lines = captured.err.splitlines()
        assert len(err_lines) > 2

        assert isinstance(_markers.parse_line(out_lines[0]), _markers.TaskStartMarker)
        assert isinstance(_markers.parse_line(out_lines[-1]), _markers.TaskEndMarker)
