################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for ``orquestra.sdk._base._logs._markers``.
"""

import json
import sys

import pytest

from orquestra.sdk import dates
from orquestra.sdk._base._logs import _markers

INSTANT = dates.from_isoformat("2005-04-25T20:37:00+00:00")


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

    @staticmethod
    def test_no_ids(capsys):
        # TODO: remove this test case
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-530
        # Given
        wf_run_id = None
        task_inv_id = None
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
            assert len(lines) == 1
            assert lines[0] == message
