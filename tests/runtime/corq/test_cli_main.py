################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Test suite for the corq entrypoint module (orquestra.sdk._base.cli._corq._main)."""

import argparse
import json
import logging
import sys
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import ray as _ray

import orquestra.sdk._base.cli._corq._main
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    GetLogsResponse,
    ResponseMetadata,
    ResponseStatusCode,
    SubmitWorkflowDefResponse,
)
from orquestra.sdk.schema.workflow_run import State, WorkflowRunMinimal

# v2 actions
V2_ACTIONS_ATTRS = [
    "orq_get_task_def.return_value",
    "orq_get_workflow_def.return_value",
    "orq_get_workflow_run.return_value",
    "orq_get_workflow_run_results.return_value",
    "orq_submit_workflow_def.return_value",
    "orq_stop_workflow_run.return_value",
    "orq_set_default_config.return_value",
    "orq_get_default_config.return_value",
    # special v2 actions
    "orq_get_logs.return_value",
    "orq_get_artifacts.return_value",
]


def _parse_plain_text(text: str):
    return text


def _parse_json(text: str):
    return json.loads(text)


@pytest.fixture(scope="module")
def ray():
    _ray.init()
    _ray.workflow.init()
    yield
    _ray.shutdown()


@pytest.mark.parametrize(
    "argv_base",
    [
        (["orq", "submit", "workflow-def", "my_wf"]),
        (["orq", "submit", "workflow-def"]),
        (["orq", "get", "workflow-def"]),
        (["orq", "get", "workflow-run"]),
        (["orq", "get", "workflow-run", "a_run_id"]),
        (["orq", "get", "workflow-run-results", "a_run_id"]),
        (["orq", "get", "logs"]),
        (["orq", "get", "logs", "a_run_id"]),
        (["orq", "get", "default-config"]),
        (["orq", "set", "default-config", "config_name"]),
        (["orq", "stop", "workflow-run", "a_run_id"]),
        (["orq", "get", "artifacts", "a_run_id"]),
        (["orq", "get", "artifacts", "a_run_id", "a_task_id"]),
    ],
)
@pytest.mark.parametrize(
    "argv_extras, parser",
    [
        ([], _parse_plain_text),
        (["-o", "json"], _parse_json),
    ],
)
def test_error_response(monkeypatch, capsys, argv_base, argv_extras, parser):
    argv = argv_base + argv_extras
    mock_response = ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.UNKNOWN_ERROR,
            message="test message",
        )
    )
    monkeypatch.setattr(
        orquestra.sdk._base.cli._corq._main,
        "action",
        MagicMock(**{action: mock_response for action in V2_ACTIONS_ATTRS}),
    )

    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as e:
        orquestra.sdk._base.cli._corq._main.main_cli()

    assert e.value.code == ResponseStatusCode.UNKNOWN_ERROR.value

    captured = capsys.readouterr()

    assert captured.err == ""
    assert parser(captured.out)
    assert "test message" in captured.out


@pytest.mark.parametrize(
    "argv, parser",
    [
        (["orq", "submit", "workflow-def", "my_wf"], _parse_plain_text),
        (["orq", "submit", "workflow-def", "-o", "json"], _parse_json),
        (["orq", "stop", "workflow-run", "rn_id"], _parse_plain_text),
        (["orq", "stop", "workflow-run", "-o", "json", "run_id"], _parse_json),
        (["orq", "get", "workflow-def"], _parse_plain_text),
        (["orq", "get", "workflow-def", "-o", "json"], _parse_json),
        (["orq", "get", "workflow-run"], _parse_plain_text),
        (["orq", "get", "workflow-run", "-o", "json", "run_id"], _parse_json),
        (["orq", "get", "workflow-run-results", "run_id"], _parse_plain_text),
        (["orq", "get", "workflow-run-results", "run_id", "-o", "json"], _parse_json),
        (["orq", "get", "default-config"], _parse_plain_text),
        (["orq", "get", "default-config", "-o", "json"], _parse_json),
        (["orq", "set", "default-config", "default_config_name"], _parse_plain_text),
        (
            ["orq", "set", "default-config", "default_config_name", "-o", "json"],
            _parse_json,
        ),
        (["orq", "get", "artifacts", "run_id", "-o", "json"], _parse_json),
        (["orq", "get", "artifacts", "run_id", "task_id"], _parse_plain_text),
    ],
)
def test_main_cli_v2_ok_response(monkeypatch, capsys, argv, parser):
    response = ErrorResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="test message",
        )
    )
    attrs = {action: response for action in V2_ACTIONS_ATTRS}
    a = MagicMock(**attrs)
    monkeypatch.setattr(orquestra.sdk._base.cli._corq._main, "action", a)
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as e:
        orquestra.sdk._base.cli._corq._main.main_cli()
    captured = capsys.readouterr()
    assert captured.err == ""
    assert parser(captured.out)
    assert "test message" in captured.out
    assert e.value.code == ResponseStatusCode.OK.value


def test_get_logs_historical(monkeypatch, capsys):
    argv = ["orq", "get", "logs", "a-workflow-id"]
    monkeypatch.setattr(sys, "argv", argv)

    log_lines = ["general", "kenobi"]

    def _mock_get_logs(*args, **kwargs):
        return GetLogsResponse(
            meta=ResponseMetadata(
                success=True,
                code=ResponseStatusCode.OK,
                message="Retrieved logs successfully",
            ),
            logs=log_lines,
        )

    monkeypatch.setattr(
        orquestra.sdk._base.cli._corq._main.action, "orq_get_logs", _mock_get_logs
    )

    with pytest.raises(SystemExit) as e:
        orquestra.sdk._base.cli._corq._main.main_cli()

    assert e.value.code == ResponseStatusCode.OK.value

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.splitlines() == log_lines


class TestOrqSubmit:
    """
    Tests for the `orq <args> submit <args>` command
    """

    def test_verbose_flag(self, monkeypatch, capsys):
        workflow_run_id = "workflow-id"

        def _mock_submit_workflow_def(args: argparse.Namespace):
            verbose = args.verbose == logging.DEBUG
            if verbose:
                print("verbose", file=sys.stderr)
            return SubmitWorkflowDefResponse(
                meta=ResponseMetadata(
                    success=True,
                    code=ResponseStatusCode.OK,
                    message="Successfully submitted workflow.",
                ),
                workflow_runs=[WorkflowRunMinimal(id=workflow_run_id)],
            )

        monkeypatch.setattr(
            orquestra.sdk._base.cli._corq._main.action,
            "orq_submit_workflow_def",
            _mock_submit_workflow_def,
        )

        argv = ["orq", "submit", "workflow-def"]
        monkeypatch.setattr(sys, "argv", argv)

        with pytest.raises(SystemExit) as e:
            orquestra.sdk._base.cli._corq._main.main_cli()

        assert e.value.code == ResponseStatusCode.OK.value

        captured = capsys.readouterr()
        assert captured.err == ""

        argv = ["orq", "-v", "submit", "workflow-def"]
        monkeypatch.setattr(sys, "argv", argv)

        with pytest.raises(SystemExit) as e:
            orquestra.sdk._base.cli._corq._main.main_cli()

        assert e.value.code == ResponseStatusCode.OK.value

        captured = capsys.readouterr()
        assert "verbose" in captured.err


class TestParseAgeClarg:
    @staticmethod
    @pytest.mark.parametrize(
        "input_string, expected_output_timedelta",
        [
            ("9h32m", timedelta(days=0, hours=9, minutes=32, seconds=0)),
            ("8H6s", timedelta(days=0, hours=8, minutes=0, seconds=6)),
            ("10m", timedelta(days=0, hours=0, minutes=10, seconds=0)),
            ("3D6h8M13s", timedelta(days=3, hours=6, minutes=8, seconds=13)),
        ],
    )
    def test_parses_valid_strings(input_string, expected_output_timedelta):
        assert (
            orquestra.sdk._base.cli._corq._main._parse_age_clarg(
                argparse.ArgumentParser(), input_string
            )
            == expected_output_timedelta
        )

    @staticmethod
    @pytest.mark.parametrize(
        "input_string", ["NotAValidTimeString", "3h6m_plus_bunch_of_invalid_stuff"]
    )
    def test_exception_on_invalid_strings(input_string, capsys):
        with pytest.raises(SystemExit):
            orquestra.sdk._base.cli._corq._main._parse_age_clarg(
                argparse.ArgumentParser(), input_string
            )
        captured = capsys.readouterr()
        assert (
            'Time strings must be in the format "{days}d{hours}h{minutes}m{seconds}s".'
            in captured.err
        )
        assert captured.out == ""


class TestParseStateClarg:
    @staticmethod
    @pytest.mark.parametrize(
        "input_string, expected_output_state",
        [
            ("waiting", State.WAITING),
            ("running", State.RUNNING),
            ("succeeded", State.SUCCEEDED),
            ("terminated", State.TERMINATED),
            ("failed", State.FAILED),
        ],
    )
    def test_parses_valid_string(input_string, expected_output_state):
        assert (
            orquestra.sdk._base.cli._corq._main._parse_state_clarg(
                argparse.ArgumentParser(), input_string
            )
            == expected_output_state
        )

    @staticmethod
    @pytest.mark.parametrize(
        "input_string",
        ["not", "a", "valid", "state"],
    )
    def test_exception_on_invalid_strings(input_string, capsys):
        with pytest.raises(SystemExit):
            orquestra.sdk._base.cli._corq._main._parse_state_clarg(
                argparse.ArgumentParser(), input_string
            )
        captured = capsys.readouterr()
        assert f'"{input_string}" is not a valid status.' in captured.err
        assert "Status must be one of:" in captured.err
        for e in State:
            assert e.value in captured.err
        assert captured.out == ""
