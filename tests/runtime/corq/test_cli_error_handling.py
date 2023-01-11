################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import json
import logging
import subprocess
import sys
from unittest.mock import Mock

import pytest
import ray as _ray

import orquestra.sdk._base.cli._corq._main
from orquestra.sdk import exceptions
from orquestra.sdk.schema.responses import ResponseStatusCode
from orquestra.sdk.schema.workflow_run import State

ARGV_ACTION: list = [
    (["get", "workflow-def"], "orq_get_workflow_def"),
    (["submit", "workflow-def", "my_wf"], "orq_submit_workflow_def"),
    (["submit", "workflow-def"], "orq_submit_workflow_def"),
    (["get", "workflow-def"], "orq_get_workflow_def"),
    (["get", "workflow-run"], "orq_get_workflow_run"),
    (["get", "workflow-run", "a_run_id"], "orq_get_workflow_run"),
    (["get", "workflow-run-results", "a_run_id"], "orq_get_workflow_run_results"),
    (["get", "logs"], "orq_get_logs"),
]
EXCEPTION_MESSAGE_CODE: list = [
    (
        exceptions.ConfigFileNotFoundError("test ConfigFileNotFoundError msg"),
        ["No configuration file found."],
        ResponseStatusCode.NOT_FOUND,
    ),
    (
        exceptions.ConfigNameNotFoundError("test ConfigNameNotFoundError msg"),
        ["No configuration with that name found."],
        ResponseStatusCode.NOT_FOUND,
    ),
    (
        exceptions.InvalidWorkflowDefinitionError(
            "test InvalidWorkflowDefinitionError msg"
        ),
        ["The workflow definition is invalid."],
        ResponseStatusCode.INVALID_WORKFLOW_DEF,
    ),
    (
        exceptions.WorkflowDefinitionSyntaxError(
            "test WorkflowDefinitionSyntaxError msg"
        ),
        ["Syntax error in workflow definition"],
        ResponseStatusCode.INVALID_WORKFLOW_DEFS_SYNTAX,
    ),
    (
        exceptions.WorkflowTooLargeError("test WorkflowTooLargeError msg"),
        ["The workflow is too large to be executed."],
        ResponseStatusCode.INVALID_WORKFLOW_DEF,
    ),
    (
        exceptions.InvalidTaskDefinitionError("test InvalidTaskDefinitionError msg"),
        ["Invalid task definition."],
        ResponseStatusCode.INVALID_TASK_DEF,
    ),
    (
        exceptions.WorkflowNotFoundError("test WorkflowNotFoundError msg"),
        ["Workflow run with ID", "not found."],
        ResponseStatusCode.WORKFLOW_RUN_NOT_FOUND,
    ),
    (
        exceptions.InvalidWorkflowRunError("test InvalidWorkflowRunError msg"),
        ["Workflow run with ID", "exists, but is invalid."],
        ResponseStatusCode.INVALID_WORKFLOW_RUN,
    ),
    (
        exceptions.WorkflowRunNotSucceeded(
            "test WorkflowRunNotSucceeded msg", State.RUNNING
        ),
        ["Workflow", "has not succeeded."],
        ResponseStatusCode.WORKFLOW_RUN_NOT_FOUND,
    ),
    (
        NotADirectoryError("test NotADirectoryError msg"),
        ["Please remove the .orquestra file in"],
        ResponseStatusCode.NOT_A_DIRECTORY,
    ),
    (
        exceptions.UnauthorizedError("test UnauthorizedError msg"),
        ["Authorization failed.", "Please log in again."],
        ResponseStatusCode.UNAUTHORIZED,
    ),
    (
        ConnectionError("test ConnectionError msg"),
        ["Unable to connect to Ray"],
        ResponseStatusCode.CONNECTION_ERROR,
    ),
    (
        PermissionError("test PermissionError msg"),
        ["Unable to access"],
        ResponseStatusCode.PERMISSION_ERROR,
    ),
    (
        FileNotFoundError("test FileNotFoundError msg"),
        ["File", "not found."],
        ResponseStatusCode.NOT_FOUND,
    ),
    (
        ModuleNotFoundError("test ModuleNotFoundError msg"),
        ["Module", "not found."],
        ResponseStatusCode.NOT_FOUND,
    ),
    (
        NotImplementedError("test NotImplementedError msg"),
        ["The function you have tried to access is not yet implemented."],
        ResponseStatusCode.NOT_FOUND,
    ),
    (
        exceptions.RayActorNameClashError("test RayActorNameClashError msg"),
        ["Ray Actor name clash"],
        ResponseStatusCode.CONNECTION_ERROR,
    ),
]


@pytest.fixture(scope="module")
def ray():
    _ray.init()
    _ray.workflow.init()
    yield
    _ray.shutdown()


@pytest.mark.parametrize("argv, action", ARGV_ACTION)
@pytest.mark.parametrize("exception, tell_tale, expected_code", EXCEPTION_MESSAGE_CODE)
def test_response_codes(
    monkeypatch,
    exception,
    tell_tale,
    expected_code,
    argv,
    action,
    capsys,
):
    """Check that errors raised from the actions cause the correct error
    responses to be returned.
    """
    monkeypatch.setattr(sys, "argv", ["orq"] + argv)
    monkeypatch.setattr(
        orquestra.sdk._base.cli._corq._main.action, action, Mock(side_effect=exception)
    )

    with pytest.raises(SystemExit) as e:
        orquestra.sdk._base.cli._corq._main.main_cli()
    captured = capsys.readouterr()

    assert e.value.code == expected_code.value, (
        f"Expected code '{expected_code.value}' "
        f"for raised exception '{exception}', "
        f"but got code '{e.value.code}'."
    )
    assert str(exception) in captured.out, (
        f"Expected to find exception identifier '{str(exception)}' in output, "
        f"but found {captured.out}"
    )
    for telltale_string in tell_tale:
        assert telltale_string in captured.out, (
            f"Expected to find telltale '{telltale_string}' in output, "
            f"but found {captured.out}"
        )


class TestTracebackLogging:
    @staticmethod
    def test_argparse_interprets_verbose_flag_as_debug_logging_level(monkeypatch):
        monkeypatch.setattr(sys, "argv", ["orq", "-v"])

        parser = orquestra.sdk._base.cli._corq._main.make_v2_parser()
        args = parser.parse_args()

        assert args.verbose == logging.DEBUG

    @staticmethod
    def test_argparse_sets_default_logging_level_to_info(monkeypatch):
        monkeypatch.setattr(sys, "argv", ["orq"])

        parser = orquestra.sdk._base.cli._corq._main.make_v2_parser()
        args = parser.parse_args()

        assert args.verbose == logging.INFO

    @staticmethod
    @pytest.mark.parametrize("argv, action", ARGV_ACTION)
    @pytest.mark.parametrize(
        "exception, tell_tale, expected_code", EXCEPTION_MESSAGE_CODE
    )
    def test_traceback_logged_to_stderr_when_verbose_flag_set(
        argv,
        action,
        exception,
        tell_tale,
        expected_code,
        monkeypatch,
        capsys,
        caplog,
    ):
        """There's a bug in capsys that makes it impossible to examine logs to stderr
        when using it, so as a workaround we temporarily set pytests global logging
        level and examine the capture from that instead.

        The assertion that the traceback _not_ be included in the stdout holds for all
        handled exceptions, but not for unhandled exceptions where the traceback is
        included in the response object.
        """
        monkeypatch.setattr(sys, "argv", ["orq", "-v"] + argv)
        monkeypatch.setattr(
            orquestra.sdk._base.cli._corq._main.action,
            action,
            Mock(side_effect=exception),
        )

        with caplog.at_level(0):
            with pytest.raises(SystemExit):
                orquestra.sdk._base.cli._corq._main.main_cli()

        assert "Traceback (most recent call last):" in caplog.text
        assert str(exception) in caplog.text
        assert "Traceback (most recent call last):" not in capsys.readouterr().out

    @staticmethod
    @pytest.mark.parametrize("argv, action", ARGV_ACTION)
    @pytest.mark.parametrize(
        "exception, tell_tale, expected_code", EXCEPTION_MESSAGE_CODE
    )
    def test_traceback_not_logged_to_stderr_when_verbose_flag_not_set(
        argv,
        action,
        exception,
        tell_tale,
        expected_code,
        monkeypatch,
        capsys,
        caplog,
    ):
        """There's a bug in capsys that makes it impossible to examine logs to stderr
        when using it, so as a workaround we temporarily set pytests global logging
        level and examine the capture from that instead.
        """
        monkeypatch.setattr(sys, "argv", ["orq"] + argv)
        monkeypatch.setattr(
            orquestra.sdk._base.cli._corq._main.action,
            action,
            Mock(side_effect=exception),
        )

        with caplog.at_level(0):
            with pytest.raises(SystemExit):
                orquestra.sdk._base.cli._corq._main.main_cli()

        assert "Traceback (most recent call last):" not in caplog.text
        assert "Traceback (most recent call last):" not in capsys.readouterr().out


@pytest.mark.slow
@pytest.mark.xfail(reason="Raises a storage error instead, needs investigating.")
def test_race_condition_error_handling(ray, capsys, tmp_path, monkeypatch):
    """
    Confirm that errors arising due to race conditions in starting the local
    runtime are reported correctly - stdout should only contain json-formatted
    return objects, not traceback.

    For more information see ORQSDK-352
    """
    output = subprocess.run(
        """for i in 1 2; do orq get workflow-run -c local -o json& done""",
        capture_output=True,
        shell=True,
        encoding="UTF-8",
        cwd=tmp_path,
    )

    # stdout should parse as valid json
    responses = [json.loads(line) for line in output.stdout.splitlines()]

    # Failing response should have a suitable error message
    message = (
        "This is likely due to a race condition in starting the local runtime. "
        "Please try again."
    )
    for response in responses:
        if not response["meta"]["success"]:
            assert message in response["meta"]["message"]
