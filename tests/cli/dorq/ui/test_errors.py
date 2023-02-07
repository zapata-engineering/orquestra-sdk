################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base.cli._dorq._ui import _errors
from orquestra.sdk.schema.workflow_run import State


class TestPrettyPrintException:
    @staticmethod
    @pytest.mark.parametrize(
        "exc,stdout_marker",
        [
            (
                exceptions.WorkflowDefinitionModuleNotFound(
                    "my_module", ["foo", "bar/baz"]
                ),
                "couldn't find workflow definitions module",
            ),
            (
                exceptions.NoWorkflowDefinitionsFound("my_module"),
                "couldn't find any workflow definitions",
            ),
            (exceptions.UnauthorizedError(), "log in again"),
            (
                exceptions.WorkflowSyntaxError(
                    "Workflow arguments must be known at submission time."
                ),
                (
                    "Invalid workflow syntax. Workflow arguments must be known at"
                    " submission time."
                ),
            ),
            (
                exceptions.WorkflowRunNotSucceeded(message="Foo", state=State.FAILED),
                (
                    "This action only works with succeeded workflows. However, the "
                    "selected run is FAILED."
                ),
            ),
            (
                exceptions.WorkflowRunNotFinished(message="Foo", state=State.RUNNING),
                (
                    "This action only works with finished workflows. However, the "
                    "selected run is RUNNING."
                ),
            ),
            (ConnectionError("Unable to connect to Ray"), "Unable to connect to Ray"),
            (
                exceptions.LoginURLUnavailableError("localhost"),
                "The login URL for 'localhost' is unavailable. "
                "Try checking your network connection and the cluster URL.",
            ),
        ],
    )
    def test_prints_to_std_streams(capsys, exc, stdout_marker: str):
        # Given
        try:
            # Simulate raising the exception object. This is supposed to realistically
            # set the stack trace.
            raise exc
        except Exception as e:
            # When
            _errors.pretty_print_exception(e)

        # Then
        captured = capsys.readouterr()

        # Verifies that we describe the failure reason to the user.
        assert stdout_marker in captured.out

        # Verifies that user sees the exception class. This is useful for bug reports
        # and debugging.
        assert type(exc).__name__ in captured.err

        # Verifies that user sees the stack trace. This is useful for bug reports and
        # debugging.
        assert "Traceback (most recent call last):\n  File" in captured.err
