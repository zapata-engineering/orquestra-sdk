################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################
import re
from typing import Callable, List

import pytest

from orquestra.sdk._client._base.cli._ui import _errors
from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.schema.workflow_run import State

from . import exception_makers


@pytest.mark.parametrize(
    "exception_func, expected_lines",
    (
        (
            exception_makers.except_plain,
            [
                r"RuntimeError: Unable to do thing \(test_print_traceback:test_errors.py:[\d]+\)",  # noqa: E501
                r"  test_print_traceback:test_errors.py:[\d]+",
                r"  except_plain:exception_makers.py:[\d]+",
            ],
        ),
        (
            exception_makers.except_no_message,
            [
                r"RuntimeError \(test_print_traceback:test_errors.py:[\d]+\)",
                r"  test_print_traceback:test_errors.py:[\d]+",
                r"  except_no_message:exception_makers.py:[\d]+",
            ],
        ),
        (
            exception_makers.except_stack,
            [
                r"RuntimeError: Unable to do thing \(test_print_traceback:test_errors.py:[\d]+\)",  # noqa: E501
                r"  test_print_traceback:test_errors.py:[\d]+",
                r"  except_stack:exception_makers.py:[\d]+",
                r"  _b:exception_makers.py:[\d]+",
                r"  _a:exception_makers.py:[\d]+",
                r"  except_plain:exception_makers.py:[\d]+",
            ],
        ),
        (
            exception_makers.except_from,
            [
                r"RuntimeError: Unable to do thing \(test_print_traceback:test_errors.py:[\d]+\)",  # noqa: E501
                r"  test_print_traceback:test_errors.py:[\d]+",
                r"  except_from:exception_makers.py:[\d]+",
                r"Caused by:",
                r"  ValueError: Invalid file \(except_from:exception_makers.py:[\d]+\)",
                r"  Caused by:",
                r"    KeyError: 'key' \(except_from:exception_makers.py:[\d]+\)",
            ],
        ),
        (
            exception_makers.except_within_except,
            [
                r"RuntimeError: Unable to do thing \(test_print_traceback:test_errors.py:[\d]+\)",  # noqa: E501
                r"  test_print_traceback:test_errors.py:[\d]+",
                r"  except_within_except:exception_makers.py:[\d]+",
                r"While handling:",
                r"  ValueError: Invalid file \(except_within_except:exception_makers.py:[\d]+\)",  # noqa: E501
                r"  While handling:",
                r"    KeyError: 'key' \(except_within_except:exception_makers.py:[\d]+\)",  # noqa: E501
            ],
        ),
        (
            exception_makers.except_from_within_except,
            [
                r"RuntimeError: Unable to do thing \(test_print_traceback:test_errors.py:[\d]+\)",  # noqa: E501
                r"  test_print_traceback:test_errors.py:[\d]+",
                r"  except_from_within_except:exception_makers.py:[\d]+",
                r"Caused by:",
                r"  ValueError: Invalid file \(except_from_within_except:exception_makers.py:[\d]+\)",  # noqa: E501
                r"  While handling:",
                r"    KeyError: 'key' \(except_from_within_except:exception_makers.py:[\d]+\)",  # noqa: E501
            ],
        ),
    ),
)
def test_print_traceback(
    capsys: pytest.CaptureFixture[str],
    exception_func: Callable[[], None],
    expected_lines: List[str],
):
    """
    This tests the traceback printing with some contrived examples.
    Other tests in this file use proper `orquestra-sdk` exceptions
    """
    try:
        exception_func()
    except Exception as e:
        _errors._print_traceback(e)

    captured = capsys.readouterr()
    actual_lines = captured.err.splitlines()

    assert len(expected_lines) == len(actual_lines)

    for expected_line, actual_line in zip(expected_lines, actual_lines):
        match = re.match(expected_line, actual_line)
        assert match is not None, (expected_line, actual_line)


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
    def test_prints_to_std_streams(
        capsys: pytest.CaptureFixture[str], exc: Exception, stdout_marker: str
    ):
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
        # We know the file and function, so let's search for that.
        # We expect one instance on the Exception line, and one in the traceback
        assert captured.err.count("test_prints_to_std_streams:test_errors.py") == 2

    @staticmethod
    @pytest.mark.parametrize(
        "exc,stdout_marker",
        [
            (
                exceptions.InProcessFromCLIError,
                'The "in_process" runtime is designed for debugging and testing via the'
                " Python API only",
            ),
            (exceptions.InvalidTokenError, "The auth token is not valid"),
            (exceptions.ExpiredTokenError, "The auth token has expired"),
            (
                exceptions.RayNotRunningError(),
                "Could not find any running Ray instance. You can use 'orq status' to check the status of the ray service. If it is not running, it can be started with the `orq up` command.",  # noqa: E501
            ),
            (
                exceptions.WorkflowRunNotStarted("An issue submitting the workflow"),
                "An issue submitting the workflow",
            ),
            (exceptions.QERemoved("<qe removal text>"), "<qe removal text>"),
            (
                exceptions.RuntimeQuerySummaryError(
                    wf_run_id="wf.abc123.123",
                    not_found_runtimes=[],
                    unauthorized_runtimes=[],
                    not_running_runtimes=[],
                ),
                "Couldn't find a config",
            ),
        ],
    )
    def tests_prints_exception_without_traceback(capsys, exc, stdout_marker: str):
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
