################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Centralized place to store the information about where the code runs.

This module is likely to be used from many places. It shouldn't import any
3rd-party libraries (like ray).

The problem this module solves: our code needs to know if it's running in a standalone
script vs a local Ray task vs [insert more in the future]. How to pass
this information?

The solution: global variable set by the runtimes just before executing task code. To
make this as robust as possible, the flag needs to be set inside the same process that
executes the task, otherwise we risk bugs related to passing global state across
workers.
"""

from contextlib import contextmanager
from enum import Enum


class ExecContext(Enum):
    # No specific execution context set. Running regular Python code, e.g. in a Python
    # REPL or a user script.
    DIRECT = "DIRECT"

    # We're running the function marked as `@sdk.workflow` to get the workflow graph.
    WORKFLOW_BUILD = "WORKFLOW_BUILD"

    # We're inside a Ray task. This can be managed by the Local Runtime or remote
    # Compute Engine.
    RAY = "RAY"


# To be set by runtimes.
# To be read by anybody inside our code.
global_context: ExecContext = ExecContext.DIRECT


@contextmanager
def _set_context(ctx: ExecContext):
    global global_context

    prev_val = global_context
    global_context = ctx

    try:
        yield
    finally:
        global_context = prev_val


@contextmanager
def workflow_build():
    """Helper. Sets the context for workflow traversal."""
    with _set_context(ExecContext.WORKFLOW_BUILD):
        yield


@contextmanager
def ray():  # pragma: no cover - tested inside a Ray task via an integration test.
    """Helper. Sets the context for running code in a task managed by Ray."""
    with _set_context(ExecContext.RAY):
        yield


def get_current_exec_context() -> ExecContext:
    """Getter for the current execution context."""
    return global_context
