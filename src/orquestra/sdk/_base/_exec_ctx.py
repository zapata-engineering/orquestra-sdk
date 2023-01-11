################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Centralized place to store the information about where the code runs.
This module is likely to be used from many places. It shouldn't import any
3rd-party libraries (like ray).

The problem this module solves: our code needs to know if it's running in a standalone
script vs a local Ray task vs task on QE vs [insert more in the future]. How to pass
this information?

The solution: global variable set by the runtimes just before executing task code. To
make this as robust as possible, the flag needs to be set inside the same process that
executes the task, otherwise we risk bugs related to passing global state across
workers.
"""

from contextlib import contextmanager
from enum import Enum


class ExecContext(Enum):
    LOCAL_DIRECT = "LOCAL_DIRECT"
    LOCAL_RAY = "LOCAL_RAY"
    PLATFORM_QE = "PLATFORM_QE"


# To be set by runtimes.
# To be read by anybody inside our code.
global_context: ExecContext = ExecContext.LOCAL_DIRECT


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
def local_ray():
    """
    Helper. Sets the context for running code in a task on a local Ray cluster.
    """
    with _set_context(ExecContext.LOCAL_RAY):
        yield


@contextmanager
def platform_qe():
    """
    Helper. Sets the context for running code in a remote, QE-based task.
    """
    with _set_context(ExecContext.PLATFORM_QE):
        yield
