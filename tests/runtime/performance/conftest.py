################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import signal

import pytest


class TimeoutError(Exception):
    pass


@pytest.hookimpl
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "expect_under(10): Expect a test to finish under a certain time (in seconds)",
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    failed = False

    def _cancel(signal, frame):
        nonlocal failed
        failed = True

    for mark in item.iter_markers():
        if mark.name == "expect_under":
            try:
                timeout = mark.args[0]
            except IndexError:
                timeout = 10
            signal.signal(signal.SIGALRM, _cancel)
            signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        if failed:
            raise TimeoutError(f"Test timed out after {timeout}s.")
