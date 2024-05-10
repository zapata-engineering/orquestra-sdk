################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Example workflow_defs file used in tests."""

import orquestra.sdk as sdk


def _increment(x):
    """Example of a module-level function, that's neither a task def nor
    workflow def."""
    return x + 1


@sdk.task
def sample_task(a, b):
    return a, _increment(b)


@sdk.workflow
def my_wf():
    num1 = 420
    num2 = 2037

    num3, num4 = sample_task(num1, num2)
    return [num3, num4]
