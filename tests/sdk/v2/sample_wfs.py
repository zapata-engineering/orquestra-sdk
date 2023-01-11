################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################

"""
Workflow and task defs used in tests.
"""

from orquestra import sdk


@sdk.task
def add(a, b):
    return a + b


@sdk.task
def inc(a):
    return a + 1


@sdk.task(source_import=sdk.InlineImport())
def inc_2(a):
    return a + 2


@sdk.task
def integer_division(a, b):
    return a // b, a % b


@sdk.workflow
def wf(param: int):
    floor, remainder = integer_division(param, 3)
    floor_plus_6 = add(floor, 6)
    remainder_plus_3 = inc(inc_2(remainder))

    return floor_plus_6, remainder_plus_3
