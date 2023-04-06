################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from pathlib import Path

from orquestra import sdk


@sdk.task(source_import=sdk.InlineImport())
def two_outputs(a, b):
    return a + 1, b + 2


@sdk.task(source_import=sdk.InlineImport())
def single_output(a):
    return a + 3


@sdk.workflow
def unpacking_wf():
    packed1 = two_outputs(11, 12)
    _, b1 = packed1

    single2 = single_output(b1)

    packed3 = two_outputs(packed1, single2)
    a3, _ = packed3

    return b1, packed1, a3
