################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
This script dumps a workflow IR for regression tests. To regenerate the IR:
1. Pick the SDK version you want to validate. ``pip install orquestra-sdk==<version>``.
2. Run this file: ``python tests/sdk/v2/schema/data/unpacking.py``.
3. Commit in the changes in ``unpacking_wf.json``.

Note: the whole purpose of this fixture is to record old IRs. It probably shouldn't be
regenerated often.
"""
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


def main():
    wf_def_model = unpacking_wf().model

    target_path = Path(__file__).parent / "unpacking_wf.json"
    target_path.write_text(wf_def_model.json(indent=2))


if __name__ == "__main__":
    main()
