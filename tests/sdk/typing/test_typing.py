################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
The purpose of these tests are to ensure type hints are correctly mapped from the user's
function to the final SDK object.

This also includes correctly showing attributes/methods/properties that the SDK provides
on top of the user's function. For example, `WorkflowDef.local_run()`.

We're going to use `mypy` for these tests as we already assume it's installed. However,
we can't rely on the style checks for these tests: we also test for errors that could
happen.
"""
from pathlib import PurePath

import pytest
from mypy import api

NO_IMPLICIT_REEXPORT = "--no-implicit-reexport"


def idfn(val):
    if isinstance(val, str):
        if "/" in val:
            return str(PurePath(val).stem)


@pytest.fixture(scope="module")
def shared_cache(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("type-checking")


@pytest.mark.parametrize(
    "filename, expected_output, extra_args",
    [
        # task decorator
        (
            "tests/sdk/typing/task/assign_n_outputs.py",
            'error: Property "n_outputs" defined in "TaskDef" is read-only',
            [],
        ),
        (
            "tests/sdk/typing/task/has_n_outputs.py",
            "Success: no issues found in 1 source file",
            [],
        ),
        (
            "tests/sdk/typing/task/passed_correct_arg_type.py",
            "Success: no issues found in 1 source file",
            [],
        ),
        (
            "tests/sdk/typing/task/using_artifact_functions.py",
            "Success: no issues found in 1 source file",
            [],
        ),
        (
            "tests/sdk/typing/task/using_artifact_feature_as_object.py",
            'error: Unsupported operand types for - ("ArtifactFuture[int]" and "int")',
            [],
        ),
        # workflow decorator
        (
            "tests/sdk/typing/workflow/assign_model.py",
            'error: Property "model" defined in "WorkflowTemplate" is read-only',
            [],
        ),
        (
            "tests/sdk/typing/workflow/has_model.py",
            "Success: no issues found in 1 source file",
            [],
        ),
        (
            "tests/sdk/typing/workflow/does_not_have_validate.py",
            'error: "WorkflowTemplate[[], Any]" has no attribute "validate"',
            [],
        ),
        # Ensure API exported
        # We use the `--no-implicit-reexport` flag to check for this. This flag slows
        # mypy, so we only add it for this test.
        (
            "tests/sdk/typing/full_example.py",
            "Success: no issues found in 1 source file",
            [NO_IMPLICIT_REEXPORT],
        ),
    ],
    ids=idfn,
)
def test_typing(shared_cache, filename, expected_output, extra_args):
    args = ["--cache-dir", f"{shared_cache}", *extra_args, filename]
    result = api.run(args=args)
    assert expected_output in result[0]
