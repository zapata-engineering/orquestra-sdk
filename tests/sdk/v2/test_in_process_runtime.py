################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Additional tests for orquestra.sdk._base._in_process_runtime. Most of the
functionality is covered inside tests/v2/test_api.py.

We need this file because test_api.py might be too coarse for some scenarios.
When adding new tests, please consider that suite first.
"""
import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base._in_process_runtime import InProcessRuntime
from orquestra.sdk.schema import ir

from .data.complex_serialization.workflow_defs import (
    wf_pass_callables_from_task,
    wf_pass_tuple,
)


@pytest.fixture
def runtime():
    return InProcessRuntime()


@pytest.fixture
def wf_def() -> ir.WorkflowDef:
    return wf_pass_tuple().model


def test_single_run(runtime, wf_def):
    run_id = runtime.create_workflow_run(wf_def)
    assert runtime.get_workflow_run_outputs(run_id) == 3


def test_same_outputs(runtime, wf_def):
    run_id = runtime.create_workflow_run(wf_def)
    assert runtime.get_workflow_run_outputs(
        run_id
    ) == runtime.get_workflow_run_outputs_non_blocking(run_id)


def test_multiple_runs(runtime):
    wf_def1 = wf_pass_tuple().model
    wf_def2 = wf_pass_callables_from_task().model

    run_id1 = runtime.create_workflow_run(wf_def1)
    run_id2 = runtime.create_workflow_run(wf_def2)

    outputs1 = runtime.get_workflow_run_outputs(run_id1)
    outputs2 = runtime.get_workflow_run_outputs(run_id2)

    assert run_id1 != run_id2
    assert outputs1 != outputs2


class TestStop:
    @staticmethod
    def test_existing_run(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def)

        # When
        runtime.stop_workflow_run(run_id)

        # Then
        # Not much. We expect the above to be a noop.

    @staticmethod
    def test_non_existing_run(runtime):
        # Given
        run_id = "doesn't exist"

        # Then
        with pytest.raises(exceptions.WorkflowRunNotFoundError):
            # When
            runtime.stop_workflow_run(run_id)
