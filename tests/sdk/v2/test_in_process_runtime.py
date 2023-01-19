################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Additional tests for orquestra.sdk._base._in_process_runtime. Most of the
functionality is covered inside tests/v2/test_api.py.

We need this file because test_api.py might be too coarse for some scenarios.
When adding new tests, please consider that suite first.
"""
from datetime import datetime, timedelta, timezone

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base._in_process_runtime import InProcessRuntime
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import State, WorkflowRunId

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


class TestQueriesAfterRunning:
    """
    Submits a well-known workflow and tests the "query" methods.
    """

    @staticmethod
    @pytest.fixture
    def run_id(runtime: InProcessRuntime, wf_def) -> WorkflowRunId:
        run_id = runtime.create_workflow_run(wf_def)
        return run_id

    class TestGetWorkflowRunOutputs:
        @staticmethod
        def test_number_of_outputs(runtime, run_id):
            assert runtime.get_workflow_run_outputs(run_id) == 3

        @staticmethod
        def test_matches_non_blocking(runtime, run_id):
            outputs = runtime.get_workflow_run_outputs(run_id)

            assert outputs == runtime.get_workflow_run_outputs_non_blocking(run_id)

        @staticmethod
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


class TestListWorkflowRuns:
    @staticmethod
    def test_happy_path(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs()

        # Then
        assert len(wf_runs) == 2

    @staticmethod
    def test_limit(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs(limit=1)

        # Then
        assert len(wf_runs) == 1

    @staticmethod
    def test_state_running(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs(state=State.RUNNING)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 0

    @staticmethod
    def test_state_succeeded(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs(state=State.SUCCEEDED)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1

    @staticmethod
    def test_max_age(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def)
        _ = runtime.create_workflow_run(wf_def)
        runtime._start_time_store[run_id] = datetime.now(timezone.utc) - timedelta(
            days=1
        )

        # When
        wf_runs = runtime.list_workflow_runs(max_age=timedelta(minutes=1))

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1
