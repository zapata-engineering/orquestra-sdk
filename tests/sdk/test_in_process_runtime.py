################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Additional tests for orquestra.sdk._base._in_process_runtime. Most of the
functionality is covered inside tests/v2/test_api.py.

We need this file because test_api.py might be too coarse for some scenarios.
When adding new tests, please consider that suite first.
"""
from datetime import timedelta
from unittest.mock import create_autospec

import pytest

from orquestra import sdk
from orquestra.sdk import exceptions
from orquestra.sdk._client._base._in_process_runtime import InProcessRuntime
from orquestra.sdk._client._base._spaces._structs import ProjectRef
from orquestra.sdk._client._base._testing._example_wfs import (
    wf_with_explicit_n_outputs,
    wf_with_secrets,
)
from orquestra.sdk._client.secrets import _client, _models
from orquestra.sdk.shared import _dates, serde
from orquestra.sdk.shared.schema import ir
from orquestra.sdk.shared.schema.workflow_run import State, WorkflowRunId

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


@sdk.task(n_outputs=2)
def two_outputs(a, b):
    return a + b, a - b


@sdk.workflow
def wf_with_unused_outputs():
    out1, _ = two_outputs(5, 1)
    return out1


@sdk.workflow
def wf_all_used():
    out1, out2 = two_outputs(5, 1)
    return out1, out2


@pytest.fixture
def wf_def_unused_outputs() -> ir.WorkflowDef:
    return wf_with_unused_outputs().model


@pytest.fixture
def wf_def_all_used() -> ir.WorkflowDef:
    return wf_all_used().model


def test_secret_inside_ir(
    tmp_default_config_json, monkeypatch: pytest.MonkeyPatch, runtime: InProcessRuntime
):
    mocked_secret = _models.SecretDefinition(
        name="a-secret", value="mocked", resourceGroup=None
    )
    get_secret = create_autospec(_client.SecretsClient.get_secret)
    get_secret.return_value = mocked_secret
    monkeypatch.setattr(_client.SecretsClient, "get_secret", get_secret)

    run_id = runtime.create_workflow_run(wf_with_secrets().model, None, dry_run=False)
    result = runtime.get_workflow_run_outputs_non_blocking(run_id)

    assert result == (serde.result_from_artifact("Mocked", ir.ArtifactFormat.AUTO),)


def test_explicit_n_outputs_single(runtime: InProcessRuntime):
    run_id = runtime.create_workflow_run(
        wf_with_explicit_n_outputs().model, None, dry_run=False
    )
    result = runtime.get_workflow_run_outputs_non_blocking(run_id)

    assert result == (serde.result_from_artifact(True, ir.ArtifactFormat.AUTO),)


class TestQueriesAfterRunning:
    """
    Submits a well-known workflow and tests the "query" methods.
    """

    @staticmethod
    @pytest.fixture
    def run_id(runtime: InProcessRuntime, wf_def) -> WorkflowRunId:
        run_id = runtime.create_workflow_run(wf_def, None, dry_run=False)
        return run_id

    class TestGetWorkflowRunOutputs:
        @staticmethod
        def test_output_values(runtime, run_id):
            assert runtime.get_workflow_run_outputs_non_blocking(run_id) == (
                serde.result_from_artifact(3, ir.ArtifactFormat.AUTO),
            )

        @staticmethod
        def test_multiple_runs(runtime):
            wf_def1 = wf_pass_tuple().model
            wf_def2 = wf_pass_callables_from_task().model

            run_id1 = runtime.create_workflow_run(wf_def1, None, dry_run=False)
            run_id2 = runtime.create_workflow_run(wf_def2, None, dry_run=False)

            outputs1 = runtime.get_workflow_run_outputs_non_blocking(run_id1)
            outputs2 = runtime.get_workflow_run_outputs_non_blocking(run_id2)

            assert run_id1 != run_id2
            assert outputs1 != outputs2

    class TestGetAvailableOutputs:
        @staticmethod
        def test_single_task_output(runtime, run_id):
            """
            The 'run_id' fixture is a result of submitting a workflow def with a
            single-output task.
            """
            assert runtime.get_available_outputs(run_id) == {
                "invocation-0-task-sum-tuple-numbers": serde.result_from_artifact(
                    3, ir.ArtifactFormat.AUTO
                )
            }

        class TestMultipleTaskOutputs:
            @staticmethod
            def test_some_unused(runtime, wf_def_unused_outputs):
                run_id = runtime.create_workflow_run(
                    wf_def_unused_outputs, None, dry_run=False
                )

                assert runtime.get_available_outputs(run_id) == {
                    "invocation-0-task-two-outputs": serde.result_from_artifact(
                        (6, 4), ir.ArtifactFormat.AUTO
                    ),
                }

            @staticmethod
            def test_all_used(runtime, wf_def_all_used):
                run_id = runtime.create_workflow_run(
                    wf_def_all_used, None, dry_run=False
                )

                assert runtime.get_available_outputs(run_id) == {
                    "invocation-0-task-two-outputs": serde.result_from_artifact(
                        (6, 4), ir.ArtifactFormat.AUTO
                    ),
                }


class TestStop:
    @staticmethod
    def test_existing_run(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def, None, dry_run=False)

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
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_runs()

        # Then
        assert len(wf_runs) == 2

    @staticmethod
    def test_limit(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_runs(limit=1)

        # Then
        assert len(wf_runs) == 1

    @staticmethod
    def test_state_running(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_runs(state=State.RUNNING)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 0

    @staticmethod
    def test_state_succeeded(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_runs(state=State.SUCCEEDED)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1

    @staticmethod
    def test_max_age(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def, None, dry_run=False)
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)
        runtime._start_time_store[run_id] = _dates.now() - timedelta(days=1)

        # When
        wf_runs = runtime.list_workflow_runs(max_age=timedelta(minutes=1))

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1


class TestListWorkflowRunSummaries:
    @staticmethod
    def test_happy_path(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_run_summaries()

        # Then
        assert len(wf_runs) == 2

    @staticmethod
    def test_limit(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_run_summaries(limit=1)

        # Then
        assert len(wf_runs) == 1

    @staticmethod
    def test_state_running(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_run_summaries(state=State.RUNNING)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 0

    @staticmethod
    def test_state_succeeded(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)

        # When
        wf_runs = runtime.list_workflow_run_summaries(state=State.SUCCEEDED)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1

    @staticmethod
    def test_max_age(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def, None, dry_run=False)
        _ = runtime.create_workflow_run(wf_def, None, dry_run=False)
        runtime._start_time_store[run_id] = _dates.now() - timedelta(days=1)

        # When
        wf_runs = runtime.list_workflow_run_summaries(max_age=timedelta(minutes=1))

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1


class TestUnsupportedMethods:
    @staticmethod
    @pytest.mark.parametrize(
        "method",
        [
            InProcessRuntime.from_runtime_configuration,
        ],
    )
    def test_raises(runtime, method):
        with pytest.raises(NotImplementedError):
            method(runtime)


def test_project_raises_warning(runtime, wf_def):
    # Given
    with pytest.warns(expected_warning=exceptions.UnsupportedRuntimeFeature):
        _ = runtime.create_workflow_run(
            wf_def, project=ProjectRef(workspace_id="", project_id=""), dry_run=False
        )


def test_dry_run_raises_warning(runtime, wf_def):
    # Given
    with pytest.warns(expected_warning=exceptions.UnsupportedRuntimeFeature):
        _ = runtime.create_workflow_run(wf_def, project=None, dry_run=True)
