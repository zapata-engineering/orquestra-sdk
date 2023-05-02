# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from datetime import timedelta
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, Mock, call, create_autospec

import pytest

from orquestra.sdk import Project, Workspace, exceptions
from orquestra.sdk._base import serde
from orquestra.sdk._base._driver import _ce_runtime, _client, _exceptions, _models
from orquestra.sdk._base._testing._example_wfs import (
    my_workflow,
    workflow_parametrised_with_resources,
    workflow_with_different_resources,
)
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.ir import ArtifactFormat, WorkflowDef
from orquestra.sdk.schema.responses import ComputeEngineWorkflowResult, JSONResult
from orquestra.sdk.schema.workflow_run import (
    RunStatus,
    State,
    WorkflowRun,
    WorkflowRunId,
)


class TestInitialization:
    @pytest.mark.parametrize("proj_dir", [".", Path(".")])
    @pytest.mark.parametrize("verbose", [True, False])
    def test_passing_project_dir_and_config_obj(self, proj_dir, verbose):
        """
        - GIVEN a CE runtime configuration
        - WHEN creating a CERuntime instance via the constructor or factory method
        - THEN the resulting runtime objects have the same options
        """
        config = RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.CE_REMOTE,
            runtime_options={"uri": "http://localhost", "token": "blah"},
        )

        # when
        rt = _ce_runtime.CERuntime(config=config, verbose=verbose)
        rt2 = _ce_runtime.CERuntime.from_runtime_configuration(
            project_dir=proj_dir, config=config, verbose=verbose
        )

        # then
        assert rt._config == rt2._config  # type: ignore
        assert rt._verbose == rt2._verbose  # type: ignore

    def test_invalid_config(self):
        config = RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.CE_REMOTE,
            runtime_options={},
        )
        with pytest.raises(exceptions.RuntimeConfigError):
            _ce_runtime.CERuntime(config)


@pytest.fixture
def mocked_client(monkeypatch: pytest.MonkeyPatch):
    mocked_client = MagicMock(spec=_client.DriverClient)
    mocked_client.from_token.return_value = mocked_client
    monkeypatch.setattr(
        "orquestra.sdk._base._driver._client.DriverClient", mocked_client
    )
    return mocked_client


@pytest.fixture
def workflow_def_id():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def workflow_run_id():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def workflow_run_status(workflow_run_id: WorkflowRunId):
    def _workflow_run(state: State):
        workflow_def_mock = create_autospec(WorkflowDef)
        return WorkflowRun(
            id=workflow_run_id,
            workflow_def=workflow_def_mock,
            task_runs=[],
            status=RunStatus(
                state=state,
                start_time=None,
                end_time=None,
            ),
        )

    return _workflow_run


class TestCreateWorkflowRun:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_def_id: str,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.create_workflow_def.return_value = workflow_def_id
        mocked_client.create_workflow_run.return_value = workflow_run_id

        # When
        wf_run_id = runtime.create_workflow_run(my_workflow.model, None)

        # Then
        mocked_client.create_workflow_def.assert_called_once_with(
            my_workflow.model, None
        )
        mocked_client.create_workflow_run.assert_called_once_with(
            workflow_def_id,
            _models.Resources(cpu=None, memory=None, gpu=None, nodes=None),
        )
        assert isinstance(wf_run_id, WorkflowRunId)
        assert (
            wf_run_id == workflow_run_id
        ), "Workflow run ID is returned directly from the client"

    class TestWithResources:
        def test_with_memory(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_def_id: str,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.create_workflow_def.return_value = workflow_def_id
            mocked_client.create_workflow_run.return_value = workflow_run_id

            # When
            _ = runtime.create_workflow_run(
                workflow_parametrised_with_resources(memory="10Gi").model, None
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu=None, memory="10Gi", gpu=None, nodes=None),
            )

        def test_with_cpu(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_def_id: str,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.create_workflow_def.return_value = workflow_def_id
            mocked_client.create_workflow_run.return_value = workflow_run_id

            # When
            _ = runtime.create_workflow_run(
                workflow_parametrised_with_resources(cpu="1000m").model, None
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu="1000m", memory=None, gpu=None, nodes=None),
            )

        def test_with_gpu(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_def_id: str,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.create_workflow_def.return_value = workflow_def_id
            mocked_client.create_workflow_run.return_value = workflow_run_id

            # When
            _ = runtime.create_workflow_run(
                workflow_parametrised_with_resources(gpu="1").model, None
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu=None, memory=None, gpu="1", nodes=None),
            )

        def test_maximum_resource(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_def_id: str,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.create_workflow_def.return_value = workflow_def_id
            mocked_client.create_workflow_run.return_value = workflow_run_id

            # When
            _ = runtime.create_workflow_run(
                workflow_with_different_resources().model, None
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu="5000m", memory="3G", gpu="1", nodes=None),
            )

        def test_resources_from_workflow(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_def_id: str,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.create_workflow_def.return_value = workflow_def_id
            mocked_client.create_workflow_run.return_value = workflow_run_id

            # When
            _ = runtime.create_workflow_run(
                my_workflow()
                .with_resources(cpu="1", memory="1.5G", gpu="1", nodes=20)
                .model,
                None,
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu="1", memory="1.5G", gpu="1", nodes=20),
            )

    class TestWorkflowDefFailure:
        def test_invalid_wf_def(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = (
                _exceptions.InvalidWorkflowDef("message", "detail")
            )

            # When
            with pytest.raises(exceptions.WorkflowSyntaxError):
                _ = runtime.create_workflow_run(my_workflow.model, None)

        def test_unknown_http(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.create_workflow_run(my_workflow.model, None)

        @pytest.mark.parametrize(
            "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
        )
        def test_auth_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            failure_exc: Exception,
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = failure_exc

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.create_workflow_run(my_workflow.model, None)

    class TestWorkflowRunFailure:
        @pytest.fixture
        def mocked_client(self, mocked_client: MagicMock, workflow_def_id: str):
            mocked_client.create_workflow_def.return_value = workflow_def_id
            return mocked_client

        def test_invalid_wf_run(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = (
                _exceptions.InvalidWorkflowRunRequest("message", "detail")
            )

            # When
            with pytest.raises(exceptions.WorkflowRunNotStarted):
                _ = runtime.create_workflow_run(my_workflow.model, None)

        def test_unknown_http(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.create_workflow_run(my_workflow.model, None)

        @pytest.mark.parametrize(
            "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
        )
        def test_auth_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            failure_exc: Exception,
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = failure_exc

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.create_workflow_run(my_workflow.model, None)


class TestGetWorkflowRunStatus:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_workflow_run = MagicMock()
        mocked_client.get_workflow_run.return_value = mocked_workflow_run

        # When
        wf_run = runtime.get_workflow_run_status(workflow_run_id)

        # Then
        mocked_client.get_workflow_run.assert_called_once_with(workflow_run_id)
        assert (
            wf_run == mocked_workflow_run
        ), "Workflow run is returned directly from the client"

    def test_bad_workflow_run_id(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run.side_effect = _exceptions.InvalidWorkflowRunID(
            workflow_run_id
        )

        # When
        with pytest.raises(exceptions.WorkflowRunNotFoundError):
            _ = runtime.get_workflow_run_status(workflow_run_id)

    def test_missing_workflow_run(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run.side_effect = _exceptions.WorkflowRunNotFound(
            workflow_run_id
        )

        # When
        with pytest.raises(exceptions.WorkflowRunNotFoundError):
            _ = runtime.get_workflow_run_status(workflow_run_id)

    def test_unknown_http(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run.side_effect = _exceptions.UnknownHTTPError(
            MagicMock()
        )

        # When
        with pytest.raises(_exceptions.UnknownHTTPError):
            _ = runtime.get_workflow_run_status(workflow_run_id)

    @pytest.mark.parametrize(
        "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
    )
    def test_auth_failure(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
        failure_exc: Exception,
    ):
        # Given
        mocked_client.get_workflow_run.side_effect = failure_exc

        # When
        with pytest.raises(exceptions.UnauthorizedError):
            _ = runtime.get_workflow_run_status(workflow_run_id)


class TestGetWorkflowRunResultsNonBlocking:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run_results.return_value = ["result_id"]
        mocked_client.get_workflow_run_result.return_value = (
            ComputeEngineWorkflowResult(results=[JSONResult(value="[1]")])
        )

        # When
        results = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_results.assert_called_once_with(workflow_run_id)
        mocked_client.get_workflow_run_result.assert_has_calls([call("result_id")])
        assert results == (JSONResult(value="[1]"),)

    def test_happy_path_tuple(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run_results.return_value = [
            "result_id",
        ]
        mocked_client.get_workflow_run_result.side_effect = [
            ComputeEngineWorkflowResult(
                results=[JSONResult(value="1"), JSONResult(value="2")],
            )
        ]

        # When
        results = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_results.assert_called_once_with(workflow_run_id)
        mocked_client.get_workflow_run_result.assert_has_calls([call("result_id")])
        assert results == (JSONResult(value="1"), JSONResult(value="2"))

    class TestGetWorkflowRunResultsFailure:
        def test_bad_workflow_run_id(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_results.side_effect = (
                _exceptions.InvalidWorkflowRunID(workflow_run_id)
            )
            # When
            with pytest.raises(exceptions.WorkflowRunNotFoundError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        def test_workflow_run_not_found(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_results.side_effect = (
                _exceptions.WorkflowRunNotFound(workflow_run_id)
            )
            # When
            with pytest.raises(exceptions.WorkflowRunNotFoundError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        def test_no_results_not_succeeded(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            workflow_run_status,
        ):
            # Given
            mocked_client.get_workflow_run_results.return_value = []
            mocked_client.get_workflow_run.return_value = workflow_run_status(
                State.RUNNING
            )
            # When
            with pytest.raises(exceptions.WorkflowRunNotSucceeded):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        def test_no_results_succeeded(
            self,
            monkeypatch: pytest.MonkeyPatch,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            workflow_run_status,
        ):
            # Given
            mocked_client.get_workflow_run_results.return_value = []
            mocked_client.get_workflow_run.return_value = workflow_run_status(
                State.SUCCEEDED
            )
            monkeypatch.setattr(_ce_runtime._retry.time, "sleep", Mock())
            # When
            with pytest.raises(exceptions.WorkflowResultsNotReadyError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

            # We should try a few times if the results were not ready
            assert mocked_client.get_workflow_run_results.call_count == 5

        def test_eventually_get_results(
            self,
            monkeypatch: pytest.MonkeyPatch,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            workflow_run_status,
        ):
            # Given
            mocked_client.get_workflow_run_results.side_effect = [[], [], [Mock()]]
            mocked_client.get_workflow_run.return_value = workflow_run_status(
                State.SUCCEEDED
            )
            monkeypatch.setattr(_ce_runtime.serde, "deserialize", lambda x: x)
            monkeypatch.setattr(_ce_runtime._retry.time, "sleep", Mock())
            # When
            _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

            # We should have the results after 3 attempts
            assert mocked_client.get_workflow_run_results.call_count == 3

        def test_unknown_http(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_results.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        @pytest.mark.parametrize(
            "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
        )
        def test_auth_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            failure_exc: Exception,
        ):
            # Given
            mocked_client.get_workflow_run_results.side_effect = failure_exc

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

    class TestGetworkflowRunResultFailure:
        @pytest.fixture
        def mocked_client(self, mocked_client: MagicMock):
            mocked_client.get_workflow_run_results.return_value = ["result_id"]
            return mocked_client

        def test_unknown_http(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_result.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        @pytest.mark.parametrize(
            "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
        )
        def test_auth_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            failure_exc: Exception,
        ):
            # Given
            mocked_client.get_workflow_run_result.side_effect = failure_exc

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)


class TestGetAvailableOutputs:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run_artifacts.return_value = {
            f"{workflow_run_id}@task-inv-1": ["wf-art-1"],
            f"{workflow_run_id}@task-inv-2": ["wf-art-3"],
        }
        mocked_client.get_workflow_run_artifact.return_value = JSONResult(value="1")

        # When
        results = runtime.get_available_outputs(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_artifacts.assert_called_once_with(
            workflow_run_id
        )
        mocked_client.get_workflow_run_artifact.assert_has_calls(
            [call("wf-art-1"), call("wf-art-3")]
        )
        assert results == {
            "task-inv-1": JSONResult(value="1"),
            "task-inv-2": JSONResult(value="1"),
        }

    class TestGetWorkflowRunArtifactsFailure:
        def test_bad_workflow_run_id(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifacts.side_effect = (
                _exceptions.InvalidWorkflowRunID(workflow_run_id)
            )
            # When
            with pytest.raises(exceptions.WorkflowRunNotFoundError):
                _ = runtime.get_available_outputs(workflow_run_id)

        def test_workflow_run_not_found(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifacts.side_effect = (
                _exceptions.WorkflowRunNotFound(workflow_run_id)
            )
            # When
            with pytest.raises(exceptions.WorkflowRunNotFoundError):
                _ = runtime.get_available_outputs(workflow_run_id)

        def test_no_results(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifacts.return_value = {}
            # When
            outputs = runtime.get_available_outputs(workflow_run_id)
            # Then
            mocked_client.get_workflow_run_artifact.assert_not_called()
            assert outputs == {}

        def test_unknown_http(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifacts.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.get_available_outputs(workflow_run_id)

        @pytest.mark.parametrize(
            "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
        )
        def test_auth_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            failure_exc: Exception,
        ):
            mocked_client.get_workflow_run_artifacts.side_effect = failure_exc

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.get_available_outputs(workflow_run_id)

    class TestGetWorkflowRunArtifactFailure:
        @pytest.fixture
        def mocked_client(self, mocked_client: MagicMock, workflow_run_id):
            mocked_client.get_workflow_run_artifacts.return_value = {
                f"{workflow_run_id}@task-inv-1": ["wf-art-1"],
                f"{workflow_run_id}@task-inv-2": ["wf-art-3"],
            }
            return mocked_client

        def test_any_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifact.side_effect = Exception

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("wf-art-1"), call("wf-art-3")]
            )
            assert results == {}

        def test_returns_successful_artifacts_after_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifact.return_value = JSONResult(value="1")
            mocked_client.get_workflow_run_artifact.side_effect = (
                DEFAULT,
                Exception,
            )

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("wf-art-1"), call("wf-art-3")]
            )
            assert results == {"task-inv-1": JSONResult(value="1")}

        def test_continues_after_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifact.return_value = JSONResult(value="1")
            mocked_client.get_workflow_run_artifact.side_effect = (
                Exception,
                DEFAULT,
            )

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("wf-art-1"), call("wf-art-3")]
            )
            assert results == {
                "task-inv-2": JSONResult(value="1"),
            }

        def test_unknown_http(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifact.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("wf-art-1"), call("wf-art-3")]
            )
            assert results == {}

        @pytest.mark.parametrize(
            "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
        )
        def test_auth_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
            failure_exc: Exception,
        ):
            # Given
            mocked_client.get_workflow_run_artifact.side_effect = failure_exc

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("wf-art-1"), call("wf-art-3")]
            )
            assert results == {}


class TestStopWorkflowRun:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.terminate_workflow_run.return_value = None
        # When
        runtime.stop_workflow_run(workflow_run_id)
        # Then
        mocked_client.terminate_workflow_run.assert_called_once_with(workflow_run_id)

    def test_unknown_http(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.terminate_workflow_run.side_effect = _exceptions.UnknownHTTPError(
            MagicMock()
        )

        # When
        with pytest.raises(_exceptions.UnknownHTTPError):
            runtime.stop_workflow_run(workflow_run_id)

    @pytest.mark.parametrize(
        "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
    )
    def test_auth_failure(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
        failure_exc: Exception,
    ):
        mocked_client.terminate_workflow_run.side_effect = failure_exc

        # When
        with pytest.raises(exceptions.UnauthorizedError):
            runtime.stop_workflow_run(workflow_run_id)

    def test_workflow_run_not_found(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        mocked_client.terminate_workflow_run.side_effect = (
            _exceptions.WorkflowRunNotFound(workflow_run_id)
        )

        # When
        with pytest.raises(exceptions.WorkflowRunNotFoundError):
            runtime.stop_workflow_run(workflow_run_id)


class TestListWorkflowRuns:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        # Given
        wf_runs = [Mock(), Mock()]
        mocked_client.list_workflow_runs.return_value = _client.Paginated(
            contents=wf_runs
        )

        # When
        runs = runtime.list_workflow_runs()

        # Then
        mocked_client.list_workflow_runs.assert_called_once_with(
            page_size=None, page_token=None
        )
        assert runs == wf_runs

    @pytest.mark.parametrize(
        "limit, expected_requests",
        [
            (89, [call(page_size=89, page_token=None)]),
            (
                144,
                [
                    call(page_size=100, page_token=None),
                    call(page_size=44, page_token="<token sentinel 0>"),
                ],
            ),
            (
                233,
                [
                    call(page_size=100, page_token=None),
                    call(page_size=100, page_token="<token sentinel 0>"),
                    call(page_size=33, page_token="<token sentinel 1>"),
                ],
            ),
            (
                377,
                [
                    call(page_size=100, page_token=None),
                    call(page_size=100, page_token="<token sentinel 0>"),
                    call(page_size=100, page_token="<token sentinel 1>"),
                    call(page_size=77, page_token="<token sentinel 2>"),
                ],
            ),
        ],
    )
    def test_limit_applied_when_there_are_more_workflows(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        limit: int,
        expected_requests: list,
    ):
        # Given
        mocked_client.list_workflow_runs.side_effect = [
            _client.Paginated(
                contents=[Mock() for _ in range(100)],
                next_page_token=f"<token sentinel {i}>",
            )
            for i in range(4)
        ]

        # When
        _ = runtime.list_workflow_runs(limit=limit)

        # Then
        mocked_client.list_workflow_runs.assert_has_calls(expected_requests)

    @pytest.mark.parametrize(
        "limit, expected_requests",
        [
            (89, [call(page_size=89, page_token=None)]),
            (144, [call(page_size=100, page_token=None)]),
            (233, [call(page_size=100, page_token=None)]),
            (377, [call(page_size=100, page_token=None)]),
        ],
    )
    def test_limit_applied_when_there_are_fewer_workflows(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        limit: int,
        expected_requests: list,
    ):
        # Given
        mocked_client.list_workflow_runs.side_effect = [
            _client.Paginated(
                contents=[Mock() for _ in range(88)],
                next_page_token=f"<token sentinel {i}>",
            )
            for i in range(4)
        ]

        # When
        _ = runtime.list_workflow_runs(limit=limit)

        # Then
        mocked_client.list_workflow_runs.assert_has_calls(expected_requests)

    @pytest.mark.xfail(reason="Filtering not available in CE runtime yet")
    def test_filter_args_passed_to_client(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        # Given
        max_age = timedelta(hours=1)
        limit = None
        state = State.SUCCEEDED
        # When
        _ = runtime.list_workflow_runs(max_age=max_age, limit=limit, state=state)

        # Then
        mocked_client.list_workflow_runs.assert_called_once_with(
            max_age=max_age, limit=limit, state=state
        )

    def test_unknown_http(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        # Given
        mocked_client.list_workflow_runs.side_effect = _exceptions.UnknownHTTPError(
            MagicMock()
        )

        # When
        with pytest.raises(_exceptions.UnknownHTTPError):
            runtime.list_workflow_runs()

    @pytest.mark.parametrize(
        "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
    )
    def test_auth_failure(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        failure_exc: Exception,
    ):
        mocked_client.list_workflow_runs.side_effect = failure_exc

        # When
        with pytest.raises(exceptions.UnauthorizedError):
            runtime.list_workflow_runs()


class TestGetWorkflowLogs:
    @pytest.mark.xfail(
        reason="Until ORQSDK-777 is done, we're ignoring task invocation ids to avoid splitting up sdk and ray logs."  # noqa: E501
    )
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        wf_logs = [
            Mock(wf_run_id="<wf id sentinel 1>", message="<message sentinel 1>"),
            Mock(wf_run_id="<wf id sentinel 1>", message="<message sentinel 2>"),
            Mock(wf_run_id="<wf id sentinel 2>", message="<message sentinel 3>"),
        ]
        mocked_client.get_workflow_run_logs.return_value = wf_logs

        # When
        logs = runtime.get_workflow_logs(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_logs.assert_called_once_with(workflow_run_id)
        assert logs == {
            "<wf id sentinel 1>": ["<message sentinel 1>", "<message sentinel 2>"],
            "<wf id sentinel 2>": ["<message sentinel 3>"],
        }

    def test_ignore_task_inv_id(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        """
        TODO: This test covers a stopgap measure that will no longer be needed after
        ORQSDK-777. Remove it as soon as possible.
        """
        # Given
        wf_logs = [
            "<message sentinel 1>",
            "<message sentinel 2>",
            "<message sentinel 3>",
            "<message sentinel 4>",
        ]
        mocked_client.get_workflow_run_logs.return_value = wf_logs

        # When
        logs = runtime.get_workflow_logs(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_logs.assert_called_once_with(workflow_run_id)
        assert logs == {
            "UNKNOWN TASK INV ID": [
                "<message sentinel 1>",
                "<message sentinel 2>",
                "<message sentinel 3>",
                "<message sentinel 4>",
            ]
        }

    @pytest.mark.parametrize(
        "exception, expected_exception",
        [
            (_exceptions.InvalidWorkflowRunID, exceptions.WorkflowRunNotFoundError),
            (_exceptions.WorkflowRunNotFound, exceptions.WorkflowRunNotFoundError),
            (_exceptions.InvalidTokenError, exceptions.UnauthorizedError),
            (_exceptions.ForbiddenError, exceptions.UnauthorizedError),
            (_exceptions.UnknownHTTPError, _exceptions.UnknownHTTPError),
            (
                _exceptions.WorkflowRunLogsNotReadable,
                exceptions.InvalidWorkflowRunLogsError,
            ),
        ],
    )
    def test_exception_handling(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
        exception,
        expected_exception,
    ):
        # Given
        mocked_client.get_workflow_run_logs.side_effect = exception(MagicMock())

        # When
        with pytest.raises(expected_exception):
            runtime.get_workflow_logs(workflow_run_id)


class TestListWorkspaces:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        workspaces = [
            Mock(
                id="<id sentinel 1>",
                displayName="<displayName sentinel 1>",
                some_other_parameter="w/e",
            ),
            Mock(
                id="<id sentinel 2>",
                displayName="<displayName sentinel 2>",
                some_other_parameter="w/e",
            ),
        ]
        mocked_client.list_workspaces.return_value = workspaces

        workspace_defs = runtime.list_workspaces()

        assert len(workspace_defs) == 2
        assert workspace_defs == [
            Workspace(workspace_id="<id sentinel 1>", name="<displayName sentinel 1>"),
            Workspace(workspace_id="<id sentinel 2>", name="<displayName sentinel 2>"),
        ]

    @pytest.mark.parametrize(
        "exception, expected_exception",
        [
            (_exceptions.InvalidTokenError, exceptions.UnauthorizedError),
            (_exceptions.ForbiddenError, exceptions.UnauthorizedError),
        ],
    )
    def test_exception_handling(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        exception,
        expected_exception,
    ):
        # Given
        mocked_client.list_workspaces.side_effect = exception(MagicMock())

        # When
        with pytest.raises(expected_exception):
            runtime.list_workspaces()


class TestListProjects:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        projects = [
            Mock(
                id="<id sentinel 1>",
                displayName="<displayName sentinel 1>",
                resourceGroupId="<rgID1>",
                some_other_parameter="w/e",
            ),
            Mock(
                id="<id sentinel 2>",
                displayName="<displayName sentinel 2>",
                resourceGroupId="<rgID2>",
                some_other_parameter="w/e",
            ),
        ]
        mocked_client.list_projects.return_value = projects
        workspace_id = "id"

        workspace_defs = runtime.list_projects(workspace_id)

        assert len(workspace_defs) == 2
        assert workspace_defs == [
            Project(
                project_id="<id sentinel 1>",
                workspace_id="<rgID1>",
                name="<displayName sentinel 1>",
            ),
            Project(
                project_id="<id sentinel 2>",
                workspace_id="<rgID2>",
                name="<displayName sentinel 2>",
            ),
        ]

    @pytest.mark.parametrize(
        "exception, expected_exception",
        [
            (_exceptions.InvalidTokenError, exceptions.UnauthorizedError),
            (_exceptions.ForbiddenError, exceptions.UnauthorizedError),
            (_exceptions.InvalidWorkspaceZRI, exceptions.NotFoundError),
        ],
    )
    def test_exception_handling(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        exception,
        expected_exception,
    ):
        # Given
        mocked_client.list_projects.side_effect = exception(MagicMock())
        workspace_id = "id"

        # When
        with pytest.raises(expected_exception):
            runtime.list_projects(workspace_id)
