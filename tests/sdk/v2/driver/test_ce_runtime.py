# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, call

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base._driver import _ce_runtime, _client, _exceptions, _models
from orquestra.sdk._base._testing._example_wfs import my_workflow
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.responses import JSONResult
from orquestra.sdk.schema.workflow_run import WorkflowRunId


@pytest.fixture
def runtime(tmp_path, mock_workflow_db_location):
    # Fake CE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.QE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return _ce_runtime.CERuntime(config)


@pytest.fixture
def runtime_verbose(tmp_path):
    (tmp_path / ".orquestra").mkdir(exist_ok=True)
    # Fake QE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.QE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return _ce_runtime.CERuntime(config, True)


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
        assert rt._config == rt2._config
        assert rt._verbose == rt2._verbose

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
        wf_run_id = runtime.create_workflow_run(my_workflow.model)

        # Then
        mocked_client.create_workflow_def.assert_called_once_with(my_workflow.model)
        mocked_client.create_workflow_run.assert_called_once_with(
            workflow_def_id,
            _models.RuntimeType.SINGLE_NODE_RAY_RUNTIME,
        )
        assert isinstance(wf_run_id, WorkflowRunId)
        assert (
            wf_run_id == workflow_run_id
        ), "Workflow run ID is returned directly from the client"

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
                _ = runtime.create_workflow_run(my_workflow.model)

        def test_unknown_http(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.create_workflow_run(my_workflow.model)

        def test_token_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = (
                _exceptions.InvalidTokenError
            )

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.create_workflow_run(my_workflow.model)

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
                _ = runtime.create_workflow_run(my_workflow.model)

        def test_unknown_http(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.create_workflow_run(my_workflow.model)

        def test_token_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = (
                _exceptions.InvalidTokenError
            )

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.create_workflow_run(my_workflow.model)


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

    def test_token_failure(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run.side_effect = _exceptions.InvalidTokenError

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
        mocked_client.get_workflow_run_results.return_value = ["1", "2"]
        mocked_client.get_workflow_run_result.return_value = JSONResult(value="1")

        # When
        results = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_results.assert_called_once_with(workflow_run_id)
        mocked_client.get_workflow_run_result.assert_has_calls([call("1"), call("2")])
        assert len(results) == 2
        assert results[0] == 1
        assert results[1] == 1

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

        def test_no_results(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_results.return_value = []
            # When
            with pytest.raises(exceptions.WorkflowRunNotSucceeded):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

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

        def test_token_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_results.side_effect = (
                _exceptions.InvalidTokenError
            )

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

    class TestGetworkflowRunResultFailure:
        @pytest.fixture
        def mocked_client(self, mocked_client: MagicMock):
            mocked_client.get_workflow_run_results.return_value = ["1", "2"]
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

        def test_token_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_result.side_effect = (
                _exceptions.InvalidTokenError
            )

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
            "task-1": ["1", "2"],
            "task-2": ["3"],
        }
        mocked_client.get_workflow_run_artifact.return_value = JSONResult(value="1")

        # When
        results = runtime.get_available_outputs(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_artifacts.assert_called_once_with(
            workflow_run_id
        )
        mocked_client.get_workflow_run_artifact.assert_has_calls(
            [call("1"), call("2"), call("3")]
        )
        assert len(results) == 2
        assert len(results["task-1"]) == 2
        assert len(results["task-2"]) == 1

    def test_map_remote_to_local_ids(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.get_workflow_run_artifacts.return_value = {
            f"{workflow_run_id}@task-1": ["1", "2"],
            f"{workflow_run_id}@task-2": ["3"],
        }
        mocked_client.get_workflow_run_artifact.return_value = JSONResult(value="1")

        # When
        results = runtime.get_available_outputs(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_artifacts.assert_called_once_with(
            workflow_run_id
        )
        mocked_client.get_workflow_run_artifact.assert_has_calls(
            [call("1"), call("2"), call("3")]
        )
        assert len(results) == 2
        assert len(results["task-1"]) == 2
        assert len(results["task-2"]) == 1

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

        def test_token_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            mocked_client.get_workflow_run_artifacts.side_effect = (
                _exceptions.InvalidTokenError
            )

            # When
            with pytest.raises(exceptions.UnauthorizedError):
                _ = runtime.get_available_outputs(workflow_run_id)

    class TestGetWorkflowRunArtifactFailure:
        @pytest.fixture
        def mocked_client(self, mocked_client: MagicMock):
            mocked_client.get_workflow_run_artifacts.return_value = {
                "task-1": ["1", "2"],
                "task-2": ["3"],
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
                [call("1"), call("2"), call("3")]
            )
            assert len(results) == 2
            assert len(results["task-1"]) == 0
            assert len(results["task-2"]) == 0

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
                [call("1"), call("2"), call("3")]
            )
            assert len(results) == 2
            assert len(results["task-1"]) == 2
            assert len(results["task-2"]) == 0

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
                DEFAULT,
            )

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("1"), call("2"), call("3")]
            )
            assert len(results) == 2
            assert len(results["task-1"]) == 1
            assert len(results["task-2"]) == 1

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
                [call("1"), call("2"), call("3")]
            )
            assert len(results) == 2
            assert len(results["task-1"]) == 0
            assert len(results["task-2"]) == 0

        def test_token_failure(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            workflow_run_id: str,
        ):
            # Given
            mocked_client.get_workflow_run_artifact.side_effect = (
                _exceptions.InvalidTokenError
            )

            # When
            results = runtime.get_available_outputs(workflow_run_id)

            # Then
            mocked_client.get_workflow_run_artifacts.assert_called_once_with(
                workflow_run_id
            )
            mocked_client.get_workflow_run_artifact.assert_has_calls(
                [call("1"), call("2"), call("3")]
            )
            assert len(results) == 2
            assert len(results["task-1"]) == 0
            assert len(results["task-2"]) == 0


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

    def test_token_failure(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
    ):
        mocked_client.terminate_workflow_run.side_effect = _exceptions.InvalidTokenError

        # When
        with pytest.raises(exceptions.UnauthorizedError):
            runtime.stop_workflow_run(workflow_run_id)
