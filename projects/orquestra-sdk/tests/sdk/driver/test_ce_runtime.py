# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
from contextlib import nullcontext as do_not_raise
from datetime import timedelta
from typing import ContextManager, List, Optional
from unittest.mock import DEFAULT, MagicMock, Mock, call, create_autospec

import orquestra.workflow_shared._retry
import pytest
from orquestra.workflow_shared import exceptions, serde
from orquestra.workflow_shared._spaces._structs import Project, ProjectRef, Workspace
from orquestra.workflow_shared.logs import LogOutput
from orquestra.workflow_shared.schema.ir import WorkflowDef
from orquestra.workflow_shared.schema.responses import (
    ComputeEngineWorkflowResult,
    JSONResult,
)
from orquestra.workflow_shared.schema.workflow_run import (
    RunStatus,
    State,
    WorkflowRun,
    WorkflowRunId,
)

import orquestra.sdk as sdk
from orquestra.sdk._client._base import _traversal
from orquestra.sdk._client._base._driver import (
    _ce_runtime,
    _client,
    _exceptions,
    _models,
)
from orquestra.sdk._client._base._testing._example_wfs import (
    add,
    my_workflow,
    workflow_parametrised_with_resources,
    workflow_with_different_resources,
)


@pytest.fixture
def mocked_client(monkeypatch: pytest.MonkeyPatch):
    mocked_client = MagicMock(spec=_client.DriverClient)
    mocked_client.from_token.return_value = mocked_client
    monkeypatch.setattr(
        "orquestra.sdk._client._base._driver._client.DriverClient", mocked_client
    )
    return mocked_client


@pytest.fixture
def workflow_def_id():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def workflow_run_id():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def task_inv_id():
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
        monkeypatch: pytest.MonkeyPatch,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_def_id: str,
        workflow_run_id: str,
    ):
        # Given
        mocked_client.create_workflow_def.return_value = workflow_def_id
        mocked_client.create_workflow_run.return_value = workflow_run_id
        monkeypatch.setattr(_traversal, "_gen_id_hash", lambda *_: 0)

        # When
        wf_run_id = runtime.create_workflow_run(
            my_workflow.model,
            project=ProjectRef(workspace_id="a", project_id="b"),
            dry_run=False,
        )

        # Then
        mocked_client.create_workflow_def.assert_called_once_with(
            my_workflow.model, ProjectRef(workspace_id="a", project_id="b")
        )
        mocked_client.create_workflow_run.assert_called_once_with(
            workflow_def_id,
            _models.Resources(cpu=None, memory=None, gpu=None, nodes=None),
            False,
            None,
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
                workflow_parametrised_with_resources(memory="10Gi").model,
                ProjectRef(workspace_id="a", project_id="b"),
                dry_run=False,
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu=None, memory="10Gi", gpu=None, nodes=None),
                False,
                None,
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
                workflow_parametrised_with_resources(cpu="1000m").model,
                ProjectRef(workspace_id="a", project_id="b"),
                dry_run=False,
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu="1000m", memory=None, gpu=None, nodes=None),
                False,
                None,
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
                workflow_parametrised_with_resources(gpu="1").model,
                ProjectRef(workspace_id="a", project_id="b"),
                dry_run=False,
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu=None, memory=None, gpu="1", nodes=None),
                False,
                None,
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
                workflow_with_different_resources().model,
                ProjectRef(workspace_id="a", project_id="b"),
                dry_run=False,
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu="5000m", memory="3G", gpu="1", nodes=None),
                False,
                None,
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
                ProjectRef(workspace_id="a", project_id="b"),
                dry_run=False,
            )

            # Then
            mocked_client.create_workflow_run.assert_called_once_with(
                workflow_def_id,
                _models.Resources(cpu="1", memory="1.5G", gpu="1", nodes=20),
                False,
                None,
            )

    @pytest.mark.parametrize(
        "head_node_resources",
        (
            sdk.Resources(cpu=None, memory=None),
            None,
        ),
    )
    def test_with_no_head_node_resources(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_def_id: str,
        workflow_run_id: str,
        head_node_resources: sdk.Resources,
    ):
        # Given
        @sdk.workflow(
            head_node_resources=head_node_resources,
        )
        def wf():
            return add(1, 2)

        mocked_client.create_workflow_def.return_value = workflow_def_id
        mocked_client.create_workflow_run.return_value = workflow_run_id

        # When
        _ = runtime.create_workflow_run(
            wf().model,
            ProjectRef(workspace_id="a", project_id="b"),
            dry_run=False,
        )

        # Then
        mocked_client.create_workflow_run.assert_called_once_with(
            workflow_def_id,
            _models.Resources(cpu=None, memory=None, nodes=None, gpu=None),
            False,
            None,
        )

    @pytest.mark.parametrize(
        "cpu, memory,",
        (
            ("1", None),
            (None, "10Gi"),
            ("2", "20Gi"),
        ),
    )
    def test_with_head_node_resources(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_def_id: str,
        workflow_run_id: str,
        cpu: str,
        memory: str,
    ):
        # Given
        @sdk.workflow(head_node_resources=sdk.Resources(cpu=cpu, memory=memory))
        def wf():
            return add(1, 2)

        mocked_client.create_workflow_def.return_value = workflow_def_id
        mocked_client.create_workflow_run.return_value = workflow_run_id

        # When
        _ = runtime.create_workflow_run(
            wf().model,
            ProjectRef(workspace_id="a", project_id="b"),
            dry_run=False,
        )

        # Then
        mocked_client.create_workflow_run.assert_called_once_with(
            workflow_def_id,
            _models.Resources(cpu=None, memory=None, nodes=None, gpu=None),
            False,
            _models.HeadNodeResources(cpu=cpu, memory=memory),
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
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    dry_run=False,
                )

        def test_unknown_http(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    dry_run=False,
                )

        def test_invalid_project(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
        ):
            # Given
            mocked_client.create_workflow_def.side_effect = _exceptions.ForbiddenError

            # When
            with pytest.raises(exceptions.ProjectInvalidError):
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    False,
                )

        def test_no_workspace(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
        ):
            # Given

            # When
            with pytest.raises(ValueError):
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    None,
                    False,
                )

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
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    dry_run=False,
                )

        @pytest.mark.parametrize("submitted_version", (None, "0.1.0"))
        @pytest.mark.parametrize(
            "supported_versions", (None, ["0.1.0"], ["0.2.0", "0.3.0"])
        )
        def test_unsupported_sdk_version(
            self,
            mocked_client: MagicMock,
            runtime: _ce_runtime.CERuntime,
            submitted_version: Optional[str],
            supported_versions: Optional[List[str]],
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = (
                _exceptions.UnsupportedSDKVersion(submitted_version, supported_versions)
            )

            # When
            with pytest.raises(exceptions.WorkflowRunNotStarted) as exc_info:
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    dry_run=False,
                )

            error_message = str(exc_info.value)
            assert "This is an unsupported version of orquestra-sdk.\n" in error_message
            assert (
                'Try updating with `pip install -U "orquestra-sdk[all]"`'
                in error_message
            )

            if submitted_version is None:
                assert " - Current version:" not in error_message
            else:
                assert f" - Current version: {submitted_version}" in error_message

            if supported_versions is None:
                assert " - Supported versions:" not in error_message
            else:
                assert f" - Supported versions: {supported_versions}" in error_message

        def test_unknown_http(
            self, mocked_client: MagicMock, runtime: _ce_runtime.CERuntime
        ):
            # Given
            mocked_client.create_workflow_run.side_effect = (
                _exceptions.UnknownHTTPError(MagicMock())
            )

            # When
            with pytest.raises(_exceptions.UnknownHTTPError):
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    dry_run=False,
                )

        def test_auth_failure(
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
                _ = runtime.create_workflow_run(
                    my_workflow.model,
                    ProjectRef(workspace_id="a", project_id="b"),
                    dry_run=False,
                )


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
            ComputeEngineWorkflowResult(results=(JSONResult(value="[1]"),))
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
                results=(
                    JSONResult(value="1"),
                    JSONResult(value="2"),
                ),
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
            monkeypatch.setattr(
                orquestra.workflow_shared._retry.time,  # type:ignore[reportPrivateImportUsage] # noqa: E501
                "sleep",
                Mock(),
            )
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
            monkeypatch.setattr(serde, "deserialize", lambda x: x)

            monkeypatch.setattr(
                orquestra.workflow_shared._retry.time,  # type:ignore[reportPrivateImportUsage] # noqa: E501
                "sleep",
                Mock(),
            )
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
                f"{workflow_run_id}@task-inv-3": [],
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
        mocked_client.terminate_workflow_run.assert_called_once_with(
            workflow_run_id, None
        )

    @pytest.mark.parametrize(
        "force",
        (True, False),
    )
    def test_with_force(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
        force: bool,
    ):
        # Given
        mocked_client.terminate_workflow_run.return_value = None
        # When
        runtime.stop_workflow_run(workflow_run_id, force=force)
        # Then
        mocked_client.terminate_workflow_run.assert_called_once_with(
            workflow_run_id, force
        )

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
            page_size=None,
            page_token=None,
            workspace=None,
            max_age=None,
            state=None,
        )
        assert runs == wf_runs

    @pytest.mark.parametrize(
        "limit, expected_requests",
        [
            (
                89,
                [
                    call(
                        page_size=89,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                144,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=44,
                        page_token="<token sentinel 0>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                ],
            ),
            (
                233,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=100,
                        page_token="<token sentinel 0>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=33,
                        page_token="<token sentinel 1>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                ],
            ),
            (
                377,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=100,
                        page_token="<token sentinel 0>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=100,
                        page_token="<token sentinel 1>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=77,
                        page_token="<token sentinel 2>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
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
            (
                89,
                [
                    call(
                        page_size=89,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                144,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                233,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                377,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
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
            page_size=None,
            page_token=None,
            workspace=None,
            max_age=max_age,
            state=state,
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


class TestListWorkflowRunSummaries:
    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        # Given
        wf_runs = [Mock(), Mock()]
        mocked_client.list_workflow_run_summaries.return_value = _client.Paginated(
            contents=wf_runs
        )

        # When
        runs = runtime.list_workflow_run_summaries()

        # Then
        mocked_client.list_workflow_run_summaries.assert_called_once_with(
            page_size=None, page_token=None, workspace=None, max_age=None, state=None
        )
        assert runs == wf_runs

    @pytest.mark.parametrize(
        "limit, expected_requests",
        [
            (
                89,
                [
                    call(
                        page_size=89,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                144,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=44,
                        page_token="<token sentinel 0>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                ],
            ),
            (
                233,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=100,
                        page_token="<token sentinel 0>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=33,
                        page_token="<token sentinel 1>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                ],
            ),
            (
                377,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=100,
                        page_token="<token sentinel 0>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=100,
                        page_token="<token sentinel 1>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
                    call(
                        page_size=77,
                        page_token="<token sentinel 2>",
                        workspace=None,
                        max_age=None,
                        state=None,
                    ),
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
        mocked_client.list_workflow_run_summaries.side_effect = [
            _client.Paginated(
                contents=[Mock() for _ in range(100)],
                next_page_token=f"<token sentinel {i}>",
            )
            for i in range(4)
        ]

        # When
        _ = runtime.list_workflow_run_summaries(limit=limit)

        # Then
        mocked_client.list_workflow_run_summaries.assert_has_calls(expected_requests)

    @pytest.mark.parametrize(
        "limit, expected_requests",
        [
            (
                89,
                [
                    call(
                        page_size=89,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                144,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                233,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
            (
                377,
                [
                    call(
                        page_size=100,
                        page_token=None,
                        workspace=None,
                        max_age=None,
                        state=None,
                    )
                ],
            ),
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
        mocked_client.list_workflow_run_summaries.side_effect = [
            _client.Paginated(
                contents=[Mock() for _ in range(88)],
                next_page_token=f"<token sentinel {i}>",
            )
            for i in range(4)
        ]

        # When
        _ = runtime.list_workflow_run_summaries(limit=limit)

        # Then
        mocked_client.list_workflow_run_summaries.assert_has_calls(expected_requests)

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
        _ = runtime.list_workflow_run_summaries(
            max_age=max_age, limit=limit, state=state
        )

        # Then
        mocked_client.list_workflow_run_summaries.assert_called_once_with(
            page_size=None,
            page_token=None,
            workspace=None,
            max_age=max_age,
            state=state,
        )

    def test_unknown_http(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
    ):
        # Given
        mocked_client.list_workflow_run_summaries.side_effect = (
            _exceptions.UnknownHTTPError(MagicMock())
        )

        # When
        with pytest.raises(_exceptions.UnknownHTTPError):
            runtime.list_workflow_run_summaries()

    @pytest.mark.parametrize(
        "failure_exc", [_exceptions.InvalidTokenError, _exceptions.ForbiddenError]
    )
    def test_auth_failure(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        failure_exc: Exception,
    ):
        mocked_client.list_workflow_run_summaries.side_effect = failure_exc

        # When
        with pytest.raises(exceptions.UnauthorizedError):
            runtime.list_workflow_run_summaries()


class TestGetWorkflowLogs:
    @pytest.fixture
    def tag(self):
        return "shouldnt-matter"

    @pytest.fixture
    def ray_logs(self, tag: str):
        return [
            _models.WorkflowLogMessage(
                log="line 1",
                ray_filename=_models.RayFilename(
                    "/tmp/ray/session_latest/logs/worker-b4584f711ed56477c7e7c0ea4b16"
                    "717e36c35dd13ae66b6430a3e5a8-01000000-178.err"
                ),
                tag=tag,
            ),
            _models.WorkflowLogMessage(
                log="line 2",
                ray_filename=_models.RayFilename(
                    "/tmp/ray/session_latest/logs/worker-b4584f711ed56477c7e7c0ea4b16"
                    "717e36c35dd13ae66b6430a3e5a8-01000000-178.out"
                ),
                tag=tag,
            ),
            _models.WorkflowLogMessage(
                log="line 3",
                ray_filename=_models.RayFilename(
                    "/tmp/ray/session_latest/logs/something_else.log"
                ),
                tag=tag,
            ),
            _models.WorkflowLogMessage(
                log="line 4",
                ray_filename=_models.RayFilename(
                    "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
                ),
                tag=tag,
            ),
        ]

    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        tag: str,
        ray_logs: List[_models.WorkflowLogMessage],
        workflow_run_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Given
        sys_logs = [
            _models.K8sEventLog(tag=tag, log={"some": "values", "another": "thing"}),
            _models.RayHeadNodeEventLog(tag=tag, log="Ray head log line"),
            _models.RayWorkerNodeEventLog(tag=tag, log="Ray worker log line"),
            _models.UnknownEventLog(tag=tag, log="Unknown log line"),
        ]
        wf_run = Mock()
        wf_run.workflow_def.task_invocations.keys.return_value = ["inv1", "inv2"]
        mocked_client.get_workflow_run.return_value = wf_run
        mocked_client.get_workflow_run_logs.return_value = ray_logs
        mocked_client.get_system_logs.return_value = sys_logs

        get_task_logs = Mock(return_value=LogOutput(out=["mocked"], err=["mocked err"]))
        monkeypatch.setattr(runtime, "get_task_logs", get_task_logs)

        # When
        logs = runtime.get_workflow_logs(workflow_run_id)

        # Then
        mocked_client.get_system_logs.assert_called_once_with(workflow_run_id)
        mocked_client.get_workflow_run_logs.assert_called_once_with(workflow_run_id)
        get_task_logs.assert_has_calls(
            [call(workflow_run_id, "inv1"), call(workflow_run_id, "inv2")]
        )
        assert logs.per_task == {
            "inv1": LogOutput(out=["mocked"], err=["mocked err"]),
            "inv2": LogOutput(out=["mocked"], err=["mocked err"]),
        }

        assert logs.env_setup == LogOutput(out=["line 4"], err=[])

        assert logs.system == LogOutput(
            out=[
                "{'some': 'values', 'another': 'thing'}",
                "Ray head log line",
                "Ray worker log line",
                "Unknown log line",
            ],
            err=[],
        )

        assert logs.other == LogOutput(out=["line 2", "line 3"], err=["line 1"])

    def test_no_task_logs(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        tag: str,
        ray_logs: List[_models.WorkflowLogMessage],
        workflow_run_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Given
        sys_logs = [
            _models.K8sEventLog(tag=tag, log={"some": "values", "another": "thing"}),
            _models.RayHeadNodeEventLog(tag=tag, log="Ray head log line"),
            _models.RayWorkerNodeEventLog(tag=tag, log="Ray worker log line"),
            _models.UnknownEventLog(tag=tag, log="Unknown log line"),
        ]
        wf_run = Mock()
        wf_run.workflow_def.task_invocations.keys.return_value = ["inv1", "inv2"]
        mocked_client.get_workflow_run.return_value = wf_run
        mocked_client.get_workflow_run_logs.return_value = ray_logs
        mocked_client.get_system_logs.return_value = sys_logs

        # Mocking not finding any task logs at all
        get_task_logs = Mock(
            side_effect=exceptions.TaskRunLogsNotFound(workflow_run_id, Mock())
        )
        monkeypatch.setattr(runtime, "get_task_logs", get_task_logs)

        # When
        logs = runtime.get_workflow_logs(workflow_run_id)

        # Then
        mocked_client.get_system_logs.assert_called_once_with(workflow_run_id)
        mocked_client.get_workflow_run_logs.assert_called_once_with(workflow_run_id)

        assert logs.per_task == {}

        assert logs.env_setup == LogOutput(out=["line 4"], err=[])

        assert logs.system == LogOutput(
            out=[
                "{'some': 'values', 'another': 'thing'}",
                "Ray head log line",
                "Ray worker log line",
                "Unknown log line",
            ],
            err=[],
        )

        assert logs.other == LogOutput(out=["line 2", "line 3"], err=["line 1"])

    def test_no_logs_exception(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        tag: str,
        ray_logs: List[_models.WorkflowLogMessage],
        workflow_run_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Given
        wf_run = Mock()
        wf_run.workflow_def.task_invocations.keys.return_value = ["inv1", "inv2"]
        mocked_client.get_workflow_run.return_value = wf_run

        mocked_client.get_workflow_run_logs.side_effect = (
            _exceptions.WorkflowRunLogsNotFound("abc")
        )

        # When
        logs = runtime.get_workflow_logs(workflow_run_id)

        # Then
        mocked_client.get_workflow_run_logs.assert_called_once_with(workflow_run_id)

        assert logs.env_setup == LogOutput(out=[], err=[])

        assert logs.system == LogOutput(out=[], err=[])

        assert logs.other == LogOutput(out=[], err=[])

    @pytest.mark.parametrize(
        "exception, expected_exception, exception_args",
        [
            (
                _exceptions.InvalidWorkflowRunID,
                exceptions.WorkflowRunNotFoundError,
                (Mock(),),
            ),
            (
                _exceptions.WorkflowRunNotFound,
                exceptions.WorkflowRunNotFoundError,
                (Mock(),),
            ),
            (_exceptions.InvalidTokenError, exceptions.UnauthorizedError, tuple()),
            (_exceptions.ForbiddenError, exceptions.UnauthorizedError, tuple()),
            (_exceptions.UnknownHTTPError, _exceptions.UnknownHTTPError, (Mock(),)),
            (
                _exceptions.WorkflowRunLogsNotReadable,
                exceptions.InvalidWorkflowRunLogsError,
                (Mock(), Mock()),
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
        exception_args,
    ):
        # Given
        mocked_client.get_workflow_run_logs.side_effect = exception(*exception_args)

        # When
        with pytest.raises(expected_exception):
            runtime.get_workflow_logs(workflow_run_id)


class TestGetTaskLogs:
    @pytest.fixture
    def tag(self):
        return "shouldnt-matter"

    @pytest.fixture
    def logs(self, tag: str):
        return [
            _models.TaskLogMessage(
                log="line 1",
                log_filename=_models.LogFilename(
                    "/var/task_run_logs/wf/wf-run-id/task/task-inv-id.out"
                ),
                tag=tag,
            ),
            _models.TaskLogMessage(
                log="line 2",
                log_filename=_models.LogFilename(
                    "/var/task_run_logs/wf/wf-run-id/task/task-inv-id.err"
                ),
                tag=tag,
            ),
        ]

    def test_happy_path(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        logs: List[_models.TaskLogMessage],
        workflow_run_id: str,
        task_inv_id: str,
    ):
        # Given
        mocked_client.get_task_run_logs.return_value = logs
        # When
        task_logs = runtime.get_task_logs(workflow_run_id, task_inv_id)

        # Then
        assert task_logs == LogOutput(out=["line 1"], err=["line 2"])

    @pytest.mark.parametrize(
        "exception, expected_exception, exception_args",
        [
            (
                _exceptions.InvalidWorkflowRunID,
                exceptions.TaskRunLogsNotFound,
                (Mock(),),
            ),
            (
                _exceptions.TaskRunLogsNotFound,
                exceptions.TaskRunLogsNotFound,
                (Mock(), Mock()),
            ),
            (_exceptions.InvalidTokenError, exceptions.UnauthorizedError, tuple()),
            (_exceptions.ForbiddenError, exceptions.UnauthorizedError, tuple()),
            (_exceptions.UnknownHTTPError, _exceptions.UnknownHTTPError, (Mock(),)),
            (
                _exceptions.WorkflowRunLogsNotReadable,
                exceptions.InvalidWorkflowRunLogsError,
                (Mock(), Mock()),
            ),
        ],
    )
    def test_exception_handling(
        self,
        mocked_client: MagicMock,
        runtime: _ce_runtime.CERuntime,
        workflow_run_id: str,
        task_inv_id: str,
        exception,
        expected_exception,
        exception_args,
    ):
        # Given
        mocked_client.get_task_run_logs.side_effect = exception(*exception_args)

        # When
        with pytest.raises(expected_exception):
            runtime.get_task_logs(workflow_run_id, task_inv_id)


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


@pytest.mark.parametrize(
    "wf_resources, task_resources, expected_resources, raises, telltales",
    [
        (
            sdk.Resources(cpu="2000m"),
            sdk.Resources(cpu="1000m"),
            _models.Resources(nodes=None, cpu="2000m", memory=None, gpu=None),
            False,
            (),
        ),
        (
            sdk.Resources(cpu="2", gpu="1", memory="3000m"),
            sdk.Resources(cpu="1000m"),
            _models.Resources(nodes=None, cpu="2", gpu="1", memory="3000m"),
            False,
            (),
        ),
        (
            sdk.Resources(cpu="2", gpu="1", memory="3000m"),
            sdk.Resources(cpu="2", gpu="1", memory="3000m"),
            _models.Resources(nodes=None, cpu="2", gpu="1", memory="3000m"),
            False,
            (),
        ),
        (
            sdk.Resources(gpu="1"),
            sdk.Resources(cpu="2", memory="2Gi"),  # enough resources for defaults
            _models.Resources(nodes=None, cpu=None, gpu="1", memory=None),
            False,
            (),
        ),
        (
            sdk.Resources(cpu="1000m"),
            sdk.Resources(cpu="2000m"),
            None,
            True,
            ("CPU",),
        ),
        (
            sdk.Resources(cpu="1000m", memory="5000m", gpu="10"),
            sdk.Resources(cpu="2000m"),
            None,
            True,
            ("CPU",),
        ),
        (
            sdk.Resources(nodes=3, cpu="3"),
            sdk.Resources(cpu="3", memory="3Gi", gpu="10"),
            None,
            True,
            ("Memory", "GPU"),
        ),
        (
            sdk.Resources(nodes=2, cpu="3"),
            sdk.Resources(cpu="3", memory="2Gi", gpu="10"),
            None,
            True,
            ("GPU",),
        ),
        (
            sdk.Resources(nodes=2, cpu="3"),
            sdk.Resources(memory="3Gi"),
            None,
            True,
            ("Memory",),
        ),
    ],
)
def test_ce_resources(
    mocked_client: MagicMock,
    runtime: _ce_runtime.CERuntime,
    workflow_def_id: str,
    workflow_run_id: str,
    wf_resources,
    task_resources,
    expected_resources,
    raises,
    telltales,
):
    # given
    @sdk.task(resources=task_resources)
    def task():
        return 21

    @sdk.workflow(resources=wf_resources)
    def wf():
        return task()

    mocked_client.create_workflow_def.return_value = workflow_def_id
    mocked_client.create_workflow_run.return_value = workflow_run_id

    context: ContextManager
    if raises:
        context = pytest.raises(exceptions.WorkflowSyntaxError)
    else:
        context = do_not_raise()

    # When
    with context as exec_info:
        runtime.create_workflow_run(
            wf().model, ProjectRef(workspace_id="a", project_id="b"), dry_run=False
        )

    # Then
    if raises:
        assert all([telltale in str(exec_info) for telltale in telltales])
    else:
        mocked_client.create_workflow_run.assert_called_once_with(
            workflow_def_id,
            expected_resources,
            False,
            None,
        )


def test_invalid_token(
    mocked_client: MagicMock,
    runtime: _ce_runtime.CERuntime,
    workflow_def_id: str,
    workflow_run_id: str,
):
    runtime._token = "fake token"
    funcs = [
        (runtime.create_workflow_run, 3),
        (runtime.get_workflow_run_status, 1),
        (runtime.get_workflow_run_outputs_non_blocking, 1),
        (runtime.get_available_outputs, 1),
        (runtime.stop_workflow_run, 1),
        (runtime.list_workflow_run_summaries, 0),
        (runtime.list_workflow_runs, 0),
        (runtime.get_workflow_logs, 1),
        (runtime.get_task_logs, 2),
        (runtime.list_workspaces, 0),
        (runtime.list_projects, 1),
        (runtime.get_workflow_project, 1),
    ]

    for fun_call, no_of_params in funcs:
        with pytest.raises(exceptions.UnauthorizedError):
            fun_call(*[Mock()] * no_of_params)
