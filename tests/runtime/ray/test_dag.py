################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Unit tests for orquestra.sdk._ray._dag. If you need a test against a live
Ray connection, see tests/ray/test_integration.py instead.
"""
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, PropertyMock, create_autospec

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base import _dates
from orquestra.sdk._base._config import (
    LOCAL_RUNTIME_CONFIGURATION,
    RuntimeConfiguration,
    RuntimeName,
)
from orquestra.sdk._base._db import WorkflowDB
from orquestra.sdk._base._spaces._structs import ProjectRef
from orquestra.sdk._ray import _client, _dag, _ray_logs
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import State

TEST_TIME = _dates.now()


@pytest.fixture
def wf_run_id():
    return "mocked_wf_run_id"


@pytest.fixture
def client():
    return create_autospec(_dag.RayClient)


@pytest.mark.parametrize(
    "ray_wf_status, start_time, end_time, expected_orq_status",
    [
        (_client.WorkflowStatus.SUCCESSFUL, None, None, State.SUCCEEDED),
        (_client.WorkflowStatus.SUCCESSFUL, TEST_TIME, None, State.SUCCEEDED),
        (_client.WorkflowStatus.SUCCESSFUL, TEST_TIME, TEST_TIME, State.SUCCEEDED),
        (_client.WorkflowStatus.RUNNING, None, None, State.WAITING),
        (_client.WorkflowStatus.RUNNING, TEST_TIME, None, State.RUNNING),
        (_client.WorkflowStatus.RUNNING, TEST_TIME, TEST_TIME, State.SUCCEEDED),
        (_client.WorkflowStatus.FAILED, None, None, State.FAILED),
        (_client.WorkflowStatus.FAILED, TEST_TIME, None, State.FAILED),
        (_client.WorkflowStatus.FAILED, TEST_TIME, TEST_TIME, State.FAILED),
        (_client.WorkflowStatus.RESUMABLE, None, None, State.FAILED),
        (_client.WorkflowStatus.RESUMABLE, TEST_TIME, None, State.FAILED),
        (_client.WorkflowStatus.RESUMABLE, TEST_TIME, TEST_TIME, State.FAILED),
        (_client.WorkflowStatus.CANCELED, None, None, State.TERMINATED),
        (_client.WorkflowStatus.CANCELED, TEST_TIME, None, State.TERMINATED),
        (_client.WorkflowStatus.CANCELED, TEST_TIME, TEST_TIME, State.TERMINATED),
    ],
)
def test_workflow_state_from_ray_meta(
    ray_wf_status,
    start_time,
    end_time,
    expected_orq_status,
):
    assert (
        _dag._workflow_state_from_ray_meta(
            ray_wf_status,
            start_time,
            end_time,
        )
        == expected_orq_status
    )


@pytest.mark.parametrize(
    "ray_wf_status, start_time, end_time, expected_orq_status",
    [
        (_client.WorkflowStatus.SUCCESSFUL, None, None, State.WAITING),
        (_client.WorkflowStatus.SUCCESSFUL, TEST_TIME, None, State.SUCCEEDED),
        (_client.WorkflowStatus.SUCCESSFUL, TEST_TIME, TEST_TIME, State.SUCCEEDED),
        (_client.WorkflowStatus.RUNNING, None, None, State.WAITING),
        (_client.WorkflowStatus.RUNNING, TEST_TIME, None, State.RUNNING),
        (_client.WorkflowStatus.RUNNING, TEST_TIME, TEST_TIME, State.SUCCEEDED),
        # A task that didn't have a chance to run yet, because other tasks failed.
        (_client.WorkflowStatus.FAILED, None, None, State.WAITING),
        # The task that failed the workflow.
        (_client.WorkflowStatus.FAILED, TEST_TIME, None, State.FAILED),
        # A task that finished before other tasks failed.
        (_client.WorkflowStatus.FAILED, TEST_TIME, TEST_TIME, State.SUCCEEDED),
        # Resumable: something's happened to the workflow. Ray allows to
        # restart it. We don't support that yet -> we say it's failed.
        # We don't have API for cancelling a wf, either. If a wf is in this
        # state, something bad happened.
        (_client.WorkflowStatus.RESUMABLE, None, None, State.FAILED),
        (_client.WorkflowStatus.RESUMABLE, TEST_TIME, None, State.FAILED),
        (_client.WorkflowStatus.RESUMABLE, TEST_TIME, TEST_TIME, State.FAILED),
        # Cancelled: a user stopped the workflow permanently.
        (_client.WorkflowStatus.CANCELED, None, None, State.TERMINATED),
        (_client.WorkflowStatus.CANCELED, TEST_TIME, None, State.TERMINATED),
        (_client.WorkflowStatus.CANCELED, TEST_TIME, TEST_TIME, State.TERMINATED),
    ],
)
def test_task_state_from_ray_meta(
    ray_wf_status,
    start_time,
    end_time,
    expected_orq_status,
):
    assert (
        _dag._task_state_from_ray_meta(
            ray_wf_status,
            start_time,
            end_time,
        )
        == expected_orq_status
    )


class TestRayRuntime:
    """
    Unit tests for RayRuntime class. Shouldn't use a real Ray connection nor other
    background services.
    """

    @staticmethod
    @pytest.fixture
    def runtime_config(monkeypatch):
        return LOCAL_RUNTIME_CONFIGURATION

    class TestReadingLogs:
        """
        Verifies that RayRuntime gets whatever DirectRayReader produced.

        Test boundary: [RayRuntime]─┬[ServiceManager]
                                    └[DirectRayReader]
        """

        class TestGetWorkflowLogs:
            @staticmethod
            def test_direct_ray(
                monkeypatch,
                tmp_path: Path,
                runtime_config: RuntimeConfiguration,
            ):
                """
                Makes a spare ``RayRuntime`` object, mocks its attributes, and verifies
                passing data between the reader and ``RayRuntime``.
                """
                # Given
                rt = _dag.RayRuntime(
                    client=Mock(),
                    config=runtime_config,
                    project_dir=tmp_path,
                )

                logs_dict = {"inv_id1": ["Hello, there!", "General Kenobi!"]}
                get_workflow_logs = Mock(return_value=logs_dict)
                monkeypatch.setattr(
                    _ray_logs.DirectRayReader, "get_workflow_logs", get_workflow_logs
                )

                wf_run_id = "wf.1"

                # When
                result_dict = rt.get_workflow_logs(wf_run_id=wf_run_id)

                # Then
                assert result_dict == logs_dict
                get_workflow_logs.assert_called_with(wf_run_id)

        class TestGetTaskLogs:
            @staticmethod
            def test_direct_ray(
                monkeypatch,
                tmp_path: Path,
                runtime_config: RuntimeConfiguration,
            ):
                """
                Makes a spare ``RayRuntime`` object, mocks its attributes, and verifies
                passing data between the reader and ``RayRuntime``.
                """
                # Given
                rt = _dag.RayRuntime(
                    client=Mock(),
                    config=runtime_config,
                    project_dir=tmp_path,
                )

                logs_list = ["hello", "there!"]
                get_task_logs = Mock(return_value=logs_list)
                monkeypatch.setattr(
                    _ray_logs.DirectRayReader, "get_task_logs", get_task_logs
                )

                wf_run_id = "wf.1"
                task_inv_id = "inv-2"

                # When
                result_list = rt.get_task_logs(
                    wf_run_id=wf_run_id, task_inv_id=task_inv_id
                )

                # Then
                assert result_list == logs_list
                get_task_logs.assert_called_with(wf_run_id, task_inv_id)

    class TestCreateWorkflowRun:
        def test_project_raises_warning(
            self, client, runtime_config, tmp_path, monkeypatch
        ):
            monkeypatch.setattr(_dag, "make_ray_dag", Mock())
            monkeypatch.setattr(_dag, "WfUserMetadata", Mock())
            monkeypatch.setattr(_dag, "pydatic_to_json_dict", Mock())
            monkeypatch.setattr(StoredWorkflowRun, "__init__", lambda *_, **__: None)
            monkeypatch.setattr(WorkflowDB, "save_workflow_run", Mock())

            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            with pytest.warns(expected_warning=exceptions.UnsupportedRuntimeFeature):
                runtime.create_workflow_run(
                    Mock(), project=ProjectRef(workspace_id="", project_id="")
                )

    class TestListWorkflowRuns:
        def test_happy_path(self, client, runtime_config, monkeypatch, tmp_path):
            # Given
            client.list_all.return_value = [("mocked", Mock())]
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            mock_status = Mock()
            monkeypatch.setattr(
                runtime, "get_workflow_run_status", Mock(return_value=mock_status)
            )
            # When
            runs = runtime.list_workflow_runs()
            # Then
            client.list_all.assert_called()
            assert len(runs) == 1

        def test_missing_wf_in_runtime(
            self, client, runtime_config, monkeypatch, tmp_path
        ):
            # Given
            client.list_all.return_value = [("mocked", Mock())]
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            monkeypatch.setattr(
                runtime,
                "get_workflow_run_status",
                Mock(side_effect=exceptions.WorkflowRunNotFoundError),
            )
            # When
            runs = runtime.list_workflow_runs()
            # Then
            assert len(runs) == 0

        def test_with_state(self, client, runtime_config, monkeypatch, tmp_path):
            # Given
            client.list_all.return_value = [("mocked", Mock())] * 4
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            # Given
            mock_status = Mock()
            type(mock_status.status).state = PropertyMock(
                side_effect=[
                    State.RUNNING,
                    State.SUCCEEDED,
                    State.FAILED,
                    State.RUNNING,
                ]
            )
            monkeypatch.setattr(
                runtime, "get_workflow_run_status", Mock(return_value=mock_status)
            )
            # When
            runs = runtime.list_workflow_runs(state=State.RUNNING)
            # Then
            assert len(runs) == 2

        def test_with_state_list(self, client, runtime_config, monkeypatch, tmp_path):
            # Given
            client.list_all.return_value = [("mocked", Mock())] * 4
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            # Given
            mock_status = Mock()
            type(mock_status.status).state = PropertyMock(
                side_effect=[
                    State.RUNNING,
                    State.SUCCEEDED,
                    State.FAILED,
                    State.RUNNING,
                ]
            )
            monkeypatch.setattr(
                runtime, "get_workflow_run_status", Mock(return_value=mock_status)
            )
            # When
            runs = runtime.list_workflow_runs(state=[State.SUCCEEDED, State.FAILED])
            # Then
            assert len(runs) == 2

        def test_with_max_age(self, client, runtime_config, monkeypatch, tmp_path):
            # Given
            client.list_all.return_value = [("mocked", Mock())] * 4
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            mock_status = Mock()
            type(mock_status.status).start_time = PropertyMock(
                side_effect=[
                    None,
                    _dates.now() - timedelta(seconds=5),
                    _dates.now() - timedelta(seconds=5),
                    _dates.now() - timedelta(days=4),
                ]
            )
            monkeypatch.setattr(
                runtime, "get_workflow_run_status", Mock(return_value=mock_status)
            )
            # When
            runs = runtime.list_workflow_runs(max_age=timedelta(minutes=2))
            # Then
            assert len(runs) == 3

        def test_with_limit(self, client, runtime_config, monkeypatch, tmp_path):
            # Given
            client.list_all.return_value = [("mocked", Mock())] * 4
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )
            mock_status = Mock()
            type(mock_status.status).start_time = PropertyMock(
                side_effect=[
                    None,
                    _dates.now() - timedelta(seconds=5),
                    _dates.now() - timedelta(seconds=5),
                    _dates.now() - timedelta(days=4),
                ]
            )
            monkeypatch.setattr(
                runtime, "get_workflow_run_status", Mock(return_value=mock_status)
            )
            # When
            runs = runtime.list_workflow_runs(limit=2)
            # Then
            assert len(runs) == 2

        @staticmethod
        @pytest.mark.parametrize(
            "kwargs",
            [
                {"workspace": "<workspace sentinel>"},
                {"project": "<project sentinel>"},
                {"workspace": "<workspace sentinel>", "project": "<project sentinel>"},
            ],
        )
        def test_raises_WorkspacesNotSupported_error_if_workspace_or_project(
            client, runtime_config, kwargs, tmp_path
        ):
            runtime = _dag.RayRuntime(
                client=client,
                config=runtime_config,
                project_dir=tmp_path,
            )

            with pytest.raises(exceptions.WorkspacesNotSupportedError):
                runtime = _dag.RayRuntime(
                    client=client,
                    config=runtime_config,
                    project_dir=tmp_path,
                )
                runtime.list_workflow_runs(**kwargs)

    class TestStartup:
        @staticmethod
        # Ray mishandles log file handlers and we get "_io.FileIO [closed]"
        # unraisable exceptions. Last tested with Ray 2.3.0.
        @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
        def test_raises_RayNotRunningError_when_ray_not_running(monkeypatch, tmp_path):
            # GIVEN
            monkeypatch.setattr(
                _client.RayClient,
                "init",
                Mock(
                    side_effect=ConnectionError(
                        "Could not find any running Ray instance"
                    )
                ),
            )
            ray_params = _dag.RayParams()

            # WHEN
            # THEN
            with pytest.raises(exceptions.RayNotRunningError):
                _dag.RayRuntime.startup(ray_params=ray_params)
