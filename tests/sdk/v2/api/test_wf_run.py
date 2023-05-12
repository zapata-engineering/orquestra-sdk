################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._api._wf_run.
"""

import itertools
import json
import time
import typing as t
from contextlib import suppress as do_not_raise
from datetime import timedelta
from unittest.mock import DEFAULT, MagicMock, Mock, PropertyMock, create_autospec

import pytest

from orquestra.sdk._base import _api, _workflow, serde
from orquestra.sdk._base._spaces._api import list_projects, list_workspaces
from orquestra.sdk._base._spaces._structs import Project, ProjectRef, Workspace
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.exceptions import (
    ProjectInvalidError,
    UnauthorizedError,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotFoundError,
    WorkflowRunNotStarted,
)
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.configs import RuntimeName
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import ProjectId, RunStatus, State
from orquestra.sdk.schema.workflow_run import TaskRun as TaskRunModel
from orquestra.sdk.schema.workflow_run import WorkspaceId

from ..data.complex_serialization.workflow_defs import (
    capitalize,
    join_strings,
    wf_pass_tuple,
)
from ..data.configs import TEST_CONFIG_JSON


@pytest.fixture
def tmp_default_config_json(patch_config_location):
    json_file = patch_config_location / "config.json"

    with json_file.open("w") as f:
        json.dump(TEST_CONFIG_JSON, f)

    return json_file


@pytest.fixture(autouse=True)
def set_config_location(patch_config_location):
    pass


class TestRunningInProcess:
    """
    Tests the public Python API for running workflows, using the "in-process"
    runtime.

    These tests in combination with those in
    tests/test_api_with_ray.py::TestRunningLocalInBackground exhibit the behaviour
    discussed in https://zapatacomputing.atlassian.net/browse/ORQSDK-485.
    """

    class TestTwoStepForm:
        @staticmethod
        def test_pass_builtin_config_name_no_file():
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

        @staticmethod
        def test_pass_builtin_config_name_with_file(tmp_default_config_json):
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

        def test_single_run(self):
            run = wf_pass_tuple().prepare("in_process")
            run.start()
            results = run.get_results()

            assert results == 3

        def test_multiple_starts(self):
            run = wf_pass_tuple().prepare("in_process")

            run.start()
            results1 = run.get_results()

            run.start()
            results2 = run.get_results()

            assert results1 == results2

    class TestShorthand:
        def test_single_run(self):
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

    class TestWithConfig:
        @staticmethod
        def test_pass_config():
            config = _api.RuntimeConfig.in_process()
            run = wf_pass_tuple().run(config)
            results = run.get_results()

            assert results == 3


class TestWorkflowRun:
    @staticmethod
    @pytest.fixture
    def sample_wf_def() -> _workflow.WorkflowDef:
        @_workflow.workflow
        def my_wf():
            # We need at least 4 invocations to use in our tests.
            text1 = capitalize(join_strings(["hello", "there"]))
            text2 = capitalize(join_strings(["general", "kenobi"]))
            return text1, text2

        return my_wf()

    @staticmethod
    @pytest.fixture
    def mock_runtime(sample_wf_def):
        runtime = create_autospec(RuntimeInterface, name="runtime")
        # For getting workflow ID
        runtime.create_workflow_run.return_value = "wf_pass_tuple-1"
        # For getting workflow outputs
        runtime.get_workflow_run_outputs_non_blocking.return_value = (
            serde.result_from_artifact("woohoo!", ir.ArtifactFormat.AUTO),
        )
        # for simulating a workflow running
        succeeded_run_model = Mock(name="succeeded wf run model")

        # We need the output ids to have a length as we use this to determine how many
        # results we expect. We set this to 2 to avoid the special case where single
        # return values are unpacked.
        succeeded_run_model.workflow_def.output_ids.__len__ = Mock(return_value=2)

        # Default value is "SUCCEEDED"
        succeeded_run_model.status.state = State.SUCCEEDED
        runtime.get_workflow_run_status.return_value = succeeded_run_model
        # Use side effects to simulate a running workflow

        running_wf_run_model = Mock(name="running wf run model")
        running_wf_run_model.status.state = State.RUNNING
        runtime.get_workflow_run_status.side_effect = itertools.chain(
            (
                running_wf_run_model,
                running_wf_run_model,
            ),
            itertools.repeat(DEFAULT),
        )

        # got getting task run artifacts
        runtime.get_available_outputs.return_value = {
            "task_run1": serde.result_from_artifact("woohoo!", ir.ArtifactFormat.AUTO),
            "task_run2": serde.result_from_artifact("another", ir.ArtifactFormat.AUTO),
            "task_run3": serde.result_from_artifact(123, ir.ArtifactFormat.AUTO),
        }

        wf_def_model = sample_wf_def.model
        task_invs = list(wf_def_model.task_invocations.values())
        # Get logs, the runtime interface returns invocation IDs
        runtime.get_workflow_logs.return_value = {
            task_invs[0].id: ["woohoo!\n"],
            task_invs[1].id: ["another\n", "line\n"],
            # This task invocation was executed, but it produced no logs.
            task_invs[2].id: [],
            # There's also 4th task invocation in the workflow def, it wasn't executed
            # yet, so we don't return it.
        }
        running_wf_run_model.task_runs = [
            TaskRunModel(
                id="task_run1",
                invocation_id=task_invs[0].id,
                status=RunStatus(state=State.SUCCEEDED),
            ),
            TaskRunModel(
                id="task_run2",
                invocation_id=task_invs[1].id,
                status=RunStatus(state=State.FAILED),
            ),
            TaskRunModel(
                id="task_run3",
                invocation_id=task_invs[2].id,
                status=RunStatus(state=State.FAILED),
            ),
        ]

        return runtime

    @staticmethod
    @pytest.fixture
    def run(sample_wf_def, mock_runtime) -> _api.WorkflowRun:
        return _api.WorkflowRun(
            run_id=None, wf_def=sample_wf_def.model, runtime=mock_runtime, config=None
        )

    class TestByID:
        class TestResolvingConfig:
            """
            Verifies logic for figuring out what config to use.
            """

            @staticmethod
            def test_passing_config_obj():
                # Given
                run_id = "wf.mine.1234"

                # Set up config
                config = _api.RuntimeConfig(
                    runtime_name=RuntimeName.IN_PROCESS,
                    bypass_factory_methods=True,
                )

                # Set up runtime
                runtime = Mock()
                setattr(config, "_get_runtime", lambda _: runtime)
                wf_def = "<wf def sentinel>"
                runtime.get_workflow_run_status().workflow_def = wf_def

                # When
                run = _api.WorkflowRun.by_id(
                    run_id=run_id,
                    config=config,
                )

                # Then
                # Uses the passed in config
                assert run._config == config

                # Sets other attrs appropriately
                assert run._run_id == run_id
                assert run._wf_def == wf_def
                assert run._runtime == runtime

            @staticmethod
            def test_passing_config_name(monkeypatch):
                # Given
                run_id = "wf.mine.1234"

                # Set up config
                config_name = "ray"
                config_obj = _api.RuntimeConfig(
                    runtime_name=RuntimeName.RAY_LOCAL,
                    name=config_name,
                    bypass_factory_methods=True,
                )
                monkeypatch.setattr(
                    _api.RuntimeConfig, "load", Mock(return_value=config_obj)
                )

                # Set up runtime
                runtime = Mock()
                setattr(config_obj, "_get_runtime", lambda _: runtime)
                wf_def = "<wf def sentinel>"
                runtime.get_workflow_run_status().workflow_def = wf_def

                # When
                run = _api.WorkflowRun.by_id(
                    run_id=run_id,
                    config=config_name,
                )

                # Then
                # Uses the passed in config
                assert run._config == config_obj

                # Sets other attrs appropriately
                assert run._run_id == run_id
                assert run._wf_def == wf_def
                assert run._runtime == runtime

            @staticmethod
            def test_passing_invalid_obj():
                # Given
                run_id = "wf.mine.1234"
                config: t.Any = object()

                # Then
                with pytest.raises(TypeError):
                    # When
                    _ = _api.WorkflowRun.by_id(
                        run_id=run_id,
                        config=config,
                    )

            class TestNotPassingConfig:
                @staticmethod
                def test_exists_in_local_db(monkeypatch):
                    # Given
                    run_id = "wf.mine.1234"
                    config_name = "ray"

                    # Simulate filled DB
                    monkeypatch.setattr(
                        _api.WorkflowRun,
                        "_get_stored_run",
                        Mock(
                            return_value=StoredWorkflowRun(
                                workflow_run_id=run_id,
                                config_name=config_name,
                                workflow_def=wf_pass_tuple().model,
                            )
                        ),
                    )

                    # Set up config
                    config_obj = _api.RuntimeConfig(
                        runtime_name=RuntimeName.RAY_LOCAL,
                        name=config_name,
                        bypass_factory_methods=True,
                    )
                    monkeypatch.setattr(
                        _api.RuntimeConfig, "load", Mock(return_value=config_obj)
                    )

                    # Set up runtime
                    runtime = Mock()
                    setattr(config_obj, "_get_runtime", lambda _: runtime)
                    wf_def = "<wf def sentinel>"
                    runtime.get_workflow_run_status().workflow_def = wf_def

                    # When
                    run = _api.WorkflowRun.by_id(run_id=run_id)

                    # Then
                    # Uses the passed in config
                    assert run._config == config_obj

                    # Sets other attrs appropriately
                    assert run._run_id == run_id
                    assert run._wf_def == wf_def
                    assert run._runtime == runtime

                @staticmethod
                def test_missing_from_db(monkeypatch):
                    # Given
                    run_id = "wf.mine.1234"

                    # Simulate empty DB
                    monkeypatch.setattr(
                        _api.WorkflowRun,
                        "_get_stored_run",
                        Mock(side_effect=WorkflowRunNotFoundError),
                    )

                    # Then
                    with pytest.raises(WorkflowRunNotFoundError):
                        # When
                        _ = _api.WorkflowRun.by_id(run_id=run_id)

    class TestGetStatus:
        @staticmethod
        def test_raises_exception_for_unstarted_workflow(run):
            with pytest.raises(WorkflowRunNotStarted):
                run.get_status()

        @staticmethod
        def test_returns_status_from_runtime(run, mock_runtime):
            # Given
            run.start()
            # When
            state = run.get_status()
            # Then
            mock_runtime.get_workflow_run_status.assert_called()
            assert state == State.RUNNING

    class TestGetStatusModel:
        @staticmethod
        def test_raises_exception_for_unstarted_workflow(run, mock_runtime):
            with pytest.raises(WorkflowRunNotStarted):
                run.get_status_model()

        @staticmethod
        def test_matches_get_status(run):
            run.start()

            model = run.get_status_model()

            assert model.status.state == run.get_status()

    class TestWaitUntilFinished:
        @staticmethod
        def test_raises_exception_if_workflow_not_started():
            # Given
            run = wf_pass_tuple().prepare("in_process")
            # When
            with pytest.raises(WorkflowRunNotStarted) as exc_info:
                run.wait_until_finished()
            # Then
            assert (
                "You will need to call the `.start()` method prior to calling this "
                "method."
            ) in str(exc_info)

        class TestHappyPath:
            @staticmethod
            def test_verbose(monkeypatch, run, mock_runtime, capsys):
                # Given
                monkeypatch.setattr(time, "sleep", MagicMock())

                # When
                run.start()
                run.wait_until_finished()

                # Then
                # We expect wait_until_finished to keep calling get_workflow_run_status
                # from the runtime until the status is in a completed state.
                # The mock runtime will return RUNNING twice before SUCCEEDED.
                # We expect 3 total calls to get_workflow_run_status
                assert mock_runtime.get_workflow_run_status.call_count == 3

                # We expect x prints to stderr.
                captured = capsys.readouterr()
                assert captured.out == ""
                assert captured.err == (
                    "wf_pass_tuple-1 is RUNNING. Sleeping for 4.0s...\n"
                    "wf_pass_tuple-1 is RUNNING. Sleeping for 4.0s...\n"
                    "wf_pass_tuple-1 is SUCCEEDED\n"
                )

            @staticmethod
            def test_quiet(monkeypatch, run, mock_runtime, capsys):
                # Given
                monkeypatch.setattr(time, "sleep", MagicMock())

                # When
                run.start()
                run.wait_until_finished(verbose=False)

                # Then
                # We expect wait_until_finished to keep calling get_workflow_run_status
                # from the runtime until the status is in a completed state.
                # The mock runtime will return RUNNING twice before SUCCEEDED.
                # We expect 3 total calls to get_workflow_run_status
                assert mock_runtime.get_workflow_run_status.call_count == 3

                # We expect no prints to stderr.
                captured = capsys.readouterr()
                assert captured.out == ""
                assert captured.err == ""

    class TestGetResults:
        @staticmethod
        def test_raises_exception_if_workflow_not_started():
            # Given
            run = wf_pass_tuple().prepare("in_process")
            # When
            with pytest.raises(WorkflowRunNotStarted) as exc_info:
                run.get_results()
            # Then
            assert (
                "You will need to call the `.start()` method prior to calling this "
                "method."
            ) in str(exc_info)

        @staticmethod
        def test_raises_exception_if_workflow_not_finished(run):
            # Given
            run.start()
            # When
            with pytest.raises(WorkflowRunNotFinished) as exc_info:
                run.get_results()
            # Then
            assert (
                "Workflow run with id wf_pass_tuple-1 has not finished. "
                "Current state: State.RUNNING"
            ) in str(exc_info)

        @staticmethod
        @pytest.mark.slow
        def test_waits_when_wait_is_true(run, mock_runtime):
            # Given
            run.start()
            # When
            results = run.get_results(wait=True)
            # Then
            assert mock_runtime.get_workflow_run_status.call_count >= 1
            assert results is not None
            assert results == "woohoo!"

        @staticmethod
        def test_waits_when_wait_is_explicitly_false(run, mock_runtime):
            # Remove RUNNING in mock
            mock_runtime.get_workflow_run_status.side_effect = None
            # Given
            run.start()
            # When
            results = run.get_results(wait=False)
            # Then
            assert results is not None
            assert results == "woohoo!"
            assert mock_runtime.get_workflow_run_status.call_count == 1

    class TestGetArtifacts:
        @staticmethod
        def test_raises_exception_if_workflow_not_started(run):
            # When
            with pytest.raises(WorkflowRunNotStarted) as exc_info:
                run.get_artifacts()
            # Then
            assert (
                "You will need to call the `.start()` method prior to calling this "
                "method."
            ) in str(exc_info)

        @staticmethod
        def test_handling_n_outputs():
            """
            Some tasks return 1 value, some return multiple. The values in the
            dict returned from `sdk.WorkflowRun.get_artifacts()` is supposed
            to correspond to whatever we would get if we ran the task function
            directly.

            Test boundary::
                [sdk.WorkflowRun]->[RuntimeInterface]
                                 ->[ir.WorkflowDef]
            """
            # Given
            runtime = create_autospec(RuntimeInterface)

            # The RuntimeInterface's contract for get_available_outputs is
            # to return whatever the task function returned.
            runtime.get_available_outputs.return_value = {
                "inv1": serde.result_from_artifact(42, ir.ArtifactFormat.AUTO),
                "inv2": serde.result_from_artifact((21, 38), ir.ArtifactFormat.AUTO),
            }

            mock_inv1 = create_autospec(ir.TaskInvocation)
            mock_inv1.output_ids = ["art1"]

            mock_inv2 = create_autospec(ir.TaskInvocation)
            mock_inv2.output_ids = ["art2", "art3"]

            wf_def = create_autospec(ir.WorkflowDef)
            wf_def.task_invocations = {
                "inv1": mock_inv1,
                "inv2": mock_inv2,
            }

            wf_run = _api.WorkflowRun(
                run_id="wf.1",
                wf_def=wf_def,
                runtime=runtime,
            )

            # When
            artifacts_dict = wf_run.get_artifacts()

            # Then
            assert artifacts_dict == {
                "inv1": 42,
                "inv2": (21, 38),
            }

    class TestGetTasks:
        @staticmethod
        def test_raises_exception_if_workflow_not_started(run):
            with pytest.raises(WorkflowRunNotStarted) as exc_info:
                run.get_tasks()

            # Then
            assert (
                "You will need to call the `.start()` method prior to calling this "
                "method."
            ) in str(exc_info)

        @staticmethod
        def test_get_tasks_from_started_workflow(run):
            # Given
            run.start()

            # When
            tasks = run.get_tasks()

            # Then
            assert len(tasks) == 3

            wf_def_model = run._wf_def
            for task in tasks:
                assert task.workflow_run_id == run.run_id
                assert task._runtime == run._runtime
                assert task._wf_def == run._wf_def
                assert task.task_invocation_id in wf_def_model.task_invocations

    class TestGetLogs:
        @staticmethod
        def test_raises_exception_if_workflow_not_started(run):
            # When
            with pytest.raises(WorkflowRunNotStarted) as exc_info:
                run.get_logs()

            # Then
            assert (
                "You will need to call the `.start()` method prior to calling this "
                "method."
            ) in str(exc_info)

        @staticmethod
        def test_happy_path(run):
            # Given
            run.start()

            # When
            logs = run.get_logs()

            # Then
            assert len(logs) == 3
            expected_inv = "invocation-0-task-capitalize"
            assert expected_inv in logs
            assert len(logs[expected_inv]) == 1
            assert logs[expected_inv][0] == "woohoo!\n"

    class TestGetConfig:
        @staticmethod
        def test_happy_path():
            config = _api.RuntimeConfig.in_process()
            wf = wf_pass_tuple().run(config=config)

            assert wf.config == config

        @staticmethod
        def test_no_config_run():
            with pytest.raises(FutureWarning):
                wf_pass_tuple().run()

    class TestStop:
        @staticmethod
        def test_happy_path():
            # Given
            run_id = "wf.1"
            wf_def = Mock()
            runtime = Mock()
            config = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # When
            run.stop()

            # Then
            runtime.stop_workflow_run.assert_called_with(run_id)

        @staticmethod
        def test_not_started():
            # Given
            run_id = None
            wf_def = Mock()
            runtime = Mock()
            config = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # Then
            with pytest.raises(WorkflowRunNotStarted):
                # When
                run.stop()

        @staticmethod
        @pytest.mark.parametrize(
            "exc",
            [
                UnauthorizedError(),
                WorkflowRunCanNotBeTerminated(),
                WorkflowRunNotFoundError(),
            ],
        )
        def test_error_from_runtime(exc):
            # Given
            run_id = "wf.1"
            wf_def = Mock()

            runtime = Mock()
            runtime.stop_workflow_run.side_effect = exc

            config = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # Then
            with pytest.raises(type(exc)):
                # When
                run.stop()


class TestListWorkflows:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        run = MagicMock()
        type(run).id = PropertyMock(side_effect=["wf0", "wf1", "wf2"])
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_workflow_runs.return_value = [run, run, run]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    def test_get_all_wfs(self, mock_config_runtime):
        # Given
        # When
        runs = _api.list_workflow_runs("mocked_config")
        # Then
        assert len(runs) == 3
        assert runs[0].run_id == "wf0"
        assert runs[1].run_id == "wf1"
        assert runs[2].run_id == "wf2"

    def test_invalid_max_age(self, mock_config_runtime):
        # Given
        # When
        with pytest.raises(ValueError) as exc_info:
            _ = _api.list_workflow_runs("mocked_config", max_age="hello")
        assert exc_info.match("Time strings must")

    @pytest.mark.parametrize(
        "max_age, delta",
        [
            ("1d", timedelta(days=1)),
            ("2h", timedelta(hours=2)),
            ("3m", timedelta(minutes=3)),
            ("4s", timedelta(seconds=4)),
            ("1d2h3m4s", timedelta(days=1, seconds=7384)),
        ],
    )
    def test_with_max_age(self, mock_config_runtime, max_age, delta):
        # Given
        # When
        _ = _api.list_workflow_runs("mocked_config", max_age=max_age)
        # Then
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=delta,
            state=None,
            workspace=None,
            project=None,
        )

    def test_with_limit(self, mock_config_runtime):
        # Given
        # When
        _ = _api.list_workflow_runs("mocked_config", limit=10)
        # Then
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=10,
            max_age=None,
            state=None,
            workspace=None,
            project=None,
        )

    def test_with_state(self, mock_config_runtime):
        # Given
        # When
        _ = _api.list_workflow_runs("mocked_config", state=State.SUCCEEDED)
        # Then
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=State.SUCCEEDED,
            workspace=None,
            project=None,
        )

    @pytest.mark.parametrize(
        "workspace",
        [
            WorkspaceId("<workspace ID sentinel>"),
            Workspace("<workspace ID sentinel>", "<workspace name sentinel>"),
        ],
    )
    def test_with_workspace(self, mock_config_runtime, workspace):
        _ = _api.list_workflow_runs("mocked_config", workspace=workspace)
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="<workspace ID sentinel>",
            project=None,
        )

    @pytest.mark.parametrize(
        "project",
        [
            ProjectId("<project ID sentinel>"),
            ProjectRef(
                workspace_id="<workspace ID sentinel>",
                project_id="<project ID sentinel>",
            ),
            Project(
                project_id="<project ID sentinel>",
                workspace_id="<workspace ID sentinel>",
                name="<project name sentinel>",
            ),
        ],
    )
    def test_with_project(self, mock_config_runtime, project):
        # We include the workspace argument here to avoid exceptions being raised when a
        # projectId is specified without an accompanying workspace ID.
        _ = _api.list_workflow_runs(
            "mocked_config", project=project, workspace="<workspace ID sentinel>"
        )
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="<workspace ID sentinel>",
            project="<project ID sentinel>",
        )


@pytest.mark.parametrize(
    "workspace_id, project_id, raises, expected",
    [
        ("a", "b", do_not_raise(), ProjectRef(workspace_id="a", project_id="b")),
        ("a", None, pytest.raises(ProjectInvalidError), None),
        (None, "b", pytest.raises(ProjectInvalidError), None),
        (None, None, do_not_raise(), None),
    ],
)
class TestProjectId:
    def test_prepare(self, workspace_id, project_id, raises, expected):
        with raises:
            wf = wf_pass_tuple().prepare(
                "in_process", workspace_id=workspace_id, project_id=project_id
            )
            assert wf._project == expected

    def test_run(self, workspace_id, project_id, raises, expected, monkeypatch):
        monkeypatch.setattr(_api._wf_run.WorkflowRun, "start", Mock())
        with raises:
            wf = wf_pass_tuple().run(
                "in_process", workspace_id=workspace_id, project_id=project_id
            )
            assert wf._project == expected


class TestListWorkspaces:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        ws = MagicMock()
        type(ws).workspace_id = PropertyMock(
            side_effect=[
                "ws1",
                "ws2",
            ]
        )
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_workspaces.return_value = [ws, ws]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    def test_list_workspaces(self, mock_config_runtime):
        # Given
        # When
        runs = list_workspaces("mocked_config")
        # Then
        assert len(runs) == 2
        assert runs[0].workspace_id == "ws1"
        assert runs[1].workspace_id == "ws2"
        mock_config_runtime.list_workspaces.assert_called_once()


class TestListProjects:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        ws = MagicMock()
        type(ws).project_id = PropertyMock(
            side_effect=[
                "p1",
                "p2",
            ]
        )
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_projects.return_value = [ws, ws]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    @pytest.mark.parametrize(
        "workspace, expected_argument",
        [
            ("string_workspace_id", "string_workspace_id"),
            (Workspace(workspace_id="id", name="name"), "id"),
        ],
    )
    def test_list_projects(self, mock_config_runtime, workspace, expected_argument):
        # Given
        # When
        runs = list_projects("mocked_config", workspace)
        # Then
        assert len(runs) == 2
        assert runs[0].project_id == "p1"
        assert runs[1].project_id == "p2"
        mock_config_runtime.list_projects.assert_called_with(expected_argument)
