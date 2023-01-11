################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._api.
"""

import builtins
import json
import subprocess
import sys
import time
import typing as t
import unittest
import warnings
from unittest.mock import DEFAULT, MagicMock, Mock, patch

import pytest

from orquestra.sdk._base import _api, _config
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.exceptions import (
    ConfigNameNotFoundError,
    NotFoundError,
    TaskRunNotFound,
    UnauthorizedError,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotFoundError,
    WorkflowRunNotStarted,
)
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    RuntimeConfigurationFile,
    RuntimeName,
)
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import RunStatus, State, TaskRun

from .data.complex_serialization.workflow_defs import wf_pass_tuple
from .data.configs import TEST_CONFIG_JSON


@pytest.fixture()
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


@pytest.fixture
def tmp_default_config_json(patch_config_location):
    json_file = patch_config_location / "config.json"

    with open(json_file, "w") as f:
        json.dump(TEST_CONFIG_JSON, f)

    return json_file


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
        def test_pass_builtin_config_name_no_file(patch_config_location):
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
    def mock_runtime():
        runtime = MagicMock(RuntimeInterface)
        # For getting workflow ID
        runtime.create_workflow_run.return_value = "wf_pass_tuple-1"
        # For getting workflow outputs
        runtime.get_workflow_run_outputs_non_blocking.return_value = "woohoo!"
        # for simulating a workflow running
        _succeeded = MagicMock()
        # Default value is "SUCCEEDED"
        _succeeded.status.state = State.SUCCEEDED
        runtime.get_workflow_run_status.return_value = _succeeded
        # Use side effects to simulate a running workflow
        # Note: if you call get_workflow_run_status too many times, you may see a
        #       StopIteration exception
        _running = MagicMock()
        _running.status.state = State.RUNNING
        runtime.get_workflow_run_status.side_effect = [
            _running,
            _running,
            DEFAULT,
            DEFAULT,
        ]
        # got getting task run artifacts
        runtime.get_available_outputs.return_value = {
            "task_run1": "woohoo!",
            "task_run2": "another",
            "task_run3": 123,
        }
        # Get logs, the runtime interface returns invocation IDs
        runtime.get_full_logs.return_value = {
            "task_invocation1": ["woohoo!\n"],
            "task_invocation2": ["another\n", "line\n"],
            "task_invocation3": ["hello\n", "a log\n"],
        }
        return runtime

    @staticmethod
    @pytest.fixture
    def run(mock_runtime) -> _api.WorkflowRun:
        return _api.WorkflowRun(None, wf_pass_tuple().model, mock_runtime)

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

        @staticmethod
        def test_waits_until_finished(monkeypatch, run, mock_runtime):
            # Given
            monkeypatch.setattr(time, "sleep", MagicMock())
            # When
            run.start()
            run.wait_until_finished()
            # Then
            # We expect wait_until_finished to keep calling get_workflow_run_status from
            # the runtime until the status is in a completed state.
            # The mock runtime will return RUNNING twice before SUCCEEDED.
            # We expect 3 total calls to get_workflow_run_status
            assert mock_runtime.get_workflow_run_status.call_count == 3

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
        def test_get_all_artifacts(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts()
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 3
            assert "task_run1" in artifacts
            assert "task_run2" in artifacts
            assert "task_run3" in artifacts

        @staticmethod
        def test_get_with_list(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts(
                ["task_run1", "task_run3"], only_available=True
            )
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 2
            assert "task_run1" in artifacts
            assert "task_run3" in artifacts

        @staticmethod
        def test_get_with_str(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts("task_run1")
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 1
            assert "task_run1" in artifacts

        @staticmethod
        def test_get_with_unknown_str(run, mock_runtime):
            # Given
            run.start()
            # When
            with pytest.raises(TaskRunNotFound) as exc_info:
                _ = run.get_artifacts("doesn't exist")
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert exc_info.match(
                r"Task run with id `.*` not found. "
                "It may not be completed or does not exist in this WorkflowRun."
            )

        @staticmethod
        def test_get_only_available_with_list(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts(
                ["task_run1", "task_run3"], only_available=True
            )
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 2
            assert "task_run1" in artifacts
            assert "task_run3" in artifacts

        @staticmethod
        def test_get_only_available_with_str(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts("task_run1", only_available=True)
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 1
            assert "task_run1" in artifacts

        @staticmethod
        def test_get_only_available_unknown_str(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts("doesn't exist", only_available=True)
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 0

        @staticmethod
        def test_get_only_available_both_unknown_and_known(run, mock_runtime):
            # Given
            run.start()
            # When
            artifacts = run.get_artifacts(
                ["doesn't exist", "task_run1"], only_available=True
            )
            # Then
            mock_runtime.get_available_outputs.assert_called()
            assert len(artifacts) == 1
            assert "task_run1" in artifacts

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
            run.start()

            tasks = run.get_tasks()

            assert len(tasks) == 1
            task = tasks.pop()
            assert task.workflow_run_id == run.run_id
            assert task._runtime == run._runtime
            assert task._wf_def == run._wf_def
            assert task.task_run_id == next(iter(run._wf_def.task_invocations.keys()))

    class TestGetLogs:
        @staticmethod
        def test_raises_exception_if_workflow_not_started(run):
            # When
            with pytest.raises(WorkflowRunNotStarted) as exc_info:
                run.get_logs(tasks=[])
            # Then
            assert (
                "You will need to call the `.start()` method prior to calling this "
                "method."
            ) in str(exc_info)

        @staticmethod
        def test_get_logs(run):
            # Given
            run.start()
            # When
            logs = run.get_logs(tasks=["task_run1"])
            # Then
            assert len(logs) == 1
            assert "task_run1" in logs
            assert len(logs["task_run1"]) == 1
            assert logs["task_run1"][0] == "woohoo!\n"

        @staticmethod
        def test_get_logs_str(run):
            # Given
            run.start()
            # When
            logs = run.get_logs(tasks="task_run1")
            # Then
            assert len(logs) == 1
            assert "task_run1" in logs

        @staticmethod
        def test_get_logs_missing_only_available_false(run, mock_runtime):
            # Given
            mock_runtime.get_full_logs.side_effect = [DEFAULT, NotFoundError()]
            run.start()
            # When
            with pytest.raises(TaskRunNotFound) as exc_info:
                _ = run.get_logs(tasks=["task_run1", "doesn't exist"])
            # Then
            assert exc_info.match("Task run with id `.*` not found")

        @staticmethod
        def test_get_logs_missing_only_available_true(run, mock_runtime):
            # Given
            mock_runtime.get_full_logs.side_effect = [DEFAULT, NotFoundError()]
            run.start()
            # When
            logs = run.get_logs(
                tasks=["task_run1", "doesn't exist"], only_available=True
            )
            # Then
            assert len(logs) == 1
            assert "task_run1" in logs
            assert "doesn't exist" not in logs

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


class TestTaskRun:
    class TestInit:
        @staticmethod
        def test_init_simple_wf():
            # given
            from .data.task_run_workflow_defs import (
                return_num,
                simple_wf_one_task_two_invocations,
            )

            wf_def = simple_wf_one_task_two_invocations().model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            wf_id = "id"  # mock wf id - we dont need runtime here

            # when
            task_run = _api.TaskRun(inv_id, wf_id, Mock(), wf_def)

            # then
            assert task_run.task_run_id == inv_id
            assert "task_run_workflow_defs" in task_run.module
            assert task_run.fn_name == return_num.__name__
            assert task_run.workflow_run_id == wf_id

        @staticmethod
        def test_init_simple_wf_inline_fn():
            # given
            from .data.task_run_workflow_defs import (
                return_five_inline,
                simple_wf_one_task_inline,
            )

            wf_def = simple_wf_one_task_inline().model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            wf_id = "id"  # mock wf id - we dont need runtime here

            # when
            task_run = _api.TaskRun(inv_id, wf_id, Mock(), wf_def)

            # then
            assert task_run.task_run_id == inv_id
            assert task_run.module is None
            assert task_run.fn_name == return_five_inline.__name__
            assert task_run.workflow_run_id == wf_id

    class TestGetStatus:
        @staticmethod
        def test_get_status():
            # Given
            from .data.task_run_workflow_defs import simple_wf_one_task_two_invocations

            wf_def = simple_wf_one_task_two_invocations().model
            inv_ids = list(wf_def.task_invocations.keys())
            wf_id = "id"  # mock wf id - we dont need runtime here

            runtime = MagicMock(RuntimeInterface)
            ir_task_run_mocks = [
                TaskRun(
                    id="",
                    invocation_id=inv_ids[0],
                    status=RunStatus(state=State.SUCCEEDED),
                ),
                TaskRun(
                    id="",
                    invocation_id=inv_ids[1],
                    status=RunStatus(state=State.FAILED),
                ),
            ]
            wf_run = MagicMock()
            wf_run.task_runs = ir_task_run_mocks
            runtime.get_workflow_run_status.return_value = wf_run

            # When
            task_runs = [
                _api.TaskRun(inv_id, wf_id, runtime, wf_def) for inv_id in inv_ids
            ]
            statuses = [task_run.get_status() for task_run in task_runs]

            # Then
            assert len(statuses) == 2
            assert State.SUCCEEDED in statuses
            assert State.FAILED in statuses

    class TestGetLogs:
        @staticmethod
        def test_get_logs():
            # Given
            from .data.task_run_workflow_defs import simple_wf_one_task_two_invocations

            wf_def = simple_wf_one_task_two_invocations().model
            inv_ids = list(wf_def.task_invocations.keys())
            wf_id = "id"  # mock wf id - we dont need runtime here

            runtime = MagicMock(RuntimeInterface)
            my_log = Mock()  # it can be whatever
            runtime.get_full_logs.return_value = my_log

            # When
            task_runs = [
                _api.TaskRun(inv_id, wf_id, runtime, wf_def) for inv_id in inv_ids
            ]
            logs = [task_run.get_logs() for task_run in task_runs]

            # Then
            assert len(logs) == 2
            # Getting logs should be transparent. return whatever runtime returns to us
            assert logs[0] == my_log
            assert logs[1] == my_log

    class TestGetOutputs:
        @staticmethod
        def test_get_outputs_all_finished():
            from .data.task_run_workflow_defs import simple_wf_one_task_two_invocations

            wf_def = simple_wf_one_task_two_invocations().model
            inv_ids = list(wf_def.task_invocations.keys())
            wf_id = "id"  # mock wf id - we dont need runtime here

            runtime = MagicMock(RuntimeInterface)
            runtime_outputs = {inv_id: 5 for inv_id in inv_ids}
            runtime.get_available_outputs.return_value = runtime_outputs

            # When
            task_runs = [
                _api.TaskRun(inv_id, wf_id, runtime, wf_def) for inv_id in inv_ids
            ]
            outputs = [task_run.get_outputs() for task_run in task_runs]

            # Then
            assert outputs == [5, 5]

        @staticmethod
        def test_get_outputs_not_all_finished():
            from .data.task_run_workflow_defs import simple_wf_one_task_two_invocations

            wf_def = simple_wf_one_task_two_invocations().model
            inv_ids = list(wf_def.task_invocations.keys())
            wf_id = "id"  # mock wf id - we dont need runtime here

            runtime = MagicMock(RuntimeInterface)
            runtime_outputs = {inv_ids[0]: 15}
            runtime.get_available_outputs.return_value = runtime_outputs

            # When
            task_runs = [
                _api.TaskRun(inv_id, wf_id, runtime, wf_def) for inv_id in inv_ids
            ]
            available_output = task_runs[0].get_outputs()

            # Then
            with pytest.raises(TaskRunNotFound):
                task_runs[1].get_outputs()
            assert available_output == 15

    class TestGetParents:
        @staticmethod
        def _are_two_task_runs_the_same_task(t1: _api.TaskRun, t2: _api.TaskRun):
            return (
                t1.task_run_id == t2.task_run_id
                and t1.workflow_run_id == t2.workflow_run_id
                and t1.fn_name == t2.fn_name
                and t1.module == t2.module
            )

        @staticmethod
        def test_no_parents():
            # given
            from .data.task_run_workflow_defs import simple_wf_one_task_inline

            wf_def = simple_wf_one_task_inline().model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            wf_id = "id"  # mock wf id - we dont need runtime here

            # when
            task_run = _api.TaskRun(inv_id, wf_id, Mock(), wf_def)
            parents = task_run.get_parents()

            # then
            assert len(parents) == 0

        def test_multiple_parents(self):
            """
            Workflow graph in the scenario under test:
               5
               │
              [ ] <- 0 parents, 1 const input
             __│__
             │   │
             ▼   ▼
            [X] [ ]  7 <- each have 1 the same parent, the task with 0 parents
             │___│___│
                 │
                 ▼
                [X] <- 2 parents, each of the tasks with 1 parent
            """
            # given
            from .data.task_run_workflow_defs import wf_task_with_two_parents

            wf_def = wf_task_with_two_parents().model
            inv_ids = list(wf_def.task_invocations.keys())
            wf_id = "id"  # mock wf id - we dont need runtime here

            # when
            task_runs = [
                _api.TaskRun(inv_id, wf_id, Mock(), wf_def) for inv_id in inv_ids
            ]
            parents = {task_run: task_run.get_parents() for task_run in task_runs}

            # then
            assert len(parents) == 4
            num_of_parents = sorted(len(task_run) for task_run in parents.values())
            assert num_of_parents == [0, 1, 1, 2]  # check for number of parents
            # verify the parent correctness - the task with 1 parent, its parent is the
            # task with no parents. etc.
            no_parent_task = [
                task for task, parent in parents.items() if len(parent) == 0
            ][0]
            one_parent_tasks = [
                task for task, parent in parents.items() if len(parent) == 1
            ]
            two_parent_task = [
                task for task, parent in parents.items() if len(parent) == 2
            ][0]
            # both one-parent tasks have the same parent - the task with - parents
            for one_parent_task in one_parent_tasks:
                assert self._are_two_task_runs_the_same_task(
                    list(parents[one_parent_task])[0], no_parent_task
                )
            # 2 parent task has each of 1-parent tasks as its parent
            parents_of_two_parent_task = list(parents[two_parent_task])
            for parent in parents_of_two_parent_task:
                assert self._are_two_task_runs_the_same_task(
                    parent, one_parent_tasks[0]
                ) or self._are_two_task_runs_the_same_task(parent, one_parent_tasks[1])
            # and make sure those parents are different tasks
            assert not self._are_two_task_runs_the_same_task(
                parents_of_two_parent_task[0], parents_of_two_parent_task[1]
            )

    class TestGetInput:
        @pytest.mark.parametrize(
            "workflow, expected_args, expected_kwargs",
            [
                ("simple_wf_one_task_inline", [], {}),
                ("wf_single_task_with_const_parent", [21], {}),
                ("wf_single_task_with_const_parent_kwargs", [], {"kwargs": 36}),
                ("wf_single_task_with_const_parent_args_kwargs", [21], {"kwargs": 36}),
            ],
        )
        def test_const_as_parent(self, workflow, expected_args, expected_kwargs):
            # given
            from .data import task_run_workflow_defs

            wf_def = getattr(task_run_workflow_defs, workflow).model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            wf_id = "id"  # mock wf id - we dont need runtime here

            # when
            task_run = _api.TaskRun(inv_id, wf_id, Mock(), wf_def)
            inputs = task_run.get_inputs()

            # then
            assert inputs.args == expected_args
            assert inputs.kwargs == expected_kwargs

        @staticmethod
        def _find_task_by_args_and_kwargs_number(arg_num, kwarg_num, wf_def):
            return next(
                iter(
                    inv_id
                    for inv_id, inv in wf_def.task_invocations.items()
                    if len(inv.args_ids) == arg_num and len(inv.kwargs_ids) == kwarg_num
                )
            )

        def test_tasks_as_parents(self):
            """
            Workflow graph in the scenario under test:
                 5
                 │
             10,[ ] <- 0 parents, 1 const input
             __│__
             │   │
             │   │
             ▼   ▼
            [ ] [X]  5 <- each have 1 the same parent, the task with 0 parents
             │___│___│
                 │
                 ▼
                [X] <- 2 parents, each of the tasks with 1 parent and const value
            Tasks marked as X are mocked to be not finished yet
            """

            # given
            from .data.task_run_workflow_defs import wf_for_input_test

            wf_def = wf_for_input_test().model
            # find the task with 1 arg and 0 kwarg args. It is the one at the top
            first_task = self._find_task_by_args_and_kwargs_number(1, 0, wf_def)
            # find the task with 1 arg and 1 kwarg. 2nd task that finished
            second_task = self._find_task_by_args_and_kwargs_number(1, 1, wf_def)

            runtime = MagicMock(RuntimeInterface)
            runtime_outputs = {first_task: 15, second_task: 25}
            runtime.get_available_outputs.return_value = runtime_outputs
            wf_id = "id"  # mock wf id - we dont need runtime here

            # when
            task_runs = [
                _api.TaskRun(inv_id, wf_id, runtime, wf_def)
                for inv_id in wf_def.task_invocations.keys()
            ]
            inputs = [task_run.get_inputs() for task_run in task_runs]

            # then
            expected_inputs = [
                ({5}, {}),
                ({10}, {"kwarg": 15}),
                ({10}, {"kwarg": 15}),
                ({25, _api.TaskRun.INPUT_UNAVAILABLE, 7}, {}),
            ]
            assert len(inputs) == len(expected_inputs)
            for mapped in map(lambda input: (set(input.args), input.kwargs), inputs):
                assert mapped in expected_inputs

        def test_task_with_multiple_output_as_parent(self):
            # given
            from .data.task_run_workflow_defs import wf_multi_output_task

            wf_def = wf_multi_output_task().model
            wf_id = "id"  # mock wf id - we dont need runtime here

            # find the task with 0 arg and 0 kwarg args
            first_task = self._find_task_by_args_and_kwargs_number(0, 0, wf_def)
            # find the task with 1 arg and 1 kwarg args. 2nd task with that finished
            second_task = self._find_task_by_args_and_kwargs_number(1, 0, wf_def)

            runtime = MagicMock(RuntimeInterface)
            runtime_outputs = {first_task: (21, 36), second_task: 25}
            runtime.get_available_outputs.return_value = runtime_outputs

            # when
            task_run = _api.TaskRun(second_task, wf_id, runtime, wf_def)
            task_input = task_run.get_inputs()

            # then
            assert task_input.args == [21]
            assert task_input.kwargs == {}


VALID_RUNTIME_NAMES: list = ["RAY_LOCAL", "QE_REMOTE", "IN_PROCESS", "CE_REMOTE"]
VALID_CONFIG_NAMES: list = ["name_with_underscores", "name with spaces"]


class TestRuntimeConfiguration:
    class TestInit:
        @staticmethod
        def test_raises_value_error_if_called_directly():
            with pytest.raises(ValueError) as exc_info:
                _api.RuntimeConfig("test_runtime_name")
            assert (
                "Please use the appropriate factory method for your desired runtime."
                in str(exc_info.value)
            )
            assert "`RuntimeConfig.in_process()`" in str(exc_info.value)
            assert "`RuntimeConfig.qe()`" in str(exc_info.value)
            assert "`RuntimeConfig.ray()`" in str(exc_info.value)
            assert "`RuntimeConfig.ce()`" in str(exc_info.value)

        @staticmethod
        def test_raises_exception_for_invalid_config_name():
            with pytest.raises(ValueError) as exc_info:
                _api.RuntimeConfig("bad_runtime_name", bypass_factory_methods=True)
            for valid_name in RuntimeName:
                assert valid_name.value in str(exc_info)

        @staticmethod
        @pytest.mark.parametrize("runtime_name", VALID_RUNTIME_NAMES)
        @pytest.mark.parametrize("config_name", VALID_CONFIG_NAMES)
        def test_assign_custom_name(runtime_name, config_name):
            config = _api.RuntimeConfig(
                runtime_name, name=config_name, bypass_factory_methods=True
            )

            assert config.name == config_name
            assert config._runtime_name == runtime_name

    class TestEq:
        config = _api.RuntimeConfig(
            "QE_REMOTE", name="test_config", bypass_factory_methods=True
        )
        setattr(config, "uri", "test_uri")
        setattr(config, "token", "test_token")

        def test_returns_true_for_matching_configs(self):
            test_config = _api.RuntimeConfig(
                self.config._runtime_name,
                name=self.config.name,
                bypass_factory_methods=True,
            )
            test_config.uri = self.config.uri
            test_config.token = self.config.token

            assert self.config == test_config

        @pytest.mark.parametrize(
            "runtime_name, config_name, runtime_options",
            [
                (
                    "QE_REMOTE",
                    "name_mismatch",
                    {"uri": "test_uri", "token": "test_token"},
                ),
                (
                    "RAY_LOCAL",
                    "test_config",
                    {"uri": "test_uri", "token": "test_token"},
                ),
                (
                    "QE_REMOTE",
                    "test_config",
                    {
                        "uri": "test_uri",
                        "token": "test_token",
                        "address": "test_address",
                    },
                ),
            ],
        )
        def test_returns_false_for_mismatched_configs(
            self, runtime_name, config_name, runtime_options
        ):
            test_config = _api.RuntimeConfig(
                runtime_name, name=config_name, bypass_factory_methods=True
            )
            for key in runtime_options:
                setattr(test_config, key, runtime_options[key])

            assert self.config != test_config

        @pytest.mark.parametrize("other", [9, "test_str", {"test_dict": None}])
        def test_returns_false_for_mismatched_type(self, other):
            assert self.config != other

    @pytest.mark.parametrize("runtime_name", VALID_RUNTIME_NAMES)
    class TestNameProperty:
        @staticmethod
        @pytest.mark.parametrize("config_name", VALID_CONFIG_NAMES)
        def test_happy_path(config_name, runtime_name):
            """
            This test looks nonsensical, but is intended to test the custom getter and
            setter defined for the name property.
            """
            config = _api.RuntimeConfig(
                runtime_name, name=config_name, bypass_factory_methods=True
            )

            config.name = config_name
            assert config_name == config_name

        @staticmethod
        def test_getter_raises_warning_if_name_is_not_set(runtime_name):
            config = _api.RuntimeConfig(runtime_name, bypass_factory_methods=True)

            telltale_string = (
                "You are trying to access the name of a RuntimeConfig instance that "
                "has not been named."
            )
            with pytest.warns(UserWarning, match=telltale_string):
                name = config.name

            assert name is None

    class TestGetRuntimeOptions:
        @staticmethod
        def test_happy_path():
            config = _api.RuntimeConfig(
                "QE_REMOTE", name="test_config", bypass_factory_methods=True
            )
            config.uri = "test_uri"
            config.token = "test_token"
            assert config._get_runtime_options() == {
                "uri": "test_uri",
                "token": "test_token",
            }

    class TestFactories:
        class TestInProcessFactory:
            @staticmethod
            def test_with_minimal_args():
                config = _api.RuntimeConfig.in_process()

                assert config.name == "in_process"
                assert config._runtime_name == "IN_PROCESS"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = _api.RuntimeConfig.in_process(name="test config")

                assert str(config.name) == "in_process"
                assert config._runtime_name == "IN_PROCESS"

        class TestRayFactory:
            @staticmethod
            def test_with_minimal_args():
                config = _api.RuntimeConfig.ray()

                assert config.name == "local"
                assert config._runtime_name == "RAY_LOCAL"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = _api.RuntimeConfig.ray(name="test config")

                assert config.name == "local"
                assert config._runtime_name == "RAY_LOCAL"

        class TestQeFactory:
            @staticmethod
            def test_with_minimal_args():
                config = _api.RuntimeConfig.qe(
                    uri="https://prod-d.orquestra.io/",
                    token="test token",
                )

                name = config.name
                assert name == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"
                assert config.token == "test token"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = _api.RuntimeConfig.qe(
                        name="test config",
                        uri="https://prod-d.orquestra.io/",
                        token="test token",
                    )

                assert str(config.name) == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"
                assert config.token == "test token"

        class TestRemoteRayFactory:
            @staticmethod
            def test_with_minimal_args():
                config = _api.RuntimeConfig.qe(
                    uri="https://prod-d.orquestra.io/",
                    token="test token",
                )

                name = config.name
                assert name == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"
                assert config.token == "test token"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = _api.RuntimeConfig.qe(
                        name="test config",
                        uri="https://prod-d.orquestra.io/",
                        token="test token",
                    )

                assert str(config.name) == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"
                assert config.token == "test token"

    class TestGetRuntime:
        @staticmethod
        def test_raises_exception_when_orquestra_runtime_is_not_installed(monkeypatch):
            def invalid_import(*_):
                raise ModuleNotFoundError

            monkeypatch.setattr(_api, "_build_runtime", Mock())
            monkeypatch.setattr(builtins, "__import__", invalid_import)

            config = _api.RuntimeConfig.ray()

            with pytest.raises(ModuleNotFoundError):
                config._get_runtime()

    class TestStr:
        @staticmethod
        def test_with_essential_params_only(change_test_dir):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                config = _api.RuntimeConfig("RAY_LOCAL", bypass_factory_methods=True)
            assert "RuntimeConfiguration 'None' for runtime RAY_LOCAL" in str(config)

        @staticmethod
        def test_with_optional_params(tmp_path):
            config = _api.RuntimeConfig(
                "RAY_LOCAL",
                name="test_name",
                bypass_factory_methods=True,
            )
            config.address = "test_address"
            config.uri = "test_url"
            config.token = "blah"

            outstr = str(config)

            for test_str in [
                "RuntimeConfiguration 'test_name'",
                "runtime RAY_LOCAL",
                "with parameters:",
                "- uri: test_url",
                "- token: blah",
                "- address: test_address",
            ]:
                assert test_str in outstr

    class TestListConfigs:
        @staticmethod
        def test_default_file_location(tmp_default_config_json):

            config_names = _api.RuntimeConfig.list_configs()

            assert config_names == [
                name for name in TEST_CONFIG_JSON["configs"]
            ] + list(_config.SPECIAL_CONFIG_NAME_DICT.keys())

        @staticmethod
        def test_custom_file_location(tmp_config_json):

            config_names = _api.RuntimeConfig.list_configs(tmp_config_json)

            assert config_names == [
                name for name in TEST_CONFIG_JSON["configs"]
            ] + list(_config.SPECIAL_CONFIG_NAME_DICT.keys())

        @staticmethod
        def test_empty_configs_key(patch_config_location):
            with open(patch_config_location / "config.json", "w") as f:
                json.dump({"configs": {}}, f)

            config_names = _api.RuntimeConfig.list_configs()

            assert config_names == list(_config.SPECIAL_CONFIG_NAME_DICT.keys())

        @staticmethod
        def test_no_configs_key(patch_config_location):
            with open(patch_config_location / "config.json", "w") as f:
                json.dump({}, f)

            config_names = _api.RuntimeConfig.list_configs()

            assert config_names == list(_config.SPECIAL_CONFIG_NAME_DICT.keys())

    class TestLoad:
        @pytest.mark.parametrize(
            "config_name", [name for name in TEST_CONFIG_JSON["configs"]]
        )
        class TestLoadSuccess:
            @staticmethod
            def test_with_default_file_path(tmp_default_config_json, config_name):
                config = _api.RuntimeConfig.load(config_name)

                config_params = TEST_CONFIG_JSON["configs"][config_name]
                assert config.name == config_name
                assert config._runtime_name == config_params["runtime_name"], (
                    f"config '{config_name}' has runtime_name '{config._runtime_name}',"
                    f" but should have config name '{config_params['runtime_name']}'."
                )
                for key in config_params["runtime_options"]:
                    assert getattr(config, key) == config_params["runtime_options"][key]

            @staticmethod
            def test_with_custom_file_path(tmp_config_json, config_name):
                config = _api.RuntimeConfig.load(
                    config_name, config_save_file=tmp_config_json
                )

                config_params = TEST_CONFIG_JSON["configs"][config_name]
                assert config.name == config_name
                assert config._runtime_name == config_params["runtime_name"]
                for key in config_params["runtime_options"]:
                    assert getattr(config, key) == config_params["runtime_options"][key]

        @staticmethod
        def test_invalid_name(tmp_config_json):
            with pytest.raises(ConfigNameNotFoundError):
                _api.RuntimeConfig.load(
                    "non-existing", config_save_file=tmp_config_json
                )

    class TestLoadDefault:
        @staticmethod
        def test_with_default_file_path(tmp_default_config_json):

            config = _api.RuntimeConfig.load_default()

            default_config_params = TEST_CONFIG_JSON["configs"][
                TEST_CONFIG_JSON["default_config_name"]
            ]
            assert config.name == default_config_params["config_name"]
            assert config._runtime_name == default_config_params["runtime_name"]
            assert config.uri == default_config_params["runtime_options"]["uri"]
            assert config.token == default_config_params["runtime_options"]["token"]

        @staticmethod
        def test_with_custom_file_path(tmp_config_json):
            config = _api.RuntimeConfig.load_default(config_save_file=tmp_config_json)

            default_config_params = TEST_CONFIG_JSON["configs"][
                TEST_CONFIG_JSON["default_config_name"]
            ]
            assert config.name == default_config_params["config_name"]
            assert config._runtime_name == default_config_params["runtime_name"]
            assert config.uri == default_config_params["runtime_options"]["uri"]
            assert config.token == default_config_params["runtime_options"]["token"]

    class TestIsSaved:
        @staticmethod
        def test_returns_true_if_saved(tmp_default_config_json):
            config = _api.RuntimeConfig.load("test_config_default")
            assert config.is_saved()

        @staticmethod
        def test_returns_false_if_unnamed(tmp_default_config_json):
            config = _api.RuntimeConfig.load("test_config_default")
            config._name = None
            assert not config.is_saved()

        @staticmethod
        def test_returns_false_if_no_previous_save_file(tmp_default_config_json):
            config = _api.RuntimeConfig.load("test_config_default")
            config._config_save_file = None
            assert not config.is_saved()

        @staticmethod
        def test_returns_false_if_no_file():
            config = _api.RuntimeConfig(
                "IN_PROCESS",
                name="test_name",
                bypass_factory_methods=True,
                config_save_file="not_a_valid_file",
            )
            assert not config.is_saved()

        @staticmethod
        def test_returns_false_if_unsaved_changes(tmp_default_config_json):
            config = _api.RuntimeConfig.load("test_config_default")
            config.name = "new_name"
            assert not config.is_saved()

        @staticmethod
        @pytest.mark.parametrize("config_name", ["local", "in_process"])
        def test_returns_true_if_reserved(config_name, tmp_default_config_json):
            config = _api.RuntimeConfig.load(config_name)
            assert config.is_saved()

    class TestAsDict:
        @staticmethod
        def test_with_no_runtime_options():
            config = _api.RuntimeConfig("IN_PROCESS", bypass_factory_methods=True)

            dict = config._as_dict()

            assert dict["config_name"] == "None"
            assert dict["runtime_name"] == "IN_PROCESS"
            assert dict["runtime_options"] == {}

        @staticmethod
        def test_with_all_runtime_options():
            config = _api.RuntimeConfig("IN_PROCESS", bypass_factory_methods=True)
            config.uri = "test_uri"
            config.address = "test_address"
            config.token = "test_token"

            dict = config._as_dict()

            assert dict["config_name"] == "None"
            assert dict["runtime_name"] == "IN_PROCESS"
            assert dict["runtime_options"]["uri"] == config.uri
            assert dict["runtime_options"]["address"] == config.address
            assert dict["runtime_options"]["token"] == config.token


def test_python_310_importlib_abc_bug():
    """
    In Python 3.10, there seems to be a bug in importlib that causes `importlib.abc` to
    fail to resolve properly following `import importlib`. As a result,

    ```bash
    python -c "import orquestra.sdk as sdk"
    ```

    can raise an AttributeError if we've used that pattern anywhere, for example:

    ```
    File "<string>", line 1, in <module>
    File "/Users/benjaminmummery/Documents/Projects/orquestra-sdk/src/orquestra/sdk/v2/__init__.py", line 23, in <module>
        from ._workflow import NotATaskWarning, WorkflowDef, WorkflowTemplate, workflow
    File "/Users/benjaminmummery/Documents/Projects/orquestra-sdk/src/orquestra/sdk/v2/_workflow.py", line 25, in <module>
        from . import _api, _dsl, loader
    File "/Users/benjaminmummery/Documents/Projects/orquestra-sdk/src/orquestra/sdk/v2/loader.py", line 34, in <module>
        class ImportFaker(importlib.abc.MetaPathFinder):
    AttributeError: module 'importlib' has no attribute 'abc'. Did you mean: '_abc'?
    ```

    If this test is failing, we'll need to track down the file where we're using `abc`
    import it explicitly instead:

    ```python
    from importlib import abc
    ...

    and then reference `abc` rather than `importlib.abc`.

    """  # noqa E501
    command = f'{str(sys.executable)} -c "import orquestra.sdk as sdk"'
    proc = subprocess.run(command, shell=True, capture_output=True)
    assert proc.returncode == 0, proc.stderr.decode()


@pytest.mark.parametrize(
    "input_config_file, expected_output_config_file, expected_stdout",
    [
        (  # No changes required
            {
                "configs": {
                    "single_config_no_changes": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "single_config_no_changes": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            ["No changes required for file"],
        ),
        (  # 2 config files, only one of which needs changing"
            {
                "configs": {
                    "2_configs_1_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    },
                    "not_this_one": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "2_configs_1_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    },
                    "not_this_one": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - 2_configs_1_needs_changing",  # NOQA E501
            ],
        ),
        (  # 1 config that needs changing, has additional fields that shouldn't change.
            {
                "configs": {
                    "single_config_with_additional_fields": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"blah": "blah_val"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "single_config_with_additional_fields": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "blah": "blah_val",
                            "temp_dir": None,
                        },
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - single_config_with_additional_fields",  # NOQA E501
            ],
        ),
        (  # multiple configs, all need updating
            {
                "configs": {
                    "multiple_configs_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    },
                    "this_one_too": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "blah": "other_blah_val",
                        },
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "multiple_configs_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "temp_dir": None,
                        },
                    },
                    "this_one_too": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "blah": "other_blah_val",
                            "temp_dir": None,
                        },
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 2 entries:\n - multiple_configs_need_updating\n - this_one_too",  # NOQA E501
            ],
        ),
        (  # Mix of QE and Ray configs - only ray should be updated.
            {
                "configs": {
                    "mix_of_QE_and_RAY": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    },
                    "update_me": {"runtime_name": "RAY_LOCAL", "runtime_options": {}},
                    "but_not_me": {"runtime_name": "QE_REMOTE", "runtime_options": {}},
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "mix_of_QE_and_RAY": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "temp_dir": None,
                        },
                    },
                    "update_me": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "temp_dir": None,
                        },
                    },
                    "but_not_me": {"runtime_name": "QE_REMOTE", "runtime_options": {}},
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 2 entries:\n - mix_of_QE_and_RAY\n - update_me",  # NOQA E501
            ],
        ),
        (  # version alone needs updating
            {
                "configs": {
                    "version_alone_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": "0.0.0",
            },
            {
                "configs": {
                    "version_alone_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 0 entries.",
            ],
        ),
        (  # version and configs need updating
            {
                "configs": {
                    "version_and_config_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    }
                },
                "version": "0.0.0",
            },
            {
                "configs": {
                    "version_and_config_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - version_and_config_need_updating",  # NOQA E501
            ],
        ),
    ],
)
class TestMigrateConfigFile:
    @staticmethod
    def test_for_default_file(
        input_config_file, expected_output_config_file, expected_stdout, capsys
    ):
        with patch(
            "builtins.open",
            unittest.mock.mock_open(read_data=json.dumps(input_config_file)),
        ) as m:
            _api.migrate_config_file()

        if input_config_file == expected_output_config_file:
            m().write.assert_not_called()
        else:
            m().write.assert_called_once_with(
                json.dumps(expected_output_config_file, indent=2)
            )
        captured = capsys.readouterr()
        for string in expected_stdout:
            assert string in captured.out

    @staticmethod
    def test_for_single_custom_file(
        input_config_file,
        expected_output_config_file,
        expected_stdout,
        capsys,
        tmp_path,
    ):
        config_file = tmp_path / "test_configs.json"
        with open(config_file, "w") as f:
            json.dump(input_config_file, f, indent=2)

        _api.migrate_config_file(config_file)

        with open(config_file, "r") as f:
            data = json.load(f)

        assert data == expected_output_config_file
        captured = capsys.readouterr()
        for string in expected_stdout:
            assert string in captured.out

    @staticmethod
    def test_for_multiple_files(
        input_config_file,
        expected_output_config_file,
        expected_stdout,
        capsys,
        tmp_path,
    ):
        config_file_1 = tmp_path / "test_configs_1.json"
        config_file_2 = tmp_path / "test_configs_2.json"
        with open(config_file_1, "w") as f:
            json.dump(input_config_file, f, indent=2)
        file_2_data = {
            "configs": {
                "single_config_no_changes": {
                    "runtime_name": "RAY_LOCAL",
                    "runtime_options": {},
                }
            },
            "version": CONFIG_FILE_CURRENT_VERSION,
        }
        expected_file_2_data = {
            "configs": {
                "single_config_no_changes": {
                    "runtime_name": "RAY_LOCAL",
                    "runtime_options": {
                        "temp_dir": None,
                    },
                }
            },
            "version": CONFIG_FILE_CURRENT_VERSION,
        }
        expected_file_2_outstr = f"Successfully migrated file {config_file_2} to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - single_config_no_changes"  # NOQA E501
        with open(config_file_2, "w") as f:
            json.dump(file_2_data, f)

        _api.migrate_config_file([config_file_1, config_file_2])

        with open(config_file_1, "r") as f:
            data_1 = json.load(f)
        with open(config_file_2, "r") as f:
            data_2 = json.load(f)
        assert data_1 == expected_output_config_file
        assert data_2 == expected_file_2_data
        captured = capsys.readouterr()

        for string in expected_stdout + [expected_file_2_outstr]:
            assert string in captured.out
