################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for repos. Isolated unit tests unless explicitly named as integration.
"""

import datetime
import json
import sys
import typing as t
import warnings
from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest
import requests

from orquestra import sdk
from orquestra.sdk._client._base import _config
from orquestra.sdk._client._base._config import SPECIAL_CONFIG_NAME_DICT
from orquestra.sdk._client._base._driver._client import DriverClient
from orquestra.sdk._client._base._testing import _example_wfs, _reloaders
from orquestra.sdk._client._base.cli import _repos
from orquestra.sdk._client._base.cli._ui import _models as ui_models
from orquestra.sdk._runtime._ray import _dag
from orquestra.sdk._shared import exceptions
from orquestra.sdk._shared.dates import _dates
from orquestra.sdk._shared.logs._interfaces import LogOutput, WorkflowLogs
from orquestra.sdk._shared.schema import ir
from orquestra.sdk._shared.schema.configs import RuntimeName
from orquestra.sdk._shared.schema.workflow_run import RunStatus, State
from orquestra.sdk._shared.schema.workflow_run import TaskRun as TaskRunModel
from orquestra.sdk._shared.schema.workflow_run import WorkflowRun as WorkflowRunModel
from orquestra.sdk._shared.schema.workflow_run import WorkflowRunSummary

from ..sdk.data.configs import TEST_CONFIG_JSON

INSTANT_1 = _dates.from_comps(2023, 2, 24, 7, 26, 7, 704015, utc_hour_offset=1)
INSTANT_2 = _dates.from_comps(2023, 2, 24, 7, 28, 37, 123, utc_hour_offset=1)


class TestWorkflowRunRepo:
    @staticmethod
    @pytest.fixture
    def mock_wf_run():
        """
        Returns a mock of shape of sdk.WorkflowRun. Used by other fixtures.
        """
        return create_autospec(sdk.WorkflowRun)

    @staticmethod
    @pytest.fixture
    def mock_by_id(monkeypatch, mock_wf_run):
        """
        Returns a mock of sdk.WorkflowRun.by_id.
        """
        by_id = Mock(return_value=mock_wf_run)
        monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

        return by_id

    class TestIsolation:
        """
        Isolated unit tests for WorkflowRunRepo.

        Test boundary::

            [WorkflowRunRepo]->[sdk.WorkflowRun]
                             ->[sdk.WorkflowDef]
        """

        class TestGetConfigNameByRunID:
            @staticmethod
            @pytest.mark.usefixtures("mock_by_id")
            def test_happy_path(mock_wf_run):
                # Given
                config = sdk.RuntimeConfig.ray()
                mock_wf_run.config = config

                repo = _repos.WorkflowRunRepo()
                wf_run_id = "wf.1"

                # When
                result_config_name = repo.get_config_name_by_run_id(wf_run_id)

                # Then
                assert result_config_name == config.name

            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [exceptions.ConfigFileNotFoundError()],
            )
            def test_passes_errors(mock_by_id, exc):
                # Given
                mock_by_id.side_effect = exc

                repo = _repos.WorkflowRunRepo()
                wf_run_id = "wf.1"

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_config_name_by_run_id(wf_run_id)

        @staticmethod
        def test_get_wf_by_run_id(mock_by_id, mock_wf_run):
            # Given
            run_id = "wf.1"
            config_name = "<config sentinel>"

            mock_wf_run.get_status_model().id = run_id

            repo = _repos.WorkflowRunRepo()

            # When
            wf_run = repo.get_wf_by_run_id(run_id, config_name)

            # Then
            assert wf_run.id == run_id
            mock_by_id.assert_called_with(run_id, config_name)

        class TestGetTaskRunID:
            @staticmethod
            def test_happy_path(mock_by_id, mock_wf_run):
                # Given
                wf_run_id = "wf.1"
                task_inv_id = "inv2"
                config = "config3"
                repo = _repos.WorkflowRunRepo()

                # Mocks
                status = RunStatus(
                    state=State.SUCCEEDED, start_time=None, end_time=None
                )
                mock_wf_run.get_status_model().task_runs = [
                    TaskRunModel(
                        id="1",
                        invocation_id="inv1",
                        status=status,
                    ),
                    TaskRunModel(
                        id="2",
                        invocation_id="inv2",
                        status=status,
                    ),
                    TaskRunModel(
                        id="3",
                        invocation_id="inv3",
                        status=status,
                    ),
                ]

                # When
                task_run_id = repo.get_task_run_id(
                    wf_run_id=wf_run_id,
                    task_inv_id=task_inv_id,
                    config_name=config,
                )

                # Then
                assert task_run_id == "2"

            @staticmethod
            def test_invalid_inv_id(mock_by_id, mock_wf_run):
                # Given
                wf_run_id = "wf.1"
                task_inv_id = "inv2_doesnt_exist"
                config = "config3"
                repo = _repos.WorkflowRunRepo()

                # Mocks
                status = RunStatus(
                    state=State.SUCCEEDED, start_time=None, end_time=None
                )
                mock_wf_run.get_status_model().task_runs = [
                    TaskRunModel(
                        id="1",
                        invocation_id="inv1",
                        status=status,
                    ),
                ]

                # Then
                with pytest.raises(exceptions.TaskInvocationNotFoundError) as exc_info:
                    # When
                    _ = repo.get_task_run_id(
                        wf_run_id=wf_run_id,
                        task_inv_id=task_inv_id,
                        config_name=config,
                    )
                assert exc_info.value.invocation_id == task_inv_id

            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.NotFoundError(),
                    exceptions.ConfigNameNotFoundError(),
                ],
            )
            def test_passing_errors(mock_by_id, exc):
                # Given
                wf_run_id = "wf.1"
                task_inv_id = "inv2_doesnt_exist"
                config = "config3"
                repo = _repos.WorkflowRunRepo()

                # Mocks
                mock_by_id.side_effect = exc

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_task_run_id(
                        wf_run_id=wf_run_id,
                        task_inv_id=task_inv_id,
                        config_name=config,
                    )

        class TestSubmit:
            @staticmethod
            def test_passes_parameters():
                # Given
                repo = _repos.WorkflowRunRepo()

                config = "test_cfg"
                run_id = "wf.2"
                project_id = "my_project"
                workspace_id = "my_workspace"

                wf_def = Mock()
                wf_def.run().run_id = run_id

                # When
                result_id = repo.submit(
                    wf_def,
                    config,
                    workspace_id=workspace_id,
                    project_id=project_id,
                    ignore_dirty_repo=True,
                )

                # Then
                assert result_id == run_id
                wf_def.run.assert_called_with(
                    config, workspace_id=workspace_id, project_id=project_id
                )

            class TestWithDirtyRepo:
                @staticmethod
                @pytest.fixture
                def wf_def():
                    run_id = "wf.2"
                    wf_def = Mock()

                    def _fake_run_method(*args, **kwargs):
                        warnings.warn(
                            "You have uncommitted changes", exceptions.DirtyGitRepo
                        )

                        wf_run = Mock()
                        wf_run.run_id = run_id
                        return wf_run

                    wf_def.run = _fake_run_method

                    return wf_def

                @staticmethod
                def test_raises_exception(wf_def):
                    # Given
                    repo = _repos.WorkflowRunRepo()
                    config = "test_cfg"

                    # When + Then
                    with pytest.raises(exceptions.DirtyGitRepo):
                        _ = repo.submit(
                            wf_def,
                            config,
                            ignore_dirty_repo=False,
                            workspace_id="ws",
                            project_id="project",
                        )

                @staticmethod
                def test_warns(wf_def):
                    # Given
                    repo = _repos.WorkflowRunRepo()
                    config = "test_cfg"

                    # When + Then
                    with pytest.warns(exceptions.DirtyGitRepo):
                        _ = repo.submit(
                            wf_def,
                            config,
                            ignore_dirty_repo=True,
                            workspace_id="ws",
                            project_id="project",
                        )

        class TestStop:
            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.UnauthorizedError(),
                    exceptions.WorkflowRunCanNotBeTerminated(),
                ],
            )
            def test_passing_data(monkeypatch, exc):
                # Given
                run_id = "wf.1"
                config_name = "<config sentinel>"
                force = False

                wf_run = Mock()
                wf_run.stop.side_effect = exc

                by_id = Mock(return_value=wf_run)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # Then
                # Validate passing exception
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.stop(run_id, config_name, force)

                # Then
                # Validate passing args
                by_id.assert_called_with(run_id, config_name)

        class TestGetWFOutputs:
            @staticmethod
            def test_passing_data(monkeypatch):
                run_id = "wf.1"
                config_name = "<config sentinel>"

                wf_run = Mock()
                fake_outputs = (
                    "<output sentinel 0>",
                    "<output sentinel 1>",
                )
                wf_run.get_results.return_value = fake_outputs

                by_id = Mock(return_value=wf_run)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # When
                outputs = repo.get_wf_outputs(run_id, config_name)

                # Then
                assert outputs == fake_outputs

            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.NotFoundError(),
                    exceptions.ConfigNameNotFoundError(),
                ],
            )
            def test_passing_errors(monkeypatch, exc):
                run_id = "wf.1"
                config_name = "<config sentinel>"

                by_id = Mock(side_effect=exc)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_wf_outputs(run_id, config_name)

        class TestGetTaskFNNames:
            @staticmethod
            def wf_run_with_task_defs(task_defs: t.List[ir.TaskDef]) -> sdk.WorkflowRun:
                wf_run_model = Mock()
                wf_run_model.workflow_def.tasks.values.return_value = task_defs

                wf_run = Mock(sdk.WorkflowRun)
                wf_run.get_status_model.return_value = wf_run_model

                return wf_run

            def test_mixed_imports(self, monkeypatch):
                # Given
                wf_run_id = "wf.1"
                config = "<config sentinel>"

                # Mocks
                wf_run = self.wf_run_with_task_defs(
                    [
                        ir.TaskDef(
                            id="task1",
                            fn_ref=ir.ModuleFunctionRef(
                                module="tasks",
                                function_name="task_in_another_module",
                            ),
                            parameters=[],
                            output_metadata=ir.TaskOutputMetadata(
                                is_subscriptable=False, n_outputs=1
                            ),
                            source_import_id="imp1",
                        ),
                        ir.TaskDef(
                            id="task2",
                            fn_ref=ir.FileFunctionRef(
                                file_path="other_tasks.py",
                                function_name="task_in_another_file",
                            ),
                            parameters=[],
                            output_metadata=ir.TaskOutputMetadata(
                                is_subscriptable=False, n_outputs=1
                            ),
                            source_import_id="imp1",
                        ),
                        ir.TaskDef(
                            id="task3",
                            fn_ref=ir.InlineFunctionRef(
                                function_name="inlined_task", encoded_function=[]
                            ),
                            parameters=[],
                            output_metadata=ir.TaskOutputMetadata(
                                is_subscriptable=False, n_outputs=1
                            ),
                            source_import_id="imp1",
                        ),
                    ]
                )
                by_id = Mock(return_value=wf_run)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # When
                names = repo.get_task_fn_names(wf_run_id, config)

                # Then
                assert names == [
                    "inlined_task",
                    "task_in_another_file",
                    "task_in_another_module",
                ]

            def test_shadowing_names(self, monkeypatch):
                # Given
                wf_run_id = "wf.1"
                config = "<config sentinel>"

                # Mocks
                fn_name = "my_fn"
                wf_run = self.wf_run_with_task_defs(
                    [
                        ir.TaskDef(
                            id="task1",
                            fn_ref=ir.ModuleFunctionRef(
                                module="tasks1", function_name=fn_name
                            ),
                            parameters=[],
                            output_metadata=ir.TaskOutputMetadata(
                                is_subscriptable=False, n_outputs=1
                            ),
                            source_import_id="imp1",
                        ),
                        ir.TaskDef(
                            id="task2",
                            fn_ref=ir.ModuleFunctionRef(
                                module="tasks2", function_name=fn_name
                            ),
                            parameters=[],
                            output_metadata=ir.TaskOutputMetadata(
                                is_subscriptable=False, n_outputs=1
                            ),
                            source_import_id="imp1",
                        ),
                    ]
                )
                by_id = Mock(return_value=wf_run)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # When
                names = repo.get_task_fn_names(wf_run_id, config)

                # Then
                assert names == [fn_name]

            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.NotFoundError(),
                    exceptions.ConfigNameNotFoundError(),
                ],
            )
            def test_passing_errors(monkeypatch, exc):
                wf_run_id = "wf.1"
                config_name = "<config sentinel>"

                by_id = Mock(side_effect=exc)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_task_fn_names(wf_run_id, config_name)

        class TestGetTaskInvIDs:
            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.NotFoundError(),
                    exceptions.ConfigNameNotFoundError(),
                ],
            )
            def test_passing_errors(monkeypatch, exc):
                wf_run_id = "wf.1"
                config_name = "<config sentinel>"
                task_fn_name = "my_fn"

                by_id = Mock(side_effect=exc)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)
                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_task_inv_ids(wf_run_id, config_name, task_fn_name)

        class TestGetTaskOutputs:
            class TestHappyPath:
                @staticmethod
                def _make_wf_run(
                    monkeypatch,
                    inv_id: str,
                    fake_task_run_outputs,
                    task_meta: ir.TaskOutputMetadata,
                ) -> sdk.WorkflowRun:
                    task_run = create_autospec(sdk.TaskRun)
                    task_run.task_invocation_id = inv_id
                    task_run.get_outputs.return_value = fake_task_run_outputs

                    task_def = create_autospec(ir.TaskDef)
                    task_def_id = "task_def_1"
                    task_def.output_metadata = task_meta

                    inv = create_autospec(ir.TaskInvocation)
                    inv.task_id = task_def_id

                    wf_def = create_autospec(ir.WorkflowDef)
                    wf_def.task_invocations = {inv_id: inv}
                    wf_def.tasks = {task_def_id: task_def}

                    wf_run = create_autospec(sdk.WorkflowRun)
                    wf_run.get_tasks.return_value = {task_run}
                    wf_run.get_status_model().workflow_def = wf_def

                    by_id = Mock(return_value=wf_run)
                    monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                    return wf_run

                def test_single_output(self, monkeypatch):
                    # Given
                    run_id = "wf.1"
                    inv_id = "inv1"
                    config_name = "<config sentinel>"
                    fake_output = "<task output sentinel>"

                    self._make_wf_run(
                        monkeypatch,
                        inv_id=inv_id,
                        fake_task_run_outputs=fake_output,
                        task_meta=ir.TaskOutputMetadata(
                            n_outputs=1,
                            is_subscriptable=False,
                        ),
                    )

                    repo = _repos.WorkflowRunRepo()

                    # When
                    outputs = repo.get_task_outputs(
                        wf_run_id=run_id, task_inv_id=inv_id, config_name=config_name
                    )

                    # Then
                    assert outputs == (fake_output,)

                def test_multiple_outputs(self, monkeypatch):
                    # Given
                    run_id = "wf.1"
                    inv_id = "inv1"
                    config_name = "<config sentinel>"
                    fake_outputs = ("<out1>", "<out2>")

                    self._make_wf_run(
                        monkeypatch,
                        inv_id=inv_id,
                        fake_task_run_outputs=fake_outputs,
                        task_meta=ir.TaskOutputMetadata(
                            n_outputs=2,
                            is_subscriptable=True,
                        ),
                    )

                    repo = _repos.WorkflowRunRepo()

                    # When
                    outputs = repo.get_task_outputs(
                        wf_run_id=run_id, task_inv_id=inv_id, config_name=config_name
                    )

                    # Then
                    assert outputs == fake_outputs

            @staticmethod
            def test_invalid_inv_id(monkeypatch):
                run_id = "wf.1"
                inv_id = "inv1"
                config_name = "<config sentinel>"

                task_run = create_autospec(sdk.TaskRun)
                task_run.task_invocation_id = "<invalid inv ID>"

                wf_run = create_autospec(sdk.WorkflowRun)
                wf_run.get_tasks.return_value = {task_run}

                by_id = Mock(return_value=wf_run)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(exceptions.TaskInvocationNotFoundError):
                    # When
                    _ = repo.get_task_outputs(
                        wf_run_id=run_id, task_inv_id=inv_id, config_name=config_name
                    )

            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.NotFoundError(),
                    exceptions.ConfigNameNotFoundError(),
                ],
            )
            def test_passing_errors(monkeypatch, exc):
                run_id = "wf.1"
                inv_id = "inv1"
                config_name = "<config sentinel>"

                by_id = Mock(side_effect=exc)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_task_outputs(
                        wf_run_id=run_id, task_inv_id=inv_id, config_name=config_name
                    )

        class TestGetWFLogs:
            @staticmethod
            def test_passing_values(mock_by_id, mock_wf_run):
                # Given
                config = "<config sentinel>"
                wf_run_id = "<id sentinel>"
                per_task_logs_dict = {
                    "inv1": LogOutput(out=["my_log", "my_another_log"], err=[]),
                    "inv2": LogOutput(out=["and another one"], err=[]),
                }
                env_setup_logs_list = LogOutput(
                    out=[
                        "<env setup log sentinel 1>",
                        "<env setup log sentinel 2>",
                    ],
                    err=[],
                )
                sys_logs_list = LogOutput(
                    out=[
                        "<sys log sentinel 1>",
                        "<sys log sentinel 2>",
                    ],
                    err=[],
                )

                mock_wf_run.get_logs.return_value = WorkflowLogs(
                    per_task=per_task_logs_dict,
                    env_setup=env_setup_logs_list,
                    system=sys_logs_list,
                    other=LogOutput(out=[], err=[]),
                )

                repo = _repos.WorkflowRunRepo()

                # When
                logs = repo.get_wf_logs(
                    wf_run_id,
                    config,
                )

                # Then
                assert logs.per_task == per_task_logs_dict
                assert logs.system == sys_logs_list
                assert logs.env_setup == env_setup_logs_list

            @staticmethod
            @pytest.mark.parametrize(
                "exc", [ConnectionError(), exceptions.UnauthorizedError()]
            )
            def test_passing_errors(mock_by_id, mock_wf_run, exc):
                # Given
                config = "<config sentinel>"
                wf_run_id = "<id sentinel>"

                mock_wf_run.get_logs.side_effect = exc

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.get_wf_logs(wf_run_id, config)

        class TestGetTaskLogs:
            @staticmethod
            def test_passing_values(mock_by_id, mock_wf_run):
                # Given
                config = "<config sentinel>"
                wf_run_id = "<id sentinel>"
                task_inv_id = "<inv id sentinel>"
                log_lines = ["my_log", "my_another_log"]

                tasks = [create_autospec(sdk.TaskRun), create_autospec(sdk.TaskRun)]
                tasks[0].task_invocation_id = task_inv_id
                tasks[0].get_logs.return_value = log_lines

                tasks[1].task_invocation_id = "inv3"

                mock_wf_run.get_tasks.return_value = tasks

                repo = _repos.WorkflowRunRepo()

                # When
                retrieved = repo.get_task_logs(wf_run_id, task_inv_id, config)

                # Then
                assert retrieved == {task_inv_id: log_lines}

            @staticmethod
            def test_invalid_inv_id(mock_by_id, mock_wf_run):
                # Given
                config = "<config sentinel>"
                wf_run_id = "<id sentinel>"
                task_inv_id = "<inv id sentinel>"

                tasks = [create_autospec(sdk.TaskRun), create_autospec(sdk.TaskRun)]
                tasks[0].task_invocation_id = "inv0"
                tasks[1].task_invocation_id = "inv1"

                mock_wf_run.get_tasks.return_value = tasks

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(exceptions.TaskInvocationNotFoundError):
                    # When
                    _ = repo.get_task_logs(wf_run_id, task_inv_id, config)

            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.NotFoundError,
                    exceptions.ConfigNameNotFoundError,
                ],
            )
            def test_passing_errors(mock_by_id, mock_wf_run, exc):
                # Given
                config = "<config sentinel>"
                wf_run_id = "<id sentinel>"
                task_inv_id = "<inv id sentinel>"

                mock_by_id.side_effect = exc

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(exc):
                    # When
                    _ = repo.get_task_logs(wf_run_id, task_inv_id, config)

    class TestIntegration:
        @staticmethod
        def test_list_wf_runs(monkeypatch):
            # Given
            config = "ray"
            ws = "ws"
            stub_run_ids = ["wf.1", "wf.2"]
            state = State("RUNNING")
            mock_wf_runs = []

            for stub_id in stub_run_ids:
                wf_run = Mock()
                wf_run.get_status_model.return_value = WorkflowRunModel(
                    id=stub_id,
                    workflow_def=create_autospec(ir.WorkflowDef),
                    task_runs=[],
                    status=RunStatus(state=state, start_time=None, end_time=None),
                )
                mock_wf_runs.append(wf_run)

            monkeypatch.setattr(
                sdk, "list_workflow_runs", Mock(return_value=mock_wf_runs)
            )

            # Prevent RayRuntime from connecting to a real cluster.
            monkeypatch.setattr(_dag.RayRuntime, "startup", Mock())

            repo = _repos.WorkflowRunRepo()

            # When
            runs = repo.list_wf_runs(config, ws)

            # Then
            assert [run.id for run in runs] == stub_run_ids

        @staticmethod
        def test_list_wf_run_summaries(monkeypatch):
            # Given
            config = "ray"
            stub_run_ids = ["wf.1", "wf.2"]
            state = State("RUNNING")
            mock_wf_run_summaries = []
            owner = "owner"
            total_task_runs = 99
            completed_task_runs = 1

            for stub_id in stub_run_ids:
                wf_run = WorkflowRunSummary(
                    id=stub_id,
                    status=RunStatus(state=state, start_time=None, end_time=None),
                    owner=owner,
                    total_task_runs=total_task_runs,
                    completed_task_runs=completed_task_runs,
                    dry_run=None,
                )
                mock_wf_run_summaries.append(wf_run)

            monkeypatch.setattr(
                sdk,
                "list_workflow_run_summaries",
                Mock(return_value=mock_wf_run_summaries),
            )

            # Prevent RayRuntime from connecting to a real cluster.
            monkeypatch.setattr(_dag.RayRuntime, "startup", Mock())

            repo = _repos.WorkflowRunRepo()

            # When
            runs = repo.list_wf_run_summaries(config)

            # Then
            assert [run.id for run in runs] == stub_run_ids, runs[0]

        class TestWithInProcess:
            """
            Uses sample workflow definition and in-process runtime to acquire a
            status model for tests.
            """

            @staticmethod
            @pytest.fixture(scope="class")
            def example_wf_run():
                @sdk.task(source_import=sdk.InlineImport())
                def fn1():
                    return 1

                @sdk.task(source_import=sdk.InlineImport())
                def fn2():
                    return 2

                @sdk.workflow
                def my_wf():
                    art1 = fn1()
                    art2_1 = fn2()
                    art2_2 = fn2()

                    return art1, art2_1, art2_2

                run = my_wf().run("in_process")

                return run

            @staticmethod
            def test_get_task_fn_names(monkeypatch, example_wf_run: sdk.WorkflowRun):
                # Given
                config = "<config sentinel>"
                repo = _repos.WorkflowRunRepo()

                # Mocks
                monkeypatch.setattr(
                    sdk.WorkflowRun, "by_id", Mock(return_value=example_wf_run)
                )

                # When
                names = repo.get_task_fn_names(example_wf_run.run_id, config)

                # Then
                assert names == ["fn1", "fn2"]

            @staticmethod
            def test_get_task_inv_ids(monkeypatch, example_wf_run: sdk.WorkflowRun):
                # Given
                config = "<config sentinel>"
                repo = _repos.WorkflowRunRepo()

                # Mocks
                monkeypatch.setattr(
                    sdk.WorkflowRun, "by_id", Mock(return_value=example_wf_run)
                )

                # When
                inv_ids = repo.get_task_inv_ids(
                    wf_run_id=example_wf_run.run_id,
                    config_name=config,
                    task_fn_name="fn2",
                )

                # Then
                assert len(inv_ids) == 2


class TestSummaryRepo:
    @staticmethod
    @pytest.mark.parametrize(
        "wf_run,expected_summary",
        [
            pytest.param(
                WorkflowRunModel(
                    id="wf.2",
                    workflow_def=_example_wfs.complicated_wf().model,
                    task_runs=[],
                    status=RunStatus(
                        state=State.WAITING, start_time=None, end_time=None
                    ),
                ),
                ui_models.WFRunSummary(
                    wf_def_name="complicated_wf",
                    wf_run_id="wf.2",
                    wf_run_status=RunStatus(
                        state=State.WAITING, start_time=None, end_time=None
                    ),
                    task_rows=[],
                    n_tasks_succeeded=0,
                    n_task_invocations_total=4,
                ),
                id="waiting",
            ),
            pytest.param(
                WorkflowRunModel(
                    id="wf.2",
                    workflow_def=_example_wfs.complicated_wf().model,
                    task_runs=[
                        TaskRunModel(
                            id="task_run_1",
                            invocation_id="invocation-1-task-capitalize",
                            status=RunStatus(
                                state=State.SUCCEEDED,
                                start_time=INSTANT_1,
                                end_time=INSTANT_2,
                            ),
                        ),
                        TaskRunModel(
                            id="task_run_2",
                            invocation_id="invocation-2-task-concat",
                            status=RunStatus(
                                state=State.RUNNING, start_time=INSTANT_2, end_time=None
                            ),
                        ),
                    ],
                    status=RunStatus(
                        state=State.RUNNING, start_time=INSTANT_1, end_time=None
                    ),
                ),
                ui_models.WFRunSummary(
                    wf_def_name="complicated_wf",
                    wf_run_id="wf.2",
                    wf_run_status=RunStatus(
                        state=State.RUNNING, start_time=INSTANT_1, end_time=None
                    ),
                    task_rows=[
                        ui_models.WFRunSummary.TaskRow(
                            task_fn_name="capitalize",
                            inv_id="invocation-1-task-capitalize",
                            status=RunStatus(
                                state=State.SUCCEEDED,
                                start_time=INSTANT_1,
                                end_time=INSTANT_2,
                            ),
                            message=None,
                        ),
                        ui_models.WFRunSummary.TaskRow(
                            task_fn_name="concat",
                            inv_id="invocation-2-task-concat",
                            status=RunStatus(
                                state=State.RUNNING,
                                start_time=INSTANT_2,
                                end_time=None,
                            ),
                            message=None,
                        ),
                    ],
                    n_tasks_succeeded=1,
                    n_task_invocations_total=4,
                ),
                id="running",
            ),
        ],
    )
    def test_wf_run_summary(
        wf_run: WorkflowRunModel, expected_summary: ui_models.WFRunSummary
    ):
        # Given
        repo = _repos.SummaryRepo()

        # When
        result_summary = repo.wf_run_summary(wf_run)

        # Then
        assert result_summary == expected_summary

    @staticmethod
    @pytest.mark.parametrize(
        "wf_run,expected_summary",
        [
            pytest.param(
                [
                    WorkflowRunSummary(
                        id="wf.2",
                        status=RunStatus(
                            state=State.RUNNING,
                            start_time=INSTANT_1 + datetime.timedelta(seconds=30),
                            end_time=None,
                        ),
                        owner="taylor.swift@zapatacomputing.com",
                        total_task_runs=2,
                        completed_task_runs=1,
                        dry_run=False,
                    ),
                    WorkflowRunSummary(
                        id="wf.1",
                        status=RunStatus(
                            state=State.WAITING, start_time=INSTANT_1, end_time=None
                        ),
                        owner="taylor.swift@zapatacomputing.com",
                        total_task_runs=0,
                        completed_task_runs=0,
                        dry_run=False,
                    ),
                ],
                ui_models.WFList(
                    wf_rows=[
                        ui_models.WFList.WFRow(
                            workflow_run_id="wf.1",
                            status="WAITING",
                            tasks_succeeded="0/0",
                            start_time=INSTANT_1,
                            owner="taylor.swift@zapatacomputing.com",
                        ),
                        ui_models.WFList.WFRow(
                            workflow_run_id="wf.2",
                            status="RUNNING",
                            tasks_succeeded="1/2",
                            start_time=INSTANT_1 + datetime.timedelta(seconds=30),
                            owner="taylor.swift@zapatacomputing.com",
                        ),
                    ],
                ),
            ),
            pytest.param(
                [
                    WorkflowRunModel(
                        id="wf.2",
                        workflow_def=_example_wfs.complicated_wf().model,
                        task_runs=[],
                        status=RunStatus(
                            state=State.RUNNING,
                            start_time=INSTANT_1 + datetime.timedelta(seconds=30),
                            end_time=None,
                        ),
                    ),
                    WorkflowRunModel(
                        id="wf.1",
                        workflow_def=_example_wfs.complicated_wf().model,
                        task_runs=[],
                        status=RunStatus(
                            state=State.WAITING, start_time=None, end_time=None
                        ),
                    ),
                ],
                ui_models.WFList(
                    wf_rows=[
                        ui_models.WFList.WFRow(
                            workflow_run_id="wf.1",
                            status="WAITING",
                            tasks_succeeded="0/0",
                            start_time=None,
                        ),
                        ui_models.WFList.WFRow(
                            workflow_run_id="wf.2",
                            status="RUNNING",
                            tasks_succeeded="0/0",
                            start_time=INSTANT_1 + datetime.timedelta(seconds=30),
                        ),
                    ],
                ),
            ),
        ],
    )
    def test_wf_list_summary(
        wf_run: t.List[WorkflowRunSummary], expected_summary: ui_models.WFList
    ):
        # Given
        repo = _repos.SummaryRepo()

        # When
        result = repo.wf_list_summary(wf_run)

        # Then
        assert result == expected_summary


class TestConfigRepo:
    @pytest.fixture(autouse=True)
    def patch_token_checking(self, monkeypatch: pytest.MonkeyPatch):
        check = create_autospec(_repos.check_jwt_without_signature_verification)
        monkeypatch.setattr(_repos, "check_jwt_without_signature_verification", check)

    class TestUnit:
        """
        Test boundary::
            [ConfigRepo]->sdk._config

        """

        def test_list_config(self, monkeypatch):
            """
            Simple test that verifies that repo return all the configs returned by
            _configs internals
            """
            configs = ["config1", "config2"]
            monkeypatch.setattr(sdk.RuntimeConfig, "list_configs", lambda: configs)

            repo = _repos.ConfigRepo()

            # When
            names = repo.list_config_names()

            # Then
            assert names == configs

        def test_list_remote_config_names(self, monkeypatch):
            configs = ["config1", "config2"]
            monkeypatch.setattr(
                sdk.RuntimeConfig,
                "list_configs",
                lambda: configs + [key for key in SPECIAL_CONFIG_NAME_DICT],
            )

            repo = _repos.ConfigRepo()

            # When
            names = repo.list_remote_config_names()

            # Then
            assert names == configs

        @pytest.mark.parametrize("runtime_name", [RuntimeName.CE_REMOTE])
        def test_store_token(self, monkeypatch, runtime_name):
            repo = _repos.ConfigRepo()
            uri = "funny_uri"
            token = "even_funnier_token"
            generated_name = "why_is_it_so_funny"

            # Check parameters passed to _Config
            mock_save_or_update = Mock()

            monkeypatch.setattr(
                _config, "generate_config_name", lambda n, m: generated_name
            )

            monkeypatch.setattr(_config, "save_or_update", mock_save_or_update)

            # When
            config_name = repo.store_token_in_config(uri, token, runtime_name)

            # Then
            (
                config_parameter,
                runtime_parameter,
                options_parameter,
            ) = mock_save_or_update.call_args[0]

            assert config_parameter == generated_name
            assert runtime_parameter == runtime_name
            assert options_parameter["uri"] == uri
            assert options_parameter["token"] == token
            assert config_name == generated_name

    class TestIntegration:
        """
        We test ConfigRepo by integration because - config repo on its own is trivial
        but configs are quite fragile. It's important to make sure our CI is working
        with whatever changes are done at config level

        Test boundary::
            [ConfigRepo]->File system

        Mocks config file location.
        """

        @staticmethod
        @pytest.fixture
        def config_content():
            return TEST_CONFIG_JSON

        @staticmethod
        def test_returns_usable_configs(tmp_path: Path, monkeypatch, config_content):
            """
            Verifies that the output is a list that makes sense for the user to select
            the config value from.
            """
            # Given
            monkeypatch.setattr(Path, "home", Mock(return_value=tmp_path))

            config_path = tmp_path / ".orquestra" / "config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(config_content))

            repo = _repos.ConfigRepo()

            # When
            names = repo.list_config_names()

            # Then
            assert set(names) == {
                # built-ins
                "ray",
                # config entries
                "test_config_default",
                "test_config_no_runtime_options",
                "test_config_ce",
                "actual_name",
                "proper_token",
                "improper_token",
            }

        @staticmethod
        @pytest.mark.parametrize("runtime_name", [RuntimeName.CE_REMOTE])
        @pytest.mark.parametrize(
            "uri, token, config_name",
            [
                ("http://name.domain", "funny_token", "name"),
                ("https://actual_name.domain", "new_token", "actual_name"),
            ],
            ids=[
                "Creating new config entry",
                "Updating existing config entry",
            ],
        )
        def test_update_config(
            tmp_path: Path,
            monkeypatch,
            config_content,
            uri,
            token,
            config_name,
            runtime_name,
        ):
            """
            Verifies that the output is a list that makes sense for the user to select
            the config value from.
            """
            # Given
            monkeypatch.setattr(Path, "home", Mock(return_value=tmp_path))

            config_path = tmp_path / ".orquestra" / "config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(config_content))
            repo = _repos.ConfigRepo()
            # this assert stands to protect the json content. For this test to work
            # it assumes that such config exist, and it matches parametrized values.
            assert (
                config_content["configs"]["actual_name"]["runtime_options"]["uri"]
                == "http://actual_name.domain"
            )

            # When
            repo.store_token_in_config(uri, token, runtime_name)

            # Then
            with open(config_path) as f:
                content = json.load(f)
                assert content["configs"][config_name]["runtime_options"]["uri"] == uri
                assert (
                    content["configs"][config_name]["runtime_options"]["token"] == token
                )
                assert (
                    content["configs"][config_name]["runtime_name"]
                    == runtime_name.value
                )


class TestRuntimeRepo:
    @pytest.mark.parametrize("runtime_name", [RuntimeName.CE_REMOTE])
    def test_return_valid_token(self, monkeypatch, runtime_name):
        # Given
        fake_login_url = "http://my_login.url"

        monkeypatch.setattr(
            DriverClient,
            "get_login_url",
            lambda x, _: fake_login_url,
        )

        repo = _repos.RuntimeRepo()

        # When
        login_url = repo.get_login_url("uri", runtime_name, 0)

        # Then
        assert login_url == fake_login_url

    @pytest.mark.parametrize("runtime_name", [RuntimeName.CE_REMOTE])
    @pytest.mark.parametrize(
        "exception", [requests.ConnectionError, requests.exceptions.MissingSchema]
    )
    def test_exceptions(self, monkeypatch, exception, runtime_name):
        # Given
        def _exception(_, __):
            raise exception

        monkeypatch.setattr(
            DriverClient,
            "get_login_url",
            _exception,
        )

        repo = _repos.RuntimeRepo()

        # Then
        with pytest.raises(exceptions.LoginURLUnavailableError):
            repo.get_login_url("uri", runtime_name, 0)


class TestResolveDottedName:
    """
    Unit tests for the heuristic for module name resolution.
    """

    @staticmethod
    @pytest.mark.parametrize(
        "spec,dotted_name",
        [
            ("foo", "foo"),
            ("foo.bar", "foo.bar"),
            ("foo.py", "foo"),
            (str(Path("foo") / "bar.py"), "foo.bar"),
            (str(Path("src") / "foo" / "bar.py"), "foo.bar"),
        ],
    )
    def test_examples(spec: str, dotted_name: str):
        assert _repos.resolve_dotted_name(spec) == dotted_name


class TestWorkflowDefRepoIntegration:
    """
    Integration tests for WorkflowDefRepo.

    Test boundary::

        [real testing module]->[WorkflowDefRepo]
    """

    @staticmethod
    @pytest.fixture
    def tmp_packages_site(tmp_path):
        """
        Prepares a directory for importing Python modules and cleans up the
        module cache afterwards.
        """
        with _reloaders.restore_loaded_modules():
            sys.path.insert(0, str(tmp_path))

            yield tmp_path

    class TestGetModuleFromSpec:
        class TestDottedName:
            """
            Validates that we can pass 'dotted.module.name' and it loads the file
            appropriately.
            """

            @staticmethod
            def test_loads_top_level_module(tmp_packages_site: Path):
                # Given
                module_path = tmp_packages_site / "my_module.py"
                module_path.write_text("foo = 'abc'")

                repo = _repos.WorkflowDefRepo()

                # When
                mod = repo.get_module_from_spec("my_module")

                # Then
                assert mod.foo == "abc"

            @staticmethod
            def test_loads_submodules(tmp_packages_site: Path):
                # Given
                module_path = tmp_packages_site / "my_pkg" / "my_module.py"
                module_path.parent.mkdir(parents=True)
                module_path.write_text("foo = 'abc'")

                repo = _repos.WorkflowDefRepo()

                # When
                mod = repo.get_module_from_spec("my_pkg.my_module")

                # Then
                assert mod.foo == "abc"

            @staticmethod
            def test_loads_from_cwd(tmp_path: Path, monkeypatch):
                """
                - We have some free-form project files outside of a setuptools-like
                  distribution
                - PWD is at the project root
                - PWD wasn't added to sys.path explicitly
                """
                # Given
                proj_dir = tmp_path / "my_proj"
                proj_dir.mkdir()

                module_path = proj_dir / "my_pkg" / "my_module.py"
                module_path.parent.mkdir()
                module_path.write_text("foo = 'abc'")

                monkeypatch.chdir(proj_dir)

                repo = _repos.WorkflowDefRepo()

                with _reloaders.restore_loaded_modules():
                    # When
                    mod = repo.get_module_from_spec("my_pkg.my_module")

                    # Then
                    assert mod.foo == "abc"

        class TestNonExistingModules:
            @staticmethod
            def test_invalid_path():
                repo = _repos.WorkflowDefRepo()

                with pytest.raises(exceptions.WorkflowDefinitionModuleNotFound):
                    _ = repo.get_module_from_spec("doesnt_exist.py")

            @staticmethod
            def test_invalid_module():
                repo = _repos.WorkflowDefRepo()

                with pytest.raises(exceptions.WorkflowDefinitionModuleNotFound):
                    _ = repo.get_module_from_spec("doesnt_exist")

    class TestGetWorkflowNames:
        @staticmethod
        def test_examples_module():
            # Given
            repo = _repos.WorkflowDefRepo()

            # When
            names = repo.get_worklow_names(_example_wfs)

            # Then
            assert names == [
                "greet_wf",
                "greet_wf_kw",
                "complicated_wf",
                "multioutput_wf",
                "multioutput_task_wf",
                "multioutput_task_failed_wf",
                "my_workflow",
                "exception_wf",
                "wf_using_inline_imports",
                "wf_using_git_imports",
                "workflow_throwing_3rd_party_exception",
                "wf_using_python_imports",
                "serial_wf_with_slow_middle_task",
                "infinite_workflow",
                "serial_wf_with_file_triggers",
                "exception_wf_with_multiple_values",
                "wf_with_log",
                "wf_with_exec_ctx",
                "parametrized_wf",
                "wf_with_secrets",
                "workflow_parametrised_with_resources",
                "workflow_with_different_resources",
                "wf_with_explicit_n_outputs",
                "cause_env_setup_error",
            ]

        @staticmethod
        def test_empty_module(tmp_packages_site):
            # Given
            module_path = tmp_packages_site / "my_module.py"
            module_path.write_text("foo = 'abc'")
            repo = _repos.WorkflowDefRepo()
            module = repo.get_module_from_spec("my_module")

            # Then
            with pytest.raises(exceptions.NoWorkflowDefinitionsFound):
                # When
                _ = repo.get_worklow_names(module)

    class TestGetWorkflowDef:
        @staticmethod
        def test_standard_workflow():
            # Given
            repo = _repos.WorkflowDefRepo()
            wf_name = "greet_wf"

            # When
            wf_def = repo.get_workflow_def(_example_wfs, wf_name)

            # Then
            assert isinstance(wf_def, sdk.WorkflowDef)
            assert wf_def._name == wf_name

        @staticmethod
        def test_parametrized_workflow():
            # Given
            repo = _repos.WorkflowDefRepo()
            wf_name = "parametrized_wf"

            # Then
            with pytest.raises(exceptions.WorkflowSyntaxError):
                # When
                _ = repo.get_workflow_def(_example_wfs, wf_name)


class TestSpacesRepo:
    class TestUnit:
        """
        Test boundary::
            [SpacesRepo]->sdk._config

        """

        def test_list_workspaces(self, monkeypatch):
            """
            Simple test that verifies that repo return all the workspaces returned by
            sdk
            """
            spaces = ["ws1", "ws2"]

            monkeypatch.setattr(sdk, "list_workspaces", lambda _: spaces)

            repo = _repos.SpacesRepo()

            # When
            names = repo.list_workspaces("config")

            # Then
            assert names == spaces

        def test_list_projects(self, monkeypatch):
            """
            Simple test that verifies that repo return all the projects
            returned by
            sdk
            """
            projects = ["p1", "p2"]

            monkeypatch.setattr(sdk, "list_projects", lambda *_: projects)

            repo = _repos.SpacesRepo()

            # When
            names = repo.list_projects("config", "workspace")

            # Then
            assert names == projects
