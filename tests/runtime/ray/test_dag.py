################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Unit tests for orquestra.sdk._ray._dag. If you need a test against a live
Ray connection, see tests/ray/test_integration.py instead.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, PropertyMock, create_autospec

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base import _services
from orquestra.sdk._base._config import RuntimeConfiguration, RuntimeName
from orquestra.sdk._ray import _client, _dag, _ray_logs
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import State

TEST_TIME = datetime.now(timezone.utc)


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


class TestTupleUnwrapper:
    def test_unwraps_positional(self):
        """
        Verifies that TupleUnwrapper.call() unwraps a tuple passed as a
        positional argument.
        """
        # Given
        fn = Mock()
        unwrapper = _dag.TupleUnwrapper(
            fn=fn,
            pos_specs=[_dag.PosArgUnpackSpec(param_index=0, unpack_index=0)],
            kw_specs=[],
        )

        # When
        unwrapper(("foo", "bar"))

        # Then
        fn.assert_called_with("foo")

    def test_unwraps_keyword(self):
        """
        Verifies that TupleUnwrapper.call() unwraps a tuple passed as a
        positional argument.
        """
        # Given
        fn = Mock()
        unwrapper = _dag.TupleUnwrapper(
            fn=fn,
            pos_specs=[],
            kw_specs=[_dag.KwArgUnpackSpec(param_name="qux", unpack_index=1)],
        )

        # When
        unwrapper(qux=("foo", "bar"))

        # Then
        fn.assert_called_with(qux="bar")

    def test_leaves_unspecified_values(self):
        """
        Verifies that TupleUnwrapper.call() doesn't change arguments that
        aren't mentioned in pos_specs or kw_specs.
        """
        # Given
        fn = Mock()
        unwrapper = _dag.TupleUnwrapper(
            fn=fn,
            pos_specs=[_dag.PosArgUnpackSpec(param_index=0, unpack_index=0)],
            kw_specs=[_dag.KwArgUnpackSpec(param_name="qux", unpack_index=1)],
        )

        # When
        unwrapper(
            ("foo1", "bar1"),
            ("foo2", "bar2"),
            baz=("foo3", "bar3"),
            qux=("foo4", "bar4"),
        )

        # Then
        fn.assert_called_with(
            "foo1",
            ("foo2", "bar2"),
            baz=("foo3", "bar3"),
            qux="bar4",
        )


class TestRayRuntime:
    """
    Unit tests for RayRuntime class. Shouldn't use a real Ray connection nor other
    background services.
    """

    @staticmethod
    @pytest.fixture
    def runtime_config():
        return RuntimeConfiguration(
            config_name="TestRayRuntime",
            runtime_name=RuntimeName.RAY_LOCAL,
        )

    @staticmethod
    @pytest.fixture
    def client():
        client = Mock()
        return client

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
                    datetime.now(timezone.utc) - timedelta(seconds=5),
                    datetime.now(timezone.utc) - timedelta(seconds=5),
                    datetime.now(timezone.utc) - timedelta(days=4),
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
                    datetime.now(timezone.utc) - timedelta(seconds=5),
                    datetime.now(timezone.utc) - timedelta(seconds=5),
                    datetime.now(timezone.utc) - timedelta(days=4),
                ]
            )
            monkeypatch.setattr(
                runtime, "get_workflow_run_status", Mock(return_value=mock_status)
            )
            # When
            runs = runtime.list_workflow_runs(limit=2)
            # Then
            assert len(runs) == 2


class TestWrapSingleOutputs:
    @staticmethod
    @pytest.mark.parametrize(
        "values,invocations,expected_wrapped",
        [
            pytest.param(
                [],
                [],
                [],
                id="empty",
            ),
            pytest.param(
                [42],
                [
                    ir.TaskInvocation(
                        id="inv1",
                        task_id="task1",
                        args_ids=[],
                        kwargs_ids=[],
                        output_ids=["art1"],
                        resources=None,
                        custom_image=None,
                    ),
                ],
                [(42,)],
                id="single_task_single_output",
            ),
            pytest.param(
                [(21, 38)],
                [
                    ir.TaskInvocation(
                        id="inv1",
                        task_id="task1",
                        args_ids=[],
                        kwargs_ids=[],
                        output_ids=["art1", "art2"],
                        resources=None,
                        custom_image=None,
                    ),
                ],
                [(21, 38)],
                id="single_task_multi_output",
            ),
            pytest.param(
                [
                    (21, 38),
                    42,
                ],
                [
                    ir.TaskInvocation(
                        id="inv1",
                        task_id="task1",
                        args_ids=[],
                        kwargs_ids=[],
                        output_ids=["art1", "art2"],
                        resources=None,
                        custom_image=None,
                    ),
                    ir.TaskInvocation(
                        id="inv2",
                        task_id="task2",
                        args_ids=[],
                        kwargs_ids=[],
                        output_ids=["art3"],
                        resources=None,
                        custom_image=None,
                    ),
                ],
                [
                    (21, 38),
                    (42,),
                ],
                id="many_tasks",
            ),
        ],
    )
    def test_foo(values, invocations, expected_wrapped):
        # When
        wrapped = _dag._wrap_single_outputs(values, invocations)
        # Then
        assert wrapped == expected_wrapped


class TestPipString:
    class TestPythonImports:
        def test_empty(self):
            imp = ir.PythonImports(id="mock-import", packages=[], pip_options=[])
            pip = _dag._pip_string(imp)
            assert pip == []

        def test_with_package(self, monkeypatch: pytest.MonkeyPatch):
            # We're not testing the serde package, so we're mocking it
            monkeypatch.setattr(
                _dag.serde, "stringify_package_spec", Mock(return_value="mocked")
            )
            imp = ir.PythonImports(
                id="mock-import",
                packages=[
                    ir.PackageSpec(
                        name="one",
                        extras=[],
                        version_constraints=[],
                        environment_markers="",
                    )
                ],
                pip_options=[],
            )
            pip = _dag._pip_string(imp)
            assert pip == ["mocked"]

        def test_with_two_packages(self, monkeypatch: pytest.MonkeyPatch):
            # We're not testing the serde package, so we're mocking it
            monkeypatch.setattr(
                _dag.serde, "stringify_package_spec", Mock(return_value="mocked")
            )
            imp = ir.PythonImports(
                id="mock-import",
                packages=[
                    ir.PackageSpec(
                        name="one",
                        extras=[],
                        version_constraints=[],
                        environment_markers="",
                    ),
                    ir.PackageSpec(
                        name="one",
                        extras=["extra"],
                        version_constraints=["version"],
                        environment_markers="env marker",
                    ),
                ],
                pip_options=[],
            )
            pip = _dag._pip_string(imp)
            assert pip == ["mocked", "mocked"]

    class TestGitImports:
        @pytest.fixture
        def patch_env(self, monkeypatch: pytest.MonkeyPatch):
            monkeypatch.setenv("ORQ_RAY_DOWNLOAD_GIT_IMPORTS", "1")

        def test_http(self, patch_env):
            imp = ir.GitImport(
                id="mock-import", repo_url="https://mock/mock/mock", git_ref="mock"
            )
            pip = _dag._pip_string(imp)
            assert pip == ["git+https://mock/mock/mock@mock"]

        def test_pip_ssh_format(self, patch_env):
            imp = ir.GitImport(
                id="mock-import", repo_url="ssh://git@mock/mock/mock", git_ref="mock"
            )
            pip = _dag._pip_string(imp)
            assert pip == ["git+ssh://git@mock/mock/mock@mock"]

        def test_usual_ssh_format(self, patch_env):
            imp = ir.GitImport(
                id="mock-import", repo_url="git@mock:mock/mock", git_ref="mock"
            )
            pip = _dag._pip_string(imp)
            assert pip == ["git+ssh://git@mock/mock/mock@mock"]

        def test_no_env_set(self):
            imp = ir.GitImport(
                id="mock-import", repo_url="git@mock:mock/mock", git_ref="mock"
            )
            pip = _dag._pip_string(imp)
            assert pip == []

    class TestOtherImports:
        def test_local_import(self):
            imp = ir.LocalImport(id="mock-import")
            pip = _dag._pip_string(imp)
            assert pip == []

        def test_inline_import(self):
            imp = ir.InlineImport(id="mock-import")
            pip = _dag._pip_string(imp)
            assert pip == []
