################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Unit tests for orquestra.sdk._ray._dag. If you need a test against a live
Ray connection, see tests/ray/test_integration.py instead.
"""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra.sdk._base._config import RuntimeConfiguration, RuntimeName
from orquestra.sdk._ray import _client, _dag
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

    class TestReadingLogs:
        """
        Verifies that RayRuntime gets whatever DirectRayReader or FluentbitReader
        produced.

        Test boundary: [RayRuntime]─┬[ServiceManager]
                                    ├[DirectRayReader]
                                    └[FluentbitReader]
        """

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_attr_to_mock,fluentbit_running",
            [
                ("_ray_reader", False),
                ("_fluentbit_reader", True),
            ],
        )
        def test_get_full_logs(
            runtime_attr_to_mock: str,
            fluentbit_running: bool,
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
            rt._service_manager = Mock()
            rt._service_manager.is_fluentbit_running.return_value = fluentbit_running

            reader_mock = Mock()
            logs_dict = {"inv_id1": ["Hello, there!", "General Kenobi!"]}
            reader_mock.get_full_logs.return_value = logs_dict
            setattr(rt, runtime_attr_to_mock, reader_mock)

            run_id = "wf_or_task_run_id"

            # When
            result_dict = rt.get_full_logs(run_id=run_id)

            # Then
            assert result_dict == logs_dict
            reader_mock.get_full_logs.assert_called_with(run_id)

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_attr_to_mock,fluentbit_running",
            [
                ("_ray_reader", False),
                ("_fluentbit_reader", True),
            ],
        )
        def test_iter_logs(
            runtime_attr_to_mock: str,
            fluentbit_running: bool,
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
            rt._service_manager = Mock()
            rt._service_manager.is_fluentbit_running.return_value = fluentbit_running

            reader_mock = Mock()
            logs_batch = ["Hello, there!", "General Kenobi!"]
            reader_mock.iter_logs.return_value = [logs_batch]
            setattr(rt, runtime_attr_to_mock, reader_mock)

            run_id = "wf_or_task_run_id"

            # When
            result_batch = next(rt.iter_logs(run_id=run_id))

            # Then
            assert result_batch == logs_batch
            reader_mock.iter_logs.assert_called_with(run_id)
