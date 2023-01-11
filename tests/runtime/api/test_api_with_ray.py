################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from pathlib import Path
from unittest.mock import Mock

import pytest

import orquestra.sdk as sdk
from orquestra.sdk._base import _api
from orquestra.sdk._base._testing import _connections
from orquestra.sdk._ray import _dag


@sdk.task
def sum_tuple_numbers(numbers: tuple):
    return sum(numbers)


@sdk.workflow
def wf_pass_tuple():
    two_nums = (1, 2)
    return [sum_tuple_numbers(two_nums)]


@pytest.fixture(scope="module")
def ray():
    with _connections.make_ray_conn() as ray_params:
        yield ray_params


@pytest.mark.slow
class TestRunningLocalInBackground:
    """
    Tests the public Python API for running workflows in the background, using the
    "local" runtime.

    These tests in combination with those in tests/test_api.py::TestRunningInProcess
    exhibit the behaviour discussed in
    https://zapatacomputing.atlassian.net/browse/ORQSDK-485.
    """

    class TestTwoStepForm:
        @staticmethod
        def test_single_run(
            patch_config_location, ray, monkeypatch, tmp_path, mock_workflow_db_location
        ):
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

            run = wf_pass_tuple().prepare(sdk.RuntimeConfig.ray())
            run.start()
            run.wait_until_finished()
            results = run.get_results()

            assert results == (3,)

        @staticmethod
        def test_multiple_starts(
            patch_config_location, ray, monkeypatch, tmp_path, mock_workflow_db_location
        ):
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

            run = wf_pass_tuple().prepare(sdk.RuntimeConfig.ray())

            run.start()
            run.wait_until_finished()
            results1 = run.get_results()

            run.start()
            run.wait_until_finished()
            results2 = run.get_results()

            assert results1 == results2

    class TestShorthand:
        @staticmethod
        def test_single_run(
            patch_config_location, ray, monkeypatch, tmp_path, mock_workflow_db_location
        ):
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

            run = wf_pass_tuple().run(sdk.RuntimeConfig.ray())
            run.wait_until_finished()
            results = run.get_results()

            assert results == (3,)

    class TestReconnectToPreviousRun:
        @staticmethod
        def test_custom_save_locations(
            patch_config_location,
            ray,
            tmp_path,
            monkeypatch,
            mock_workflow_db_location,
        ):
            # GIVEN
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

            run = wf_pass_tuple().run(sdk.RuntimeConfig.ray())
            run.wait_until_finished()

            id = run.run_id
            del run

            # WHEN
            run_reconnect = sdk.WorkflowRun.by_id(
                id,
                config_save_file=tmp_path / "config.json",
            )
            results = run_reconnect.get_results()

            # THEN
            assert results == (3,)
