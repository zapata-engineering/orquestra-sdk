################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from pathlib import Path
from unittest.mock import Mock

import pytest
from orquestra.workflow_runtime._testing import _connections

import orquestra.sdk as sdk

# Ray mishandles log file handlers and we get "_io.FileIO [closed]"
# unraisable exceptions. Last tested with Ray 2.4.0.
pytestmark = pytest.mark.filterwarnings(
    "ignore::pytest.PytestUnraisableExceptionWarning"
)


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
        def test_single_run(patch_config_location, ray, monkeypatch, tmp_path):
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

            run = wf_pass_tuple().run(sdk.RuntimeConfig.ray())
            run.wait_until_finished()
            results = run.get_results()

            assert results == 3

    class TestStartFromIR:
        @staticmethod
        def test_single_run(patch_config_location, ray, monkeypatch, tmp_path):
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
            run = sdk.WorkflowRun.start_from_ir(
                wf_pass_tuple().model, sdk.RuntimeConfig.ray()
            )
            run.wait_until_finished()
            results = run.get_results()

            assert results == 3

    class TestReconnectToPreviousRun:
        @staticmethod
        def test_custom_save_locations(
            patch_config_location,
            ray,
            tmp_path,
            monkeypatch,
        ):
            # GIVEN
            monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
            monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_path / "config.json"))

            run = wf_pass_tuple().run(sdk.RuntimeConfig.ray())
            run.wait_until_finished()

            id = run.run_id
            del run

            # WHEN
            run_reconnect = sdk.WorkflowRun.by_id(
                id,
            )
            results = run_reconnect.get_results()

            # THEN
            assert results == 3
