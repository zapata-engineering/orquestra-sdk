################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from pathlib import Path
from unittest.mock import Mock

import pytest

import orquestra.sdk as sdk
from orquestra.sdk._base._testing import _connections


# region: workflow definition
@sdk.task
def get_list():
    return [1, 2, 3]


@sdk.workflow
def wf_return_single_packed_value():
    a = get_list()
    return a


@sdk.workflow
def wf_return_multiple_packed_values():
    a = get_list()
    b = get_list()
    return a, b


# endregion


# region: fixtures
@pytest.fixture(scope="module")
def ray():
    with _connections.make_ray_conn() as ray_params:
        yield ray_params


# endregion


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_consistent_returns_from_get_results(
    patch_config_location, ray, monkeypatch, tmp_path, mock_workflow_db_location
):
    # GIVEN
    # Mocking for Ray
    monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

    # WHEN
    results_in_process = (
        wf_return_single_packed_value()
        .run(sdk.RuntimeConfig.in_process())
        .get_results(wait=True)
    )
    results_ray = (
        wf_return_single_packed_value()
        .run(sdk.RuntimeConfig.ray())
        .get_results(wait=True)
    )

    # THEN
    assert results_in_process == results_ray
    assert isinstance(results_in_process, list)
    assert results_in_process == [1, 2, 3]


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_handles_multiple_returns(
    patch_config_location, ray, monkeypatch, tmp_path, mock_workflow_db_location
):
    # GIVEN
    # Mocking for Ray
    monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

    # WHEN
    results_in_process = (
        wf_return_multiple_packed_values()
        .run(sdk.RuntimeConfig.in_process())
        .get_results(wait=True)
    )
    results_ray = (
        wf_return_multiple_packed_values()
        .run(sdk.RuntimeConfig.ray())
        .get_results(wait=True)
    )

    # THEN
    assert results_in_process == results_ray
    assert isinstance(results_in_process, tuple)
    assert results_in_process == ([1, 2, 3], [1, 2, 3])
