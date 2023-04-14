################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import base64
import io
import json
import os
import tarfile
import typing as t
from pathlib import Path
from unittest.mock import Mock

import pytest
import pytest_httpserver

import orquestra.sdk as sdk
from orquestra.sdk._base._testing import _connections


# region: workflow definition
@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=[
        sdk.GitImport(
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",  # noqa: E501
            git_ref="main",
        ),
    ],
)
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

# region: HTTP mocking
QE_MINIMAL_CURRENT_REPRESENTATION: t.Dict[str, t.Any] = {
    "status": {
        "phase": "Succeeded",
        "startedAt": "1989-12-13T09:03:49Z",
        "finishedAt": "1989-12-13T09:05:14Z",
        "nodes": {},
    },
}
QE_STATUS_RESPONSE = {
    "id": "wf-id-sentinel",
    "status": "Succeeded",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION).encode()
    ).decode(),
    "completed": True,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}
QE_WORKFLOW_RESULT_JSON_DICT_SINGLE = {
    "wf-id-sentinel-2738763496": {
        "artifact-3-get-list": {
            "serialization_format": "JSON",
            "value": "[1,2,3]",
        },
        "inputs": {},
        "stepID": "wf-id-sentinel-2738763496",
        "stepName": "invocation-0-task-get-list",
        "workflowId": "wf-id-sentinel",
    },
}
QE_WORKFLOW_RESULT_JSON_DICT_MUTLIPLE = {
    "wf-id-sentinel-foo": {
        "artifact-3-get-list": {
            "serialization_format": "JSON",
            "value": "[1,2,3]",
        },
        "inputs": {},
        "stepID": "wf-id-sentinel-foo",
        "stepName": "invocation-0-task-get-list",
        "workflowId": "wf-id-sentinel",
    },
    "wf-id-sentinel-bar": {
        "artifact-7-get-list": {
            "serialization_format": "JSON",
            "value": "[1,2,3]",
        },
        "inputs": {},
        "stepID": "wf-id-sentinel-bar",
        "stepName": "invocation-1-task-get-list",
        "workflowId": "wf-id-sentinel",
    },
}


def _make_result_bytes(results_dict) -> bytes:
    results_file_bytes = json.dumps(results_dict).encode()

    tar_buf = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=tar_buf) as tar:
        # See this for creating tars in memory:
        # https://github.com/python/cpython/issues/66404#issuecomment-1093662423
        tar_info = tarfile.TarInfo("results.json")
        tar_info.size = len(results_file_bytes)
        tar.addfile(tar_info, fileobj=io.BytesIO(results_file_bytes))

    tar_buf.seek(0)
    return tar_buf.read()


# endregion


# region: fixtures
@pytest.fixture(scope="module")
def ray():
    with _connections.make_ray_conn() as ray_params:
        yield ray_params


@pytest.fixture
def define_test_config(
    httpserver: pytest_httpserver.HTTPServer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from orquestra import sdk

    monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_path / "config.json"))
    sdk.RuntimeConfig.qe(uri=f"http://127.0.0.1:{httpserver.port}", token="nice")


@pytest.fixture(scope="module")
def change_test_dir(tmp_path_factory, request):
    project_dir = tmp_path_factory.mktemp("project")
    os.chdir(project_dir)
    yield project_dir
    os.chdir(request.config.invocation_dir)


# endregion


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_consistent_returns_from_get_results(
    patch_config_location,
    ray,
    monkeypatch,
    tmp_path,
    mock_workflow_db_location,
    httpserver: pytest_httpserver.HTTPServer,
    define_test_config,
    change_test_dir,
):
    # GIVEN
    # Mocking for Ray
    monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
    # Mocking for QE
    httpserver.expect_request("/v1/workflows").respond_with_data("wf_id_sentinel_2")
    httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
    httpserver.expect_request(
        "/v2/workflows/wf_id_sentinel_2/result"
    ).respond_with_data(
        _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_SINGLE),
        content_type="application/x-gtar-compressed",
    )

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
    results_remote = (
        wf_return_single_packed_value()
        .run(sdk.RuntimeConfig.load("127"))
        .get_results(wait=True)
    )

    # THEN
    assert results_in_process == results_ray
    assert results_in_process == results_remote
    assert isinstance(results_in_process, list)
    assert results_in_process == [1, 2, 3]


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_handles_multiple_returns(
    patch_config_location,
    ray,
    monkeypatch,
    tmp_path,
    mock_workflow_db_location,
    httpserver: pytest_httpserver.HTTPServer,
    define_test_config,
    change_test_dir,
):
    # GIVEN
    # Mocking for Ray
    monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
    # Mocking for QE
    httpserver.expect_request("/v1/workflows").respond_with_data("wf_id_sentinel")
    httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
    httpserver.expect_request("/v2/workflows/wf_id_sentinel/result").respond_with_data(
        _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_MUTLIPLE),
        content_type="application/x-gtar-compressed",
    )

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
    results_remote = (
        wf_return_multiple_packed_values()
        .run(sdk.RuntimeConfig.load("127"))
        .get_results(wait=True)
    )

    # THEN
    assert results_in_process == results_ray
    assert results_in_process == results_remote
    assert isinstance(results_in_process, tuple)
    assert results_in_process == ([1, 2, 3], [1, 2, 3])
