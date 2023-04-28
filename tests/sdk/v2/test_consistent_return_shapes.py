################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Tests to confirm that our various runtimes give consistently shaped outputs.

These tests cover the behaviour in the context of both the API and CLI aim to treat the
runtimes as uniformly as possible.

Runtimes tested:
- In process
- ray local
- CE
- QE

Outputs tested:
- WF results
- artifacts
- task outputs
"""

import base64
import inspect
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import typing as t
from pathlib import Path
from unittest import mock

import pytest
import pytest_httpserver

import orquestra.sdk as sdk
from orquestra.sdk._base._testing import _connections
from orquestra.sdk.schema.workflow_run import RunStatus, TaskRun

from .driver import resp_mocks

# TODO: unify wf definitions.


# region: workflow definition
@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=[
        sdk.GitImport(
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
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


ORQUESTRA_IMPORT = "import orquestra.sdk as sdk"
TASK_DEF = inspect.getsource(get_list)
WORKFLOW_DEF_SINGLE = "\n".join(
    [ORQUESTRA_IMPORT, TASK_DEF, inspect.getsource(wf_return_single_packed_value)]
)
WORKFLOW_DEF_MULTIPLE = "\n".join(
    [ORQUESTRA_IMPORT, TASK_DEF, inspect.getsource(wf_return_multiple_packed_values)]
)

# endregion

# region: QE mocking
QE_MINIMAL_CURRENT_REPRESENTATION_SINGLE: t.Dict[str, t.Any] = {
    "status": {
        "phase": "Succeeded",
        "startedAt": "1989-12-13T09:03:49Z",
        "finishedAt": "1989-12-13T09:05:14Z",
        "nodes": {
            "foo": {
                "id": "wf-id-sentinel",
                "name": "bar",
                "displayName": "foobar",
                "type": "boo",
                "templateName": "invocation-0-task-get-list",
                "phase": "Succeeded",
                "boundaryID": "far",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:14Z",
            }
        },
    },
}
QE_MINIMAL_CURRENT_REPRESENTATION_MULTIPLE: t.Dict[str, t.Any] = {
    "status": {
        "phase": "Succeeded",
        "startedAt": "1989-12-13T09:03:49Z",
        "finishedAt": "1989-12-13T09:05:14Z",
        "nodes": {
            "foo": {
                "id": "wf-id-sentinel",
                "name": "bar",
                "displayName": "foobar",
                "type": "boo",
                "templateName": "invocation-0-task-get-list",
                "phase": "Succeeded",
                "boundaryID": "far",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:14Z",
            },
            "boofar": {
                "id": "wf-id-sentinel",
                "name": "bar",
                "displayName": "foobar",
                "type": "boo",
                "templateName": "invocation-1-task-get-list",
                "phase": "Succeeded",
                "boundaryID": "far",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:14Z",
            },
        },
    },
}


QE_STATUS_RESPONSE_SINGLE = {
    "id": "wf-id-sentinel",
    "status": "Succeeded",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION_SINGLE).encode()
    ).decode(),
    "completed": True,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}

QE_STATUS_RESPONSE_MULTIPLE = {
    "id": "wf-id-sentinel",
    "status": "Succeeded",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION_MULTIPLE).encode()
    ).decode(),
    "completed": True,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}

QE_WORKFLOW_RESULT_JSON_DICT_SINGLE = {
    "wf-id-sentinel-foobar": {
        "artifact-3-get-list": {
            "serialization_format": "JSON",
            "value": "[1,2,3]",
        },
        "inputs": {},
        "stepID": "wf-id-sentinel-foobar",
        "stepName": "invocation-0-task-get-list",
        "workflowId": "wf-id-sentinel",
    },
}
QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE = {
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
def mock_config_env_var(
    httpserver: pytest_httpserver.HTTPServer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    config_path = tmp_path / "config.json"
    monkeypatch.setenv("ORQ_CONFIG_PATH", str(config_path))
    with open(config_path, "x") as f:
        f.write(
            "{\n"
            '  "version": "0.0.2",\n'
            '  "configs": {\n'
            '    "CE": {\n'
            '    "config_name": "CE",\n'
            '    "runtime_name": "CE_REMOTE",\n'
            '    "runtime_options": {\n'
            '      "token": "nice",\n'
            f'      "uri": "http://127.0.0.1:{httpserver.port}"\n'
            "  }\n"
            "  },\n"
            '    "QE": {\n'
            '    "config_name": "QE",\n'
            '    "runtime_name": "QE_REMOTE",\n'
            '    "runtime_options": {\n'
            '      "token": "nice",\n'
            f'      "uri": "http://127.0.0.1:{httpserver.port}"\n'
            "    }\n"
            "  }\n"
            "},\n"
            '  "default_config_name": "local"\n'
            "}\n"
        )


@pytest.fixture
def mock_qe_run_single(httpserver: pytest_httpserver.HTTPServer) -> str:
    wf_id: str = "wf_id_sentinel_qe_single"

    httpserver.expect_request("/v1/workflows").respond_with_data(wf_id)
    httpserver.expect_request("/v1/workflow").respond_with_json(
        QE_STATUS_RESPONSE_SINGLE
    )
    httpserver.expect_request(f"/v2/workflows/{wf_id}/result").respond_with_data(
        _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_SINGLE),
        content_type="application/x-gtar-compressed",
    )
    httpserver.expect_request(
        f"/v2/workflows/{wf_id}/step/invocation-0-task-get-list/artifact/artifact-3-get-list"  # noqa: E501
    ).respond_with_data(
        _make_result_bytes(
            QE_WORKFLOW_RESULT_JSON_DICT_SINGLE["wf-id-sentinel-foobar"][
                "artifact-3-get-list"
            ]
        ),
        content_type="application/x-gtar-compressed",
    )
    return wf_id


@pytest.fixture
def mock_qe_run_multiple(httpserver: pytest_httpserver.HTTPServer) -> str:
    wf_id: str = "wf_id_sentinel_multiple"
    httpserver.expect_request("/v1/workflows").respond_with_data(wf_id)
    httpserver.expect_request("/v1/workflow").respond_with_json(
        QE_STATUS_RESPONSE_MULTIPLE
    )
    httpserver.expect_request(f"/v2/workflows/{wf_id}/result").respond_with_data(
        _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE),
        content_type="application/x-gtar-compressed",
    )
    httpserver.expect_request(
        f"/v2/workflows/{wf_id}/step/invocation-0-task-get-list/artifact/artifact-3-get-list"  # noqa: E501
    ).respond_with_data(
        _make_result_bytes(
            QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE["wf-id-sentinel-foo"][
                "artifact-3-get-list"
            ]
        ),
        content_type="application/x-gtar-compressed",
    )
    httpserver.expect_request(
        f"/v2/workflows/{wf_id}/step/invocation-0-task-get-list/artifact/artifact-7-get-list"  # noqa: E501
    ).respond_with_data(
        _make_result_bytes(
            QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE["wf-id-sentinel-bar"][
                "artifact-7-get-list"
            ]
        ),
        content_type="application/x-gtar-compressed",
    )
    httpserver.expect_request(
        f"/v2/workflows/{wf_id}/step/invocation-1-task-get-list/artifact/artifact-7-get-list"  # noqa: E501
    ).respond_with_data(
        _make_result_bytes(
            QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE["wf-id-sentinel-bar"][
                "artifact-7-get-list"
            ]
        ),
        content_type="application/x-gtar-compressed",
    )

    return wf_id


@pytest.fixture
def mock_ce_run_single(httpserver: pytest_httpserver.HTTPServer) -> str:
    wf_id: str = "wf_id_sentinel_ce_single"
    wf_def_id: str = "wf_def_ce_sentinel"
    result_ids = ["result_id_sentinel_0"]

    # region: Start workflow
    httpserver.expect_request("/api/workflow-definitions").respond_with_json(
        resp_mocks.make_get_wf_def_response(
            id_=wf_def_id, wf_def=wf_return_single_packed_value().model
        )
    )
    httpserver.expect_request("/api/workflow-runs").respond_with_json(
        resp_mocks.make_list_wf_run_response(
            ids=[wf_id],
            workflow_def_ids=[wf_def_id],
        )
    )
    httpserver.expect_request("/api/workflow-runs/definitionId").respond_with_json(
        resp_mocks.make_get_wf_run_response(
            id_=wf_id,
            workflow_def_id=wf_def_id,
            status=RunStatus(state="SUCCEEDED"),
            task_runs=[
                TaskRun(
                    id="foo",
                    invocation_id="invocation-0-task-get-list",
                    status=RunStatus(state="SUCCEEDED"),
                )
            ],
        )
    )
    httpserver.expect_request(f"/api/workflow-runs/{wf_id}").respond_with_json(
        resp_mocks.make_get_wf_run_response(
            id_=wf_id,
            workflow_def_id=wf_def_id,
            status=RunStatus(state="SUCCEEDED"),
            task_runs=[
                TaskRun(
                    id="foo",
                    invocation_id="invocation-0-task-get-list",
                    status=RunStatus(state="SUCCEEDED"),
                )
            ],
        )
    )
    httpserver.expect_request(
        f"/api/workflow-definitions/{wf_def_id}"
    ).respond_with_json(
        resp_mocks.make_get_wf_def_response(
            id_=wf_def_id, wf_def=wf_return_single_packed_value().model
        )
    )
    # endregion

    # Get results
    httpserver.expect_request("/api/run-results").respond_with_json(
        {"data": result_ids}
    )
    for result_id in result_ids:
        httpserver.expect_request(f"/api/run-results/{result_id}").respond_with_json(
            {
                "results": [
                    {"value": "[1, 2, 3]", "serialization_format": "JSON"},
                ]
            }
        )

    # Get artifacts
    httpserver.expect_request("/api/artifacts").respond_with_json(
        {"data": {"invocation-0-task-get-list": ["artifact-3-get-list"]}}
    )
    httpserver.expect_request("/api/artifacts/artifact-3-get-list").respond_with_json(
        resp_mocks.make_get_wf_run_artifact_response([1, 2, 3])
    )

    return wf_id


@pytest.fixture
def mock_ce_run_multiple(httpserver: pytest_httpserver.HTTPServer) -> str:
    wf_id: str = "wf_id_sentinel_ce_multiple"
    wf_def_id: str = "wf_def_ce_multiple_sentinel"
    result_ids = ["artifact-7-get-list", "artifact-3-get-list"]

    # region: Start workflow
    httpserver.expect_request("/api/workflow-definitions").respond_with_json(
        resp_mocks.make_get_wf_def_response(
            id_=wf_def_id, wf_def=wf_return_multiple_packed_values().model
        )
    )
    httpserver.expect_request("/api/workflow-runs").respond_with_json(
        resp_mocks.make_list_wf_run_response(
            ids=[wf_id],
            workflow_def_ids=[wf_def_id],
        )
    )
    httpserver.expect_request("/api/workflow-runs/definitionId").respond_with_json(
        resp_mocks.make_get_wf_run_response(
            id_=wf_id,
            workflow_def_id=wf_def_id,
            status=RunStatus(state="SUCCEEDED"),
            task_runs=[
                TaskRun(
                    id="foo",
                    invocation_id="invocation-0-task-get-list",
                    status=RunStatus(state="SUCCEEDED"),
                ),
                TaskRun(
                    id="foo",
                    invocation_id="invocation-1-task-get-list",
                    status=RunStatus(state="SUCCEEDED"),
                ),
            ],
        )
    )
    httpserver.expect_request(f"/api/workflow-runs/{wf_id}").respond_with_json(
        resp_mocks.make_get_wf_run_response(
            id_=wf_id,
            workflow_def_id=wf_def_id,
            status=RunStatus(state="SUCCEEDED"),
            task_runs=[
                TaskRun(
                    id="foo",
                    invocation_id="invocation-0-task-get-list",
                    status=RunStatus(state="SUCCEEDED"),
                ),
                TaskRun(
                    id="foo",
                    invocation_id="invocation-1-task-get-list",
                    status=RunStatus(state="SUCCEEDED"),
                ),
            ],
        )
    )
    httpserver.expect_request(
        f"/api/workflow-definitions/{wf_def_id}"
    ).respond_with_json(
        resp_mocks.make_get_wf_def_response(
            id_=wf_def_id, wf_def=wf_return_single_packed_value().model
        )
    )
    # endregion

    # Get results
    httpserver.expect_request("/api/run-results").respond_with_json(
        {"data": ["result_id"]}
    )
    for result_id in result_ids:
        httpserver.expect_request("/api/run-results/result_id").respond_with_json(
            {
                "results": [
                    {"value": "[1, 2, 3]", "serialization_format": "JSON"},
                    {"value": "[1, 2, 3]", "serialization_format": "JSON"},
                ]
            }
        )

    # Get artefacts
    httpserver.expect_request("/api/artifacts").respond_with_json(
        {
            "data": {
                "invocation-0-task-get-list": ["artifact-3-get-list"],
                "invocation-1-task-get-list": ["artifact-7-get-list"],
            }
        }
    )
    httpserver.expect_request("/api/artifacts/artifact-3-get-list").respond_with_json(
        resp_mocks.make_get_wf_run_artifact_response([1, 2, 3])
    )
    httpserver.expect_request("/api/artifacts/artifact-7-get-list").respond_with_json(
        resp_mocks.make_get_wf_run_artifact_response([1, 2, 3])
    )

    return wf_id


@pytest.fixture
def orq_project_dir_single():
    tmp_path = Path(tempfile.mkdtemp())
    tmp_path.joinpath("workflow_defs.py").write_text(WORKFLOW_DEF_SINGLE)
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield str(tmp_path)
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp_path)


@pytest.fixture
def orq_project_dir_multiple():
    tmp_path = Path(tempfile.mkdtemp())
    tmp_path.joinpath("workflow_defs.py").write_text(WORKFLOW_DEF_MULTIPLE)
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield str(tmp_path)
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp_path)


@pytest.fixture
def mock_db_env_var(tmp_path):
    with mock.patch.dict(os.environ, {"ORQ_DB_PATH": str(tmp_path / "workflows.db")}):
        yield


# endregion


@pytest.mark.usefixtures(
    "ray",
    "mock_config_env_var",
    "mock_db_env_var",
)
@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestAPI:
    @staticmethod
    def test_consistent_returns_for_single_value(
        mock_qe_run_single,
        mock_ce_run_single,
        orq_project_dir_single,
    ):
        # GIVEN
        ip_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.in_process())
        ray_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.ray())
        qe_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.load("QE"))
        ce_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.load("CE"))
        for run in [ip_run, ray_run, qe_run]:
            run.wait_until_finished()

        # WHEN
        results_ip = ip_run.get_results()
        results_ray = ray_run.get_results()
        results_qe = qe_run.get_results()
        results_ce = ce_run.get_results()

        artifacts_ip = ip_run.get_artifacts()
        artifacts_ray = ray_run.get_artifacts()
        artifacts_qe = qe_run.get_artifacts()
        artifacts_ce = ce_run.get_artifacts()

        task_outputs_ip = [t.get_outputs() for t in ip_run.get_tasks()]
        task_outputs_ray = [t.get_outputs() for t in ray_run.get_tasks()]
        task_outputs_qe = [t.get_outputs() for t in qe_run.get_tasks()]
        task_outputs_ce = [t.get_outputs() for t in ce_run.get_tasks()]

        # THEN
        assert results_ip == results_ray
        assert results_ip == results_qe
        assert results_ip == results_ce
        assert results_ip == [1, 2, 3]

        assert artifacts_ip == artifacts_ray
        assert artifacts_ip == artifacts_qe
        assert artifacts_ip == artifacts_ce
        assert artifacts_ip == {"invocation-0-task-get-list": [1, 2, 3]}

        assert task_outputs_ip == task_outputs_ray
        assert task_outputs_ip == task_outputs_qe
        assert task_outputs_ip == task_outputs_ce
        assert task_outputs_ip == [[1, 2, 3]]

    @staticmethod
    def test_consistent_returns_for_multiple_values(
        mock_qe_run_multiple,
        mock_ce_run_multiple,
        orq_project_dir_multiple,
    ):
        # GIVEN
        ip_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.in_process())
        ray_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.ray())
        qe_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.load("QE"))
        ce_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.load("CE"))

        for run in [ip_run, ray_run, qe_run]:
            run.wait_until_finished()

        # WHEN
        results_ip = ip_run.get_results()
        results_ray = ray_run.get_results()
        results_qe = qe_run.get_results()
        results_ce = ce_run.get_results()

        artifacts_ip = ip_run.get_artifacts()
        artifacts_ray = ray_run.get_artifacts()
        artifacts_qe = qe_run.get_artifacts()
        artifacts_ce = ce_run.get_artifacts()

        task_outputs_ip = [t.get_outputs() for t in ip_run.get_tasks()]
        task_outputs_ray = [t.get_outputs() for t in ray_run.get_tasks()]
        task_outputs_qe = [t.get_outputs() for t in qe_run.get_tasks()]
        task_outputs_ce = [t.get_outputs() for t in ce_run.get_tasks()]

        # THEN
        assert results_ip == results_ray
        assert results_ip == results_qe
        assert results_ip == results_ce
        assert results_ip == ([1, 2, 3], [1, 2, 3])

        assert artifacts_ip == artifacts_ray
        assert artifacts_ip == artifacts_qe
        assert artifacts_ip == artifacts_ce
        assert artifacts_ip == {
            "invocation-0-task-get-list": [1, 2, 3],
            "invocation-1-task-get-list": [1, 2, 3],
        }

        assert task_outputs_ip == task_outputs_ray
        assert task_outputs_ip == task_outputs_qe
        assert task_outputs_ip == task_outputs_ce
        assert task_outputs_ip == [[1, 2, 3], [1, 2, 3]]


@pytest.mark.usefixtures(
    "ray",
    "mock_config_env_var",
    "mock_db_env_var",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestCLI:
    @staticmethod
    def test_consistent_returns_for_single_value(
        mock_qe_run_single,
        mock_ce_run_single,
        orq_project_dir_single,
    ):
        # GIVEN
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "QE", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_ce = subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs"],
            check=True,
            capture_output=True,
        )

        m = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert m is not None
        run_id_ray = m.group("run_id")
        assert "Workflow submitted!" in run_qe.stdout.decode()
        assert "Workflow submitted!" in run_ce.stdout.decode()

        # WHEN
        results_ray = (
            subprocess.run(
                ["orq", "wf", "results", "-c", "local", run_id_ray],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )
        results_qe = (
            subprocess.run(
                ["orq", "wf", "results", "-c", "QE", mock_qe_run_single],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )
        results_ce = (
            subprocess.run(
                ["orq", "wf", "results", "-c", "CE", mock_ce_run_single],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )

        # THEN
        assert results_ray[1:] == results_qe[1:]
        assert results_ray[1:] == results_ce[1:]
        assert results_qe == [
            f"Workflow run {mock_qe_run_single} has 1 outputs.",
            "",
            "Output 0. Object type: <class 'list'>",
            "Pretty printed value:",
            "[1, 2, 3]",
            "",
        ]

    @staticmethod
    def test_consistent_returns_for_multiple_values(
        mock_qe_run_multiple,
        mock_ce_run_multiple,
        orq_project_dir_multiple,
    ):
        # GIVEN
        # Mocking for QE

        # run workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "QE", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_ce = subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs"],
            check=True,
            capture_output=True,
        )

        m = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert m is not None
        run_id_ray = m.group("run_id")
        assert "Workflow submitted!" in run_qe.stdout.decode()
        assert "Workflow submitted!" in run_ce.stdout.decode()

        # WHEN
        results_ray = (
            subprocess.run(
                ["orq", "wf", "results", "-c", "local", run_id_ray],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )
        results_qe = (
            subprocess.run(
                ["orq", "wf", "results", "-c", "QE", mock_qe_run_multiple],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )
        results_ce = (
            subprocess.run(
                ["orq", "wf", "results", "-c", "CE", mock_ce_run_multiple],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )

        # THEN
        assert results_ray[1:] == results_qe[1:]
        assert results_ray[1:] == results_ce[1:]
        assert results_qe == [
            f"Workflow run {mock_qe_run_multiple} has 2 outputs.",
            "",
            "Output 0. Object type: <class 'list'>",
            "Pretty printed value:",
            "[1, 2, 3]",
            "",
            "Output 1. Object type: <class 'list'>",
            "Pretty printed value:",
            "[1, 2, 3]",
            "",
        ]


@pytest.mark.usefixtures(
    "ray",
    "mock_config_env_var",
    "mock_db_env_var",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestCLIDownloadDir:
    @staticmethod
    def test_consistent_downloads_for_single_value(
        mock_qe_run_single,
        mock_ce_run_single,
        orq_project_dir_single,
    ):
        # GIVEN
        # # Mocking for Ray
        # monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

        # Run Workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "QE", "workflow_defs"],
            capture_output=True,
        )

        assert (
            run_qe.returncode == 0
        ), f"STDOUT: {run_qe.stdout.decode()},\n\nSTDOERR: {run_qe.stderr.decode()}"

        m = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert m is not None
        run_id_ray = m.group("run_id")
        assert mock_qe_run_single in run_qe.stdout.decode()

        # WHEN
        run_ray = subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                orq_project_dir_single,
                "-c",
                "local",
                run_id_ray,
            ],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                orq_project_dir_single,
                "-c",
                "QE",
                mock_qe_run_single,
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                orq_project_dir_single,
                "-c",
                "CE",
                mock_ce_run_single,
            ],
            check=True,
            capture_output=True,
        )

        # THEN
        with open(
            orq_project_dir_single + f"/{run_id_ray}/wf_results/0.json", "r"
        ) as f:
            ray_contents = json.load(f)
        with open(
            orq_project_dir_single + f"/{mock_qe_run_single}/wf_results/0.json", "r"
        ) as f:
            qe_contents = json.load(f)
        with open(
            orq_project_dir_single + f"/{mock_ce_run_single}/wf_results/0.json", "r"
        ) as f:
            ce_contents = json.load(f)

        assert ray_contents == qe_contents
        assert ray_contents == ce_contents
        assert ray_contents == [1, 2, 3]

    @staticmethod
    def test_consistent_downloads_for_multiple_values(
        mock_qe_run_multiple,
        mock_ce_run_multiple,
        orq_project_dir_multiple,
    ):
        # GIVEN
        # # Mocking for Ray
        # monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))

        # Run Workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "QE", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs"],
            check=True,
            capture_output=True,
        )

        m = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert m is not None
        run_id_ray = m.group("run_id")
        assert mock_qe_run_multiple in run_qe.stdout.decode()

        # WHEN
        subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                orq_project_dir_multiple,
                "-c",
                "local",
                run_id_ray,
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                orq_project_dir_multiple,
                "-c",
                "QE",
                mock_qe_run_multiple,
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                orq_project_dir_multiple,
                "-c",
                "CE",
                mock_ce_run_multiple,
            ],
            check=True,
            capture_output=True,
        )

        # THEN
        for result in range(0, 1):
            with open(
                orq_project_dir_multiple + f"/{run_id_ray}/wf_results/{result}.json",
                "r",
            ) as f:
                ray_contents = json.load(f)
            with open(
                orq_project_dir_multiple
                + f"/{mock_qe_run_multiple}/wf_results/{result}.json",
                "r",
            ) as f:
                qe_contents = json.load(f)
            with open(
                orq_project_dir_multiple
                + f"/{mock_ce_run_multiple}/wf_results/{result}.json",
                "r",
            ) as f:
                ce_contents = json.load(f)

            assert ray_contents == qe_contents
            assert ray_contents == ce_contents
            assert ray_contents == [1, 2, 3]
