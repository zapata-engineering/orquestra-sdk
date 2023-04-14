################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import base64
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


WORKFLOW_DEF_SINGLE = """
import orquestra.sdk as sdk
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
"""

WORKFLOW_DEF_MULTIPLE = """
import orquestra.sdk as sdk
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
def wf_return_multiple_packed_values():
    a = get_list()
    b = get_list()
    return a, b
"""

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


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
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
def mock_settings_env_vars(tmp_path):
    with mock.patch.dict(os.environ, {"ORQ_DB_PATH": str(tmp_path / "workflows.db")}):
        yield


# endregion


@pytest.mark.usefixtures(
    "patch_config_location",
    "ray",
    "mock_workflow_db_location",
    "define_test_config",
    "change_test_dir",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestAPI:
    @staticmethod
    def test_consistent_returns_for_single_value(
        monkeypatch,
        tmp_path,
        httpserver: pytest_httpserver.HTTPServer,
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

    @staticmethod
    def test_consistent_returns_for_multiple_values(
        monkeypatch,
        tmp_path,
        httpserver: pytest_httpserver.HTTPServer,
    ):
        # GIVEN
        # Mocking for Ray
        monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
        # Mocking for QE
        httpserver.expect_request("/v1/workflows").respond_with_data("wf_id_sentinel")
        httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
        httpserver.expect_request(
            "/v2/workflows/wf_id_sentinel/result"
        ).respond_with_data(
            _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE),
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


@pytest.mark.usefixtures(
    "patch_config_location",
    "ray",
    "define_test_config",
    "change_test_dir",
    "mock_settings_env_vars",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestCLI:
    @staticmethod
    def test_consistent_returns_for_single_value(
        monkeypatch,
        tmp_path,
        httpserver: pytest_httpserver.HTTPServer,
        orq_project_dir_single,
        mock_workflow_db_location,
    ):
        # GIVEN
        # # Mocking for Ray
        # monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
        # Mocking for QE
        httpserver.expect_request("/v1/workflows").respond_with_data("wf_id_sentinel")
        httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
        httpserver.expect_request(
            "/v2/workflows/wf_id_sentinel/result"
        ).respond_with_data(
            _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_SINGLE),
            content_type="application/x-gtar-compressed",
        )

        # Run Workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "127", "workflow_defs"],
            check=True,
            capture_output=True,
        )

        run_id_ray = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        ).group("run_id")
        assert "wf_id_sentinel" in run_qe.stdout.decode()

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
                ["orq", "wf", "results", "-c", "127", "wf_id_sentinel"],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )

        # THEN
        assert results_qe == [
            "Workflow run wf_id_sentinel has 1 outputs.",
            "",
            "Output 0. Object type: <class 'list'>",
            "Pretty printed value:",
            "[1, 2, 3]",
            "",
        ]
        assert results_ray[1:] == results_qe[1:]

    @staticmethod
    def test_consistent_returns_for_multiple_values(
        monkeypatch,
        tmp_path,
        httpserver: pytest_httpserver.HTTPServer,
        orq_project_dir_multiple,
        mock_workflow_db_location,
    ):
        # GIVEN
        # Mocking for QE
        qe_id = "wf_id_sentinel_2"
        httpserver.expect_request("/v1/workflows").respond_with_data(qe_id)
        httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
        httpserver.expect_request(f"/v2/workflows/{qe_id}/result").respond_with_data(
            _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE),
            content_type="application/x-gtar-compressed",
        )

        # run workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "127", "workflow_defs"],
            check=True,
            capture_output=True,
        )

        run_id_ray = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        ).group("run_id")
        assert qe_id in run_qe.stdout.decode()

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
                ["orq", "wf", "results", "-c", "127", qe_id],
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .split("\n")
        )

        # THEN
        assert results_qe == [
            f"Workflow run {qe_id} has 2 outputs.",
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
        assert results_ray[1:] == results_qe[1:]


@pytest.mark.usefixtures(
    "patch_config_location",
    "ray",
    "define_test_config",
    "change_test_dir",
    "mock_settings_env_vars",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestCLIDownloadDir:
    @staticmethod
    def test_consistent_downloads_for_single_value(
        monkeypatch,
        tmp_path,
        httpserver: pytest_httpserver.HTTPServer,
        orq_project_dir_single,
        mock_workflow_db_location,
    ):
        # GIVEN
        # # Mocking for Ray
        # monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
        # Mocking for QE
        run_id_qe = "wf_id_sentinel_99"
        httpserver.expect_request("/v1/workflows").respond_with_data(run_id_qe)
        httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
        httpserver.expect_request(
            f"/v2/workflows/{run_id_qe}/result"
        ).respond_with_data(
            _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_SINGLE),
            content_type="application/x-gtar-compressed",
        )

        # Run Workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "127", "workflow_defs"],
            capture_output=True,
        )

        assert (
            run_qe.returncode == 0
        ), f"STDOUT: {run_qe.stdout.decode()},\n\nSTDOERR: {run_qe.stderr.decode()}"

        run_id_ray = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        ).group("run_id")
        assert run_id_qe in run_qe.stdout.decode()

        # WHEN
        subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                tmp_path,
                "-c",
                "local",
                run_id_ray,
            ],
            check=True,
            capture_output=True,
        )
        p2 = subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                tmp_path,
                "-c",
                "127",
                run_id_qe,
            ],
            capture_output=True,
        )
        assert (
            p2.returncode == 0
        ), f"STDOUT: {p2.stdout.decode()},\n\nSTDOERR: {p2.stderr.decode()}"

        # THEN
        with open(tmp_path / f"{run_id_ray}/wf_results/0.json", "r") as f:
            ray_contents = json.load(f)
        with open(tmp_path / f"{run_id_qe}/wf_results/0.json", "r") as f:
            qe_contents = json.load(f)

        assert ray_contents == qe_contents
        assert ray_contents == [1, 2, 3]

    @staticmethod
    def test_consistent_downloads_for_multiple_values(
        monkeypatch,
        tmp_path,
        httpserver: pytest_httpserver.HTTPServer,
        orq_project_dir_multiple,
        mock_workflow_db_location,
    ):
        # GIVEN
        # # Mocking for Ray
        # monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
        # Mocking for QE
        httpserver.expect_request("/v1/workflows").respond_with_data("wf_id_sentinel")
        httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
        httpserver.expect_request(
            "/v2/workflows/wf_id_sentinel/result"
        ).respond_with_data(
            _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT_MULTIPLE),
            content_type="application/x-gtar-compressed",
        )
        run_id_qe = "wf_id_sentinel"

        # Run Workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_qe = subprocess.run(
            ["orq", "wf", "submit", "-c", "127", "workflow_defs"],
            check=True,
            capture_output=True,
        )

        run_id_ray = re.match(
            r"Workflow submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        ).group("run_id")
        assert run_id_qe in run_qe.stdout.decode()

        # WHEN
        subprocess.run(
            [
                "orq",
                "wf",
                "results",
                "--download-dir",
                tmp_path,
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
                tmp_path,
                "-c",
                "127",
                run_id_qe,
            ],
            check=True,
            capture_output=True,
        )

        # THEN
        for result in range(0, 1):
            with open(tmp_path / f"{run_id_ray}/wf_results/{result}.json", "r") as f:
                ray_contents = json.load(f)
            with open(tmp_path / f"{run_id_qe}/wf_results/{result}.json", "r") as f:
                qe_contents = json.load(f)

            assert ray_contents == qe_contents
            assert ray_contents == [1, 2, 3]
