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

Outputs tested:
- WF results
- artifacts
- task outputs
"""

import inspect
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import typing as t
from pathlib import Path

import pytest
import pytest_httpserver

import orquestra.sdk as sdk
from orquestra.sdk._client._base._testing import _connections
from orquestra.sdk._shared.schema.workflow_run import RunStatus, State, TaskRun

from .driver import resp_mocks


# region: workflow definition
# Vanilla python versions of the workflow for comparison.
# These should be an undecorated version of the task and workflow definitions below.
def get_list_vanilla():
    return [1, 2, 3]


def wf_return_single_packed_value_vanilla():
    a = get_list_vanilla()
    return a


def wf_return_multiple_packed_values_vanilla():
    a = get_list_vanilla()
    b = get_list_vanilla()
    return a, b


# SDK versions to be run.
@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=[
        sdk.GitImport(
            repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",
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
            '      "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",\n'  # noqa
            f'      "uri": "http://127.0.0.1:{httpserver.port}"\n'
            "  }\n"
            "  }\n"
            "},\n"
            '  "default_config_name": "local"\n'
            "}\n"
        )


@pytest.fixture
def post(httpserver: pytest_httpserver.HTTPServer):
    def _inner(endpoint: str, json: t.Dict):
        httpserver.expect_request(endpoint, method="POST").respond_with_json(json)

    return _inner


@pytest.fixture
def get(httpserver: pytest_httpserver.HTTPServer):
    def _inner(endpoint: str, json: t.Dict):
        httpserver.expect_request(endpoint, method="GET").respond_with_json(json)

    return _inner


@pytest.fixture
def mock_ce_run_single(post, get, httpserver: pytest_httpserver.HTTPServer) -> str:
    wf_id: str = "wf_id_sentinel_ce_single"
    wf_def_id: str = "wf_def_ce_sentinel"
    result_ids = ["result_id_sentinel_0"]

    # region: Start workflow
    post(
        "/api/workflow-definitions",
        resp_mocks.make_create_wf_def_response(id_=wf_def_id),
    )
    post("/api/workflow-runs", resp_mocks.make_submit_wf_run_response(wf_id))
    # Mock the known workflow run request.
    get(
        f"/api/workflow-runs/{wf_id}",
        resp_mocks.make_get_wf_run_response(
            id_=wf_id,
            workflow_def_id=wf_def_id,
            status=RunStatus(state=State.SUCCEEDED, start_time=None, end_time=None),
            task_runs=[
                TaskRun(
                    id="foo",
                    invocation_id="invocation-0-task-get-list",
                    status=RunStatus(
                        state=State.SUCCEEDED, start_time=None, end_time=None
                    ),
                )
            ],
        ),
    )
    # Requests for other workflow runs should return "Not Found". Used by the "query
    # all runtimes" machinery under sdk.WorkflowRun.by_id().
    httpserver.expect_request(
        re.compile(r"/api/workflow-runs/.+"), method="GET"
    ).respond_with_json({}, status=404)

    get(
        f"/api/workflow-definitions/{wf_def_id}",
        resp_mocks.make_get_wf_def_response(
            id_=wf_def_id, wf_def=wf_return_single_packed_value().model
        ),
    )
    # endregion

    # Get results
    get("/api/run-results", {"data": result_ids})

    # We currently assume only there's a single output
    # Let's make this explicit in the test
    result_id = result_ids[0]
    get(
        f"/api/run-results/{result_id}",
        {
            "results": [
                {"value": "[1, 2, 3]", "serialization_format": "JSON"},
            ]
        },
    )

    # Get artifacts
    get(
        "/api/artifacts",
        {"data": {"invocation-0-task-get-list": ["artifact-3-get-list"]}},
    )
    get(
        "/api/artifacts/artifact-3-get-list",
        resp_mocks.make_get_wf_run_artifact_response([1, 2, 3]),
    )

    return wf_id


@pytest.fixture
def mock_ce_run_multiple(post, get) -> str:
    wf_id: str = "wf_id_sentinel_ce_multiple"
    wf_def_id: str = "wf_def_ce_multiple_sentinel"
    result_ids = ["result_id_sentinel_0"]

    # region: Start workflow
    post(
        "/api/workflow-definitions",
        resp_mocks.make_create_wf_def_response(id_=wf_def_id),
    )
    post("/api/workflow-runs", resp_mocks.make_submit_wf_run_response(wf_id))
    get(
        f"/api/workflow-runs/{wf_id}",
        resp_mocks.make_get_wf_run_response(
            id_=wf_id,
            workflow_def_id=wf_def_id,
            status=RunStatus(state=State.SUCCEEDED, start_time=None, end_time=None),
            task_runs=[
                TaskRun(
                    id="foo",
                    invocation_id="invocation-0-task-get-list",
                    status=RunStatus(
                        state=State.SUCCEEDED, start_time=None, end_time=None
                    ),
                ),
                TaskRun(
                    id="foo",
                    invocation_id="invocation-1-task-get-list",
                    status=RunStatus(
                        state=State.SUCCEEDED, start_time=None, end_time=None
                    ),
                ),
            ],
        ),
    )
    get(
        f"/api/workflow-definitions/{wf_def_id}",
        resp_mocks.make_get_wf_def_response(
            id_=wf_def_id, wf_def=wf_return_single_packed_value().model
        ),
    )
    # endregion
    #
    # # Get results
    get("/api/run-results", {"data": result_ids})

    # We currently assume only there's a single output
    # Let's make this explicit in the test
    result_id = result_ids[0]
    get(
        f"/api/run-results/{result_id}",
        {
            "results": [
                {"value": "[1, 2, 3]", "serialization_format": "JSON"},
                {"value": "[1, 2, 3]", "serialization_format": "JSON"},
            ]
        },
    )
    # Get artifacts
    get(
        "/api/artifacts",
        {
            "data": {
                "invocation-0-task-get-list": ["artifact-3-get-list"],
                "invocation-1-task-get-list": ["artifact-7-get-list"],
            }
        },
    )
    get(
        "/api/artifacts/artifact-3-get-list",
        resp_mocks.make_get_wf_run_artifact_response([1, 2, 3]),
    )
    get(
        "/api/artifacts/artifact-7-get-list",
        resp_mocks.make_get_wf_run_artifact_response([1, 2, 3]),
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


@pytest.fixture(scope="module")
def single_result_vanilla():
    return wf_return_single_packed_value_vanilla()


@pytest.fixture(scope="module")
def multiple_result_vanilla():
    return wf_return_multiple_packed_values_vanilla()


# endregion


@pytest.mark.usefixtures(
    "ray",
    "mock_config_env_var",
)
@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestAPI:
    @staticmethod
    def test_consistent_returns_for_single_value(
        mock_ce_run_single,
        orq_project_dir_single,
        single_result_vanilla,
    ):
        # GIVEN
        ip_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.in_process())
        ray_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.ray())
        ce_run = wf_return_single_packed_value().run(sdk.RuntimeConfig.load("CE"))
        for run in [ip_run, ray_run]:
            run.wait_until_finished()

        # WHEN
        results_ip = ip_run.get_results()
        results_ray = ray_run.get_results()
        results_ce = ce_run.get_results()

        artifacts_ip = ip_run.get_artifacts()
        artifacts_ray = ray_run.get_artifacts()
        artifacts_ce = ce_run.get_artifacts()

        task_outputs_ip = [t.get_outputs() for t in ip_run.get_tasks()]
        task_outputs_ray = [t.get_outputs() for t in ray_run.get_tasks()]
        task_outputs_ce = [t.get_outputs() for t in ce_run.get_tasks()]

        # THEN
        assert results_ip == results_ray
        assert results_ip == results_ce
        assert results_ip == single_result_vanilla

        assert artifacts_ip == artifacts_ray
        assert artifacts_ip == artifacts_ce
        assert artifacts_ip == {"invocation-0-task-get-list": single_result_vanilla}

        assert task_outputs_ip == task_outputs_ray
        assert task_outputs_ip == task_outputs_ce
        assert task_outputs_ip == [single_result_vanilla]

    @staticmethod
    def test_consistent_returns_for_multiple_values(
        mock_ce_run_multiple,
        orq_project_dir_multiple,
        multiple_result_vanilla,
    ):
        # GIVEN
        ip_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.in_process())
        ray_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.ray())
        ce_run = wf_return_multiple_packed_values().run(sdk.RuntimeConfig.load("CE"))

        for run in [ip_run, ray_run]:
            run.wait_until_finished()

        # WHEN
        results_ip = ip_run.get_results()
        results_ray = ray_run.get_results()
        results_ce = ce_run.get_results()

        artifacts_ip = ip_run.get_artifacts()
        artifacts_ray = ray_run.get_artifacts()
        artifacts_ce = ce_run.get_artifacts()

        task_outputs_ip = [t.get_outputs() for t in ip_run.get_tasks()]
        task_outputs_ray = [t.get_outputs() for t in ray_run.get_tasks()]
        task_outputs_ce = [t.get_outputs() for t in ce_run.get_tasks()]

        # THEN
        assert results_ip == results_ray
        assert results_ip == results_ce
        assert results_ip == multiple_result_vanilla

        assert artifacts_ip == artifacts_ray
        assert artifacts_ip == artifacts_ce
        assert artifacts_ip == {
            "invocation-0-task-get-list": multiple_result_vanilla[0],
            "invocation-1-task-get-list": multiple_result_vanilla[1],
        }

        assert task_outputs_ip == task_outputs_ray
        assert task_outputs_ip == task_outputs_ce
        assert task_outputs_ip == [*multiple_result_vanilla]


@pytest.mark.usefixtures(
    "ray",
    "mock_config_env_var",
)
@pytest.mark.skipif(
    sys.platform.startswith("win32"),
    reason="Windows uses different symbols than macOS and Linux",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestCLI:
    @staticmethod
    def test_consistent_returns_for_single_value(
        mock_ce_run_single,
        orq_project_dir_single,
        single_result_vanilla,
    ):
        # GIVEN
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_ce = subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs", "-w", "ws", "-p", "p"],
            check=True,
            capture_output=True,
        )

        m = re.match(
            r"Workflow Submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )

        assert (
            m is not None
        ), f"STDOUT: {run_ray.stdout.decode()},\n\nSTDERR: {run_ray.stderr.decode()}"
        run_id_ray = m.group("run_id").strip()
        assert "Workflow Submitted!" in run_ce.stdout.decode()

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
        assert results_ray[1:] == results_ce[1:]
        assert [line.strip() for line in results_ce] == [
            f"Workflow run {mock_ce_run_single} has 1 outputs.",
            "",
            "Index   Type             Pretty Printed",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "0       <class 'list'>   [1, 2, 3]",
            "",
            "",
        ]

    @staticmethod
    def test_consistent_returns_for_multiple_values(
        mock_ce_run_multiple,
        orq_project_dir_multiple,
        multiple_result_vanilla,
    ):
        # GIVEN

        # run workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_ce = subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs", "-w", "ws", "-p", "p"],
            check=True,
            capture_output=True,
        )

        m = re.match(
            r"Workflow Submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert (
            m is not None
        ), f"STDOUT: {run_ray.stdout.decode()},\n\nSTDERR: {run_ray.stderr.decode()}"
        run_id_ray = m.group("run_id").strip()
        assert "Workflow Submitted!" in run_ce.stdout.decode()

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
        assert results_ray[1:] == results_ce[1:]
        assert [line.strip() for line in results_ce] == [
            f"Workflow run {mock_ce_run_multiple} has 2 outputs.",
            "",
            "Index   Type             Pretty Printed",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "0       <class 'list'>   [1, 2, 3]",
            "1       <class 'list'>   [1, 2, 3]",
            "",
            "",
        ]


@pytest.mark.usefixtures(
    "ray",
    "mock_config_env_var",
)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.slow
class TestCLIDownloadDir:
    @staticmethod
    def test_consistent_downloads_for_single_value(
        mock_ce_run_single,
        orq_project_dir_single,
        single_result_vanilla,
    ):
        # GIVEN

        # Run Workflows
        run_ray = subprocess.run(
            ["orq", "wf", "submit", "-c", "local", "workflow_defs"],
            check=True,
            capture_output=True,
        )
        run_ce = subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs", "-w", "ws", "-p", "p"],
            capture_output=True,
        )

        assert (
            run_ce.returncode == 0
        ), f"STDOUT: {run_ce.stdout.decode()},\n\nSTDERR: {run_ce.stderr.decode()}"

        m = re.match(
            r"Workflow Submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert (
            m is not None
        ), f"STDOUT: {run_ray.stdout.decode()},\n\nSTDERR: {run_ray.stderr.decode()}"
        run_id_ray = m.group("run_id").strip()
        assert mock_ce_run_single in run_ce.stdout.decode()

        sdk.WorkflowRun.by_id(run_id_ray).wait_until_finished()

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
            capture_output=True,
        )
        assert (
            run_ray.returncode == 0
        ), f"STDOUT: {run_ray.stdout.decode()}\n\nSTDERR: {run_ray.stderr.decode()}"
        run_ce = subprocess.run(
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
            capture_output=True,
        )
        assert (
            run_ce.returncode == 0
        ), f"STDOUT: {run_ce.stdout.decode()}\n\nSTDERR: {run_ce.stderr.decode()}"

        # THEN
        with open(
            orq_project_dir_single + f"/{run_id_ray}/wf_results/0.json", "r"
        ) as f:
            ray_contents = json.load(f)
        with open(
            orq_project_dir_single + f"/{mock_ce_run_single}/wf_results/0.json", "r"
        ) as f:
            ce_contents = json.load(f)

        assert ray_contents == ce_contents
        assert ray_contents == single_result_vanilla

    @staticmethod
    def test_consistent_downloads_for_multiple_values(
        mock_ce_run_multiple,
        orq_project_dir_multiple,
        multiple_result_vanilla,
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
        run_ce = subprocess.run(
            ["orq", "wf", "submit", "-c", "CE", "workflow_defs", "-w", "ws", "-p", "p"],
            check=True,
            capture_output=True,
        )

        m = re.match(
            r"Workflow Submitted! Run ID: (?P<run_id>.*)", run_ray.stdout.decode()
        )
        assert (
            m is not None
        ), f"STDOUT: {run_ray.stdout.decode()},\n\nSTDERR: {run_ray.stderr.decode()}"
        run_id_ray = m.group("run_id").strip()
        assert mock_ce_run_multiple in run_ce.stdout.decode()

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
                + f"/{mock_ce_run_multiple}/wf_results/{result}.json",
                "r",
            ) as f:
                ce_contents = json.load(f)

            assert ray_contents == ce_contents
            assert ray_contents == multiple_result_vanilla[result]
