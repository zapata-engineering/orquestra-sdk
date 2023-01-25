################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
"""
Checks if something terribly wrong is happening with the CLI latency.

Temporarily, CLI latency is embarrasingly high and we don't have time to fix it.
It's good enough to implement and release new features, though.

See this ticket for more investigation:
https://zapatacomputing.atlassian.net/browse/ORQSDK-507
"""
import json
import shutil
import subprocess
import tempfile
import typing as t
from pathlib import Path
import time

import pytest

from orquestra.sdk._base._testing._connections import ray_suitable_temp_dir

WORKFLOW_DEF = """
import orquestra.sdk._base._testing._long_import
import orquestra.sdk as sdk
@sdk.task
def task():
    return 1
@sdk.workflow
def workflow():
    return [task(), task()]
"""


def _run_command(command: t.List[str]):
    return subprocess.run(command, check=True, capture_output=True)


def _run_orq_command(command: t.List[str]):
    return _run_command(["orq", *command])


@pytest.fixture(scope="module")
def ray_cluster():
    with ray_suitable_temp_dir() as tmp_path:
        tmp_path.mkdir(parents=True, exist_ok=True)

        ray_temp_path = tmp_path / "ray_temp"
        ray_storage_path = tmp_path / "ray_storage"

        _run_command(
            [
                "ray",
                "start",
                "--head",
                f"--temp-dir={ray_temp_path}",
                f"--storage={ray_storage_path}",
            ]
        )
        try:
            yield
        finally:
            _run_command(["ray", "stop"])


@pytest.fixture(scope="module")
def orq_project_dir():
    tmp_path = Path(tempfile.mkdtemp())
    tmp_path.joinpath("workflow_defs.py").write_text(WORKFLOW_DEF)
    yield str(tmp_path)
    shutil.rmtree(tmp_path)


@pytest.fixture(scope="module")
def orq_workflow_run(ray_cluster, orq_project_dir):
    # Submit the workflow
    output = _run_orq_command(
        ["submit", "workflow-def", "-d", orq_project_dir, "-o", "json", "-c", "local"]
    )

    # Parse the stdout to get the workflow ID
    res = json.loads(output.stdout)
    workflow_id = res["workflow_runs"][0]["id"]

    # Ray seems to not be in-sync with workflow submission. Lets lazily wait for it
    # to have to workflow submitted. workflow-run-results will wait for it to complete
    timeout = 10
    for i in range(timeout):
        try:
            _run_orq_command(
                [
                    "get",
                    "workflow-run-results",
                    workflow_id,
                    "-d",
                    orq_project_dir,
                    "-c",
                    "local",
                ]
            )
            break
        except Exception:
            if i >= timeout:
                raise

    yield workflow_id


TEST_TIMEOUT = 20


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_orq_help():
    _run_orq_command(["-h"])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_orq_invalid():
    with pytest.raises(subprocess.CalledProcessError):
        _run_orq_command(["general-kenobi"])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_get_workflow_def(orq_project_dir):
    _run_orq_command(["get", "workflow-def", "-d", orq_project_dir])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_get_task_def(orq_project_dir):
    _run_orq_command(["get", "task-def", "-d", orq_project_dir])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_orq_submit_workflow_def(ray_cluster, orq_project_dir):
    _run_orq_command(["submit", "workflow-def", "-d", orq_project_dir, "-c", "local"])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_get_workflow_run(orq_project_dir, orq_workflow_run):
    _run_orq_command(
        ["get", "workflow-run", orq_workflow_run, "-d", orq_project_dir, "-c", "local"]
    )


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_get_workflow_run_results(orq_project_dir, orq_workflow_run):
    _run_orq_command(
        [
            "get",
            "workflow-run-results",
            orq_workflow_run,
            "-d",
            orq_project_dir,
            "-c",
            "local",
        ]
    )
