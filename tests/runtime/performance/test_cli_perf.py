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
import os
import re
import shutil
import subprocess
import sys
import tempfile
import typing as t
from pathlib import Path

import pytest

import orquestra.sdk as sdk
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
    try:
        return subprocess.run(command, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"Command: {e.cmd}", file=sys.stderr)
        print(f"Return value: {e.returncode}", file=sys.stderr)
        print(f"stdout:\n{e.stdout}", file=sys.stderr)
        print(f"stderr:\n{e.stderr}", file=sys.stderr)
        raise


def _run_orq_command(command: t.List[str]):
    return _run_command(["orq", *command])


@pytest.fixture(scope="module")
def ray_cluster():
    with ray_suitable_temp_dir() as tmp_path:
        tmp_path.mkdir(parents=True, exist_ok=True)

        os.environ["ORQ_RAY_TEMP_PATH"] = str(tmp_path / "ray_temp")
        os.environ["ORQ_RAY_STORAGE_PATH"] = str(tmp_path / "ray_storage")

        _run_orq_command(["up"])
        try:
            yield
        finally:
            _run_orq_command(["down"])


@pytest.fixture(scope="module")
def orq_project_dir():
    tmp_path = Path(tempfile.mkdtemp())
    tmp_path.joinpath("workflow_defs.py").write_text(WORKFLOW_DEF)
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield str(tmp_path)
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp_path)


@pytest.fixture(scope="module")
def orq_workflow_run(ray_cluster, orq_project_dir):
    # Submit the workflow
    output = _run_orq_command(["wf", "submit", "-c", "local", "workflow_defs"])
    # Parse the stdout to get the workflow ID
    stdout = output.stdout.decode()
    match = re.match("Workflow submitted! Run ID: (?P<wf_run_id>.*)", stdout)
    assert match is not None
    workflow_id = match.groupdict().get("wf_run_id")
    assert workflow_id is not None
    # Wait for the workflow to finish to make sure that getting the results will succeed
    wf = sdk.WorkflowRun.by_id(workflow_id)
    wf.wait_until_finished()

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
def test_orq_submit_workflow_def(ray_cluster, orq_project_dir):
    _run_orq_command(["workflow", "submit", "-c", "local", "workflow_defs"])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_get_workflow_run(orq_project_dir, orq_workflow_run):
    _run_orq_command(["workflow", "view", orq_workflow_run, "-c", "local"])


@pytest.mark.expect_under(TEST_TIMEOUT)
def test_get_workflow_run_results(orq_project_dir, orq_workflow_run):
    _run_orq_command(
        [
            "workflow",
            "results",
            orq_workflow_run,
            "-c",
            "local",
        ]
    )
