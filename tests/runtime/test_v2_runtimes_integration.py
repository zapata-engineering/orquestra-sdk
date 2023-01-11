################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Tests suited for running on a dev's machine. Not suitable for running on CI
until we nail down machine-to-machine auth.

To run this:
1. Grab a QE-compatible repo, like
    https://github.com/zapatacomputing/evangelism-workflows. Note we have to use
    a separate repo until we make multi-wf-repos possible. See this epic for
    progress: https://zapatacomputing.atlassian.net/browse/ORQSDK-348.
2. Run `orq login ...`. Note down the name under which you save the configuration.
3. Run:
    ```
    REPO="../../path/to/test/repo" \
        CONFIG="cfg-name" \
        pytest -m needs_separate_project -s
    ```
"""
import contextlib
import os
import time
from argparse import Namespace
from pathlib import Path

import pytest

import orquestra.sdk._base._config as _config
import orquestra.sdk.exceptions as exceptions
from orquestra.sdk._base._qe._qe_runtime import QERuntime
from orquestra.sdk._base.cli._corq import action
from orquestra.sdk.schema import responses, workflow_run


@contextlib.contextmanager
def _chdir(target_dir: Path):
    # TODO: reuse this across tests
    starting_dir = os.getcwd()

    try:
        os.chdir(target_dir)
        yield

    finally:
        os.chdir(starting_dir)


def _test_repo_location() -> Path:
    repo_path = os.getenv("REPO")
    assert (
        repo_path is not None
    ), "Please provide a local path to the test project repo with REPO env variable."
    return Path(repo_path)


def _config_name() -> str:
    config_name = os.getenv("CONFIG")
    assert (
        config_name is not None
    ), "Please provide a config name so we know where to send the test workflows."
    return config_name


@pytest.mark.needs_separate_project
def test_studio_like_flow():
    """The idea:
    1. `cd` into `$REPO`
    2. Get all workflow defs
    3. Submit the first workflow def
    4. Get resources before the wf is finished
        4.1. Logs for the full wf run
        4.2. Wf outputs - expect errors
    5. Poll status until the workflow is finished or failed
        5.1. Check the status is identical when checking by ID and by listing all runs
    6. Get resources and don't expect errors
        6.1. Logs for the full wf run
        6.2. Logs for a single task run
        6.3. Wf outputs
    7. Validate ID consistency between the wf def and wf run
    """
    repo_path = _test_repo_location()
    config_name = _config_name()

    # (1)
    with _chdir(repo_path):
        # We `cd`ed into the other project.
        project_dir = "."

        # (2)
        get_wfs_resp = action.orq_get_workflow_def(
            Namespace(
                config=config_name,
                workflow_def_name=None,
                directory=project_dir,
            )
        )
        assert get_wfs_resp.meta.success is True
        assert isinstance(get_wfs_resp, responses.GetWorkflowDefResponse)
        wf_name = get_wfs_resp.workflow_defs[0].name

        # (3)
        submit_resp = action.orq_submit_workflow_def(
            Namespace(
                config=config_name,
                workflow_def_name=wf_name,
                directory=project_dir,
                verbose=False,
                force=True,
            )
        )
        assert submit_resp.meta.success is True
        assert isinstance(submit_resp, responses.SubmitWorkflowDefResponse)
        wf_run_id = submit_resp.workflow_runs[0].id
        print(f"Submitted wf run {wf_run_id} to {config_name}")

        config = _config.read_config(config_name)
        rt = QERuntime(config=config, project_dir=project_dir)

        # We need to wait between submitting workflow and getting its status.
        # Otherwise, QE responds with status code 500.
        # Reported as https://zapatacomputing.atlassian.net/browse/ORQP-1081
        print("Sleeping 20s to workaround QE's 500 errors")
        time.sleep(20)

        first_status = rt.get_workflow_run_status(wf_run_id)
        assert first_status.status.state in {
            workflow_run.State.WAITING,
            workflow_run.State.RUNNING,
        }
        print(f"Workflow run state is {first_status.status.state}")

        # (4.1)
        running_wf_logs_dict = rt.get_full_logs(wf_run_id)

        # Unfortunately we can't assert much here, because the logs dict might
        # be empty right after submitting the workflow.
        assert running_wf_logs_dict is not None
        print(f"Fetched logs for {len(running_wf_logs_dict)} task invocations")

        # (4.2)
        with pytest.raises(exceptions.WorkflowRunNotSucceeded) as e:
            _ = rt.get_workflow_run_outputs_non_blocking(wf_run_id)
        print(f"As expected, got {e.typename} from attempting to get outputs")

        # (5)
        polled_status = first_status
        while polled_status.status.state not in {
            workflow_run.State.SUCCEEDED,
            workflow_run.State.TERMINATED,
            workflow_run.State.FAILED,
        }:
            # Waiting and re-trying
            sleep_time = 4
            print(
                f"{wf_run_id}'s status is: {polled_status.status.state.value}. "
                f"Sleeping for {sleep_time}s and trying again..."
            )
            time.sleep(sleep_time)
            polled_status = rt.get_workflow_run_status(wf_run_id)

        assert polled_status.status.state == workflow_run.State.SUCCEEDED
        print(f"Wf run {wf_run_id} succeeded!")

        # (5.1)
        all_statuses = rt.get_all_workflow_runs_status()
        assert polled_status == all_statuses[0]
        print(
            "Checked the status by workflow run ID and by getting all workflow runs. "
            "They were the same!"
        )

        # (6.1)
        wf_logs_dict = rt.get_full_logs(wf_run_id)
        print(f"Fetched full workflow logs for {len(wf_logs_dict)} tasks")

        assert len(wf_logs_dict) == len(polled_status.task_runs)
        # TODO: switch `run.invocation_id` into `run.id` when this change is implemented
        assert set(wf_logs_dict.keys()) == {
            run.invocation_id for run in polled_status.task_runs
        }
        for key, log_lines in wf_logs_dict.items():
            assert len(log_lines) > 0

        # (6.2)
        task_run_id = polled_status.task_runs[0].id
        task_logs_dict = rt.get_full_logs(task_run_id)
        print(f"Fetched logs for task run {task_run_id}")

        assert len(task_logs_dict) == 1
        # TODO: switch `run.invocation_id` into `run.id` when this change is implemented
        assert len(task_logs_dict[polled_status.task_runs[0].invocation_id]) > 0

        # (6.3)
        # Sometimes, QE fails to return artifacts even though it tells us that
        # the workflow was completed.
        for attempt_i in range(10):
            print(f"Trying to get workflow outputs. Attempt {attempt_i + 1}")
            try:
                output_vals = rt.get_workflow_run_outputs_non_blocking(wf_run_id)
                break
            except exceptions.NotFoundError:
                time.sleep(4)
        else:
            pytest.fail(
                "Can't get workflow results from QE despite the workflow being "
                "completed."
            )

        assert len(output_vals) > 1
        print(f"Fetched workflow output vals: {output_vals}")

        # (7)
        get_wf_def_resp = action.orq_get_workflow_def(
            Namespace(
                config=config_name,
                workflow_def_name=None,
                directory=project_dir,
            )
        )
        assert get_wf_def_resp.meta.success is True
        print("Read the original workflow def")

        assert len(get_wf_def_resp.workflow_defs) == 1
        wf_def = get_wf_def_resp.workflow_defs[0]

        assert {tr.invocation_id for tr in polled_status.task_runs} == set(
            wf_def.task_invocations.keys()
        ), "Invocation IDs in task runs don't match the IDs in workflow def"

        print("Invocation IDs match between the wf def and run!")
