import time
import warnings

import orquestra.sdk as sdk

from .cli_command_runner import run_orq_command
from .test_inputs import wfs


def _find_task_run(fn_name: str, wf_run: sdk.WorkflowRun) -> sdk.TaskRun:
    sleep_time = 4
    while True:
        task_runs = wf_run.get_tasks()
        matching = [task for task in task_runs if task.fn_name == fn_name]
        if len(matching) >= 1:
            return next(iter(matching))
        else:
            inv_ids = [run.task_invocation_id for run in task_runs]
            print(
                f"Couldn't find task runs with function name {fn_name}. "
                f"Available task invocations: {inv_ids}. "
                f"Will retry after {sleep_time}s..."
            )
            time.sleep(sleep_time)


def _run_scenario(config):
    wf: sdk.WorkflowDef = wfs.add_some_ints(
        secret_config=None,
        secret_workspace=None,
        github_username=None,
    )
    wf_run = wf.run(
        config,
    )

    _ = wf_run.wait_until_finished()

    status = wf_run.get_status()
    computed_values = wf_run.get_results()

    assert status == sdk.State.SUCCEEDED
    assert computed_values == (30, 90, 90, 500)

    if config != "in_process":
        logs_wait_time = 12
        time.sleep(logs_wait_time)

        wf_logs = wf_run.get_logs()
        assert (
            "Adding two numbers:"
            in wf_logs.per_task["invocation-0-task-sum-with-logs"].out[0]
        )
        assert (
            "Adding two numbers:"
            in wf_logs.per_task["invocation-1-task-sum-with-logs"].out[0]
        )

        task_run = _find_task_run(fn_name="sum_with_logs", wf_run=wf_run)
        task_logs = task_run.get_logs()
        assert "Adding two numbers:" in task_logs.out[0]


def test_basic_scenario():
    configs_to_test = ["ray", "in_process"]

    ray_status_output = run_orq_command(["status"]).stdout

    # Ensure Ray is not running
    assert "Not Running" in str(
        ray_status_output
    ), "Ray is running before the test starts. Please stop ray manually."

    # Start Ray
    ray_up_output = run_orq_command(["up"]).stdout
    assert "Started!" in str(ray_up_output), "Ray should be started"

    # double check that ray is actually running
    ray_status_output = run_orq_command(["status"]).stdout
    assert "Not Running" not in str(
        ray_status_output
    ), "Ray should be running at this point"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", sdk.NotATaskWarning)
        for config in configs_to_test:
            _run_scenario(config=config)

    # Shutdown Ray
    ray_up_output = run_orq_command(["down"]).stdout
    assert "Not Running" in str(ray_up_output), "Ray should be shut down"

    # Ensure Ray is not running
    ray_status_output = run_orq_command(["status"]).stdout
    assert "Not Running" in str(
        ray_status_output
    ), "Ray is running After test is concluded. Something went wrong"
