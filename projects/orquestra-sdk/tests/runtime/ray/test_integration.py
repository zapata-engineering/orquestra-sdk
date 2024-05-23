################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""
Integration tests for our code that uses Ray. This file should be kept as small
as possible, because it's slow to run. Please consider using unit tests and
RuntimeInterface mocks instead of extending this file.
"""
import json
import os
import re
import sys
import time
import typing as t
from datetime import datetime, timezone
from pathlib import Path

import pytest
from freezegun import freeze_time
from orquestra.workflow_runtime._ray import _build_workflow, _client, _dag, _ray_logs
from orquestra.workflow_runtime._ray._env import (
    RAY_DOWNLOAD_GIT_IMPORTS_ENV,
    RAY_TEMP_PATH_ENV,
)
from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.abc import RuntimeInterface
from orquestra.workflow_shared.schema import ir
from orquestra.workflow_shared.schema.responses import JSONResult
from orquestra.workflow_shared.schema.workflow_run import State, WorkflowRunId
from orquestra.workflow_shared.serde import deserialize

from orquestra import sdk
from orquestra.sdk._client._base._config import LOCAL_RUNTIME_CONFIGURATION
from orquestra.sdk._client._base._testing import _example_wfs, _ipc

# Ray mishandles log file handlers and we get "_io.FileIO [closed]"
# unraisable exceptions. Last tested with Ray 2.4.0.
pytestmark = pytest.mark.filterwarnings(
    "ignore::pytest.PytestUnraisableExceptionWarning"
)


@pytest.fixture(scope="module")
def runtime(
    shared_ray_conn,
):
    # We need to set this env variable to let our logging code know the
    # tmp location has changed.
    old_env = os.getenv(RAY_TEMP_PATH_ENV)
    os.environ[RAY_TEMP_PATH_ENV] = str(shared_ray_conn._temp_dir)

    config = LOCAL_RUNTIME_CONFIGURATION
    client = _client.RayClient()
    rt = _dag.RayRuntime(config, client)

    yield rt

    if old_env is None:
        del os.environ[RAY_TEMP_PATH_ENV]
    else:
        os.environ[RAY_TEMP_PATH_ENV] = old_env


def _poll_loop(
    continue_condition: t.Callable[[], bool], interval: float, timeout: float
):
    start_time = time.time()
    while continue_condition():
        if time.time() - start_time > timeout:
            raise TimeoutError("Polling timed out")

        time.sleep(interval)


def _wait_to_finish_wf(run_id: str, runtime: RuntimeInterface, timeout=20.0):
    def _continue_condition():
        state = runtime.get_workflow_run_status(run_id).status.state
        return state in [
            State.WAITING,
            State.RUNNING,
        ]

    _poll_loop(_continue_condition, interval=0.1, timeout=timeout)


def _count_task_runs(wf_run, state: State) -> int:
    return len([run for run in wf_run.task_runs if run.status.state == state])


@pytest.mark.slow
class TestRayRuntimeMethods:
    """
    Tests that call RayRuntime methods with a real Ray connection and
    validate that our glue code over Ray works correctly.
    """

    class TestCreateWorkflowRun:
        """
        Tests that validate .create_workflow_run().
        """

        def test_running_same_workflow_def_twice(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.multioutput_wf.model
            run_id1 = runtime.create_workflow_run(wf_def, None, False)
            run_id2 = runtime.create_workflow_run(wf_def, None, False)

            assert run_id1 != run_id2

            _wait_to_finish_wf(run_id1, runtime)
            _wait_to_finish_wf(run_id2, runtime)
            output1 = runtime.get_workflow_run_outputs_non_blocking(run_id1)
            output2 = runtime.get_workflow_run_outputs_non_blocking(run_id2)

            assert output1 == output2

        def test_sets_context(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.wf_with_exec_ctx().model

            # when
            run_id = runtime.create_workflow_run(wf_def, None, False)

            # then
            _wait_to_finish_wf(run_id, runtime)
            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)
            assert outputs == (JSONResult(value='"RAY"'),)

        def test_sets_run_id_from_env(
            self, monkeypatch: pytest.MonkeyPatch, runtime: _dag.RayRuntime
        ):
            wf_def = _example_wfs.multioutput_wf.model
            # Appending the normal run ID to prevent collisions
            global_wf_run_id = "run_id_from_env" + _dag._generate_wf_run_id(wf_def)
            monkeypatch.setenv("GLOBAL_WF_RUN_ID", global_wf_run_id)

            run_id = runtime.create_workflow_run(wf_def, None, False)

            runtime.stop_workflow_run(run_id)
            assert run_id == global_wf_run_id

        def test_simple_workflow_dry_run(
            self, monkeypatch: pytest.MonkeyPatch, runtime: _dag.RayRuntime
        ):
            wf_def = _example_wfs.exception_wf_with_multiple_values.model
            run_id = runtime.create_workflow_run(wf_def, project=None, dry_run=True)
            _wait_to_finish_wf(run_id, runtime)

            # normally this WF would fail, but as a dry-run, no task code is executed
            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)

            assert outputs == (JSONResult(value='"dry_run task output"'),)

        def test_unpacking_workflow_dry_run(
            self, monkeypatch: pytest.MonkeyPatch, runtime: _dag.RayRuntime
        ):
            wf_def = _example_wfs.multioutput_task_failed_wf.model
            run_id = runtime.create_workflow_run(wf_def, project=None, dry_run=True)
            _wait_to_finish_wf(run_id, runtime)

            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)

            res = JSONResult(value='"dry_run task output"')
            assert outputs == (
                res,
                res,
                res,
                res,
                JSONResult(
                    value='{"__tuple__": true, "__values__": ["dry_run task output"'
                    ', "dry_run task output"]}'
                ),
                res,
                res,
            )

    @pytest.mark.parametrize("trial", range(5))
    @pytest.mark.usefixtures("trial")
    class TestGetWorkflowRunStatus:
        """
        Tests that validate .get_workflow_run_status().
        """

        def test_status_right_after_start(self, runtime: _dag.RayRuntime):
            """
            Verifies that we report status correctly before workflow ends.
            It's difficult to synchronize, so there's a bunch of ifs in this
            test.
            """
            # Given
            wf_def = _example_wfs.greet_wf.model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            # When
            run = runtime.get_workflow_run_status(run_id)

            # Then
            assert run.id == run_id
            assert run.status.state in {State.WAITING, State.RUNNING, State.SUCCEEDED}
            assert run.status.start_time is not None
            assert run.workflow_def == wf_def

            if run.status.state == State.WAITING:
                assert run.status.start_time is None
                assert run.status.end_time is None
            elif run.status.state == State.RUNNING:
                assert run.status.start_time is not None
                assert run.status.end_time is None
            elif run.status.state == State.SUCCEEDED:
                assert run.status.start_time is not None
                assert run.status.end_time is not None

            for task_run in run.task_runs:
                assert task_run.invocation_id in wf_def.task_invocations.keys()

                status = task_run.status
                assert status.state in {State.WAITING, State.RUNNING, State.SUCCEEDED}

                if status.state == State.WAITING:
                    assert status.start_time is None
                    assert status.end_time is None
                elif status.state == State.RUNNING:
                    assert status.start_time is not None
                    assert status.end_time is None
                elif status.state == State.SUCCEEDED:
                    assert status.start_time is not None
                    assert status.end_time is not None

        def test_status_after_awaiting(self, runtime: _dag.RayRuntime):
            """
            Verifies that we report status correctly when all tasks are
            completed.

            Note: Ray sometimes suffers from race conditions between blocking
            on the output, and returning workflow status "completed". This is
            why this test is repeated a couple of times.
            """
            # Given
            wf_def = _example_wfs.greet_wf.model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            # Block until wf completes
            _wait_to_finish_wf(run_id, runtime)

            # When
            run = runtime.get_workflow_run_status(run_id)

            # Then
            assert run.id == run_id
            assert (
                run.status.state == State.SUCCEEDED
            ), f"Invalid state. Full status: {run.status}. Task runs: {run.task_runs}"
            assert run.status.start_time is not None
            assert run.status.end_time is not None
            assert run.workflow_def == wf_def

            for task_run in run.task_runs:
                assert task_run.invocation_id in wf_def.task_invocations.keys()

                status = task_run.status
                assert status.state == State.SUCCEEDED

                assert status.start_time is not None
                assert status.end_time is not None

                assert status.start_time.tzinfo is not None
                assert status.end_time.tzinfo is not None

                # On Windows - timer resolution might not detect the change in time
                # during start and finish of a task. Thus it is >=, not >
                assert (status.end_time - status.start_time).total_seconds() >= 0

        @freeze_time("9999-01-01")
        def test_normalizes_end_times(self, runtime: _dag.RayRuntime):
            # Given
            wf_def = _example_wfs.serial_wf_with_slow_middle_task.model
            run_id = runtime.create_workflow_run(wf_def, None, False)
            runtime.stop_workflow_run(run_id)

            # Block until wf completes
            _wait_to_finish_wf(run_id, runtime)

            # When
            run = runtime.get_workflow_run_status(run_id)

            # Then
            assert run.id == run_id
            assert (
                run.status.state == State.TERMINATED
            ), f"Invalid state. Full status: {run.status}. Task runs: {run.task_runs}"
            assert run.status.start_time is not None
            assert run.status.end_time is not None
            assert run.workflow_def == wf_def

            # The added end times should be 'now'
            assert run.status.end_time == datetime(9999, 1, 1, tzinfo=timezone.utc)
            # freeze_time doesn't affect the ray process, so the start times
            # are real and therefore earlier than the end time (if this is
            # untrue, hello from the distant past).
            assert run.status.start_time < run.status.end_time

            for task_run in run.task_runs:
                status = task_run.status

                # We're only interested in tasks that were terminated while in process.
                if not (
                    task_run.status.state == State.TERMINATED
                    and status.start_time is not None
                ):
                    continue

                assert status.end_time is not None
                assert status.start_time.tzinfo is not None
                assert status.end_time.tzinfo is not None
                assert (status.end_time - status.start_time).total_seconds() >= 0
                assert task_run.status.end_time == datetime(
                    9999, 1, 1, tzinfo=timezone.utc
                )

        @pytest.mark.skipif(
            sys.platform.startswith("win32"), reason="File writing on windows is slow."
        )
        def test_handles_ray_environment_setup_error(
            self, runtime: _dag.RayRuntime, shared_ray_conn
        ):
            # Given
            wf_def = _example_wfs.cause_env_setup_error.model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            # Block until wf completes
            _wait_to_finish_wf(run_id, runtime, timeout=100)

            # When
            run = runtime.get_workflow_run_status(run_id)

            # Then
            assert (
                run.status.state == State.FAILED
            ), f"Invalid state. Full status: {run.status}. Task runs: {run.task_runs}"
            # Grab the logs so we can debug when the test fails
            logs = _ray_logs.DirectLogReader(
                Path(shared_ray_conn._temp_dir)
            ).get_workflow_logs(wf_run_id=run_id)

            re_pattern = (
                r"Could not set up runtime environment "
                r"\('pip\.py:\d* -- Failed to install pip packages'\)\. "
                r"See environment setup logs for details. "
                r"`orq wf logs " + re.escape(str(run_id)) + r" --env-setup`"
            )

            assert re.match(re_pattern, str(run.message)), (
                f"\n-MESSAGE: {run.message}"
                f"\n-ENV SETUP OUT:\n{logs.env_setup.out}"
                f"\n-ENV SETUP ERR:\n{logs.env_setup.err}"
                f"\n-OTHER:\n{logs}"
            )

        def test_exception_in_task_stops_execution(self, runtime: _dag.RayRuntime):
            """
            The workflow graph:

               [1]
                │
                ▼
               [2] => exception
                │
                ▼
               [3] => won't run
                │
                ▼
            [return]
            """
            wf_def = _example_wfs.exception_wf_with_multiple_values.model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            wf_run = runtime.get_workflow_run_status(run_id)

            assert wf_run.status.state == State.FAILED
            assert _count_task_runs(wf_run, State.FAILED) == 1
            assert _count_task_runs(wf_run, State.SUCCEEDED) == 1
            assert _count_task_runs(wf_run, State.WAITING) == 1

    class TestListWorkflowRuns:
        """
        Tests that validate .list_workflow_runs().
        """

        def test_two_runs(self, runtime: _dag.RayRuntime):
            # Given
            wf_def = _example_wfs.greet_wf.model
            run_id1 = runtime.create_workflow_run(wf_def, None, False)
            run_id2 = runtime.create_workflow_run(wf_def, None, False)

            # When
            wf_runs = runtime.list_workflow_runs()

            # Then
            wf_run_ids = {run.id for run in wf_runs}
            assert run_id1 in wf_run_ids
            assert run_id2 in wf_run_ids
            assert all(isinstance(run, _dag.WorkflowRun) for run in wf_runs)

    class TestStopWorkflow:
        """
        Tests that validate .stop_workflow_run()
        """

        def test_on_running_workflow(self, runtime: _dag.RayRuntime, tmp_path):
            """
            Workflow graph in the scenario under test:
              [ ]  => in progress
               │
               │
               ▼
            """

            wf = _example_wfs.infinite_workflow().model

            wf_run_id = runtime.create_workflow_run(wf, None, False)
            wf_run = runtime.get_workflow_run_status(wf_run_id)
            assert wf_run.status.state == State.RUNNING

            runtime.stop_workflow_run(wf_run_id)

            # ray should cancel workflow synchronously, but just in case it doesn't
            # let's give a workflow some time to change its state
            timeout = time.time() + 30  # now + 30 seconds
            while True:
                if time.time() > timeout:
                    assert False, "timeout while waiting for workflow termination"
                wf_run = runtime.get_workflow_run_status(wf_run_id)
                if wf_run.status.state != State.RUNNING:
                    assert wf_run.status.state == State.TERMINATED
                    break

        @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
        def test_on_finished_workflow(self, runtime: _dag.RayRuntime, tmp_path):
            wf = _example_wfs.multioutput_task_wf.model
            wf_run_id = runtime.create_workflow_run(wf, None, False)
            _wait_to_finish_wf(wf_run_id, runtime)
            # ensure wf has finished
            status = runtime.get_workflow_run_status(wf_run_id)
            assert status.status.state == State.SUCCEEDED

            runtime.stop_workflow_run(wf_run_id)
            status = runtime.get_workflow_run_status(wf_run_id)
            # cancel changes the state to terminated
            assert status.status.state == State.TERMINATED

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
    class TestGetWorkflowRunOutputsNonBlocking:
        """
        Tests that validate get_workflow_run_outputs_non_blocking
        """

        def test_happy_path(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.greet_wf.model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)

            assert outputs == (
                JSONResult(value='"yooooo emiliano from zapata computing"'),
            )

        def test_failed_workflow(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.exception_wf_with_multiple_values().model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            with pytest.raises(exceptions.WorkflowRunNotSucceeded):
                runtime.get_workflow_run_outputs_non_blocking(run_id)

        def test_in_progress_workflow(self, runtime: _dag.RayRuntime, tmp_path):
            """
            Workflow graph in the scenario under test:
              [ ]  => in progress
               │
               │
               ▼
              [ ]  => waiting
               │
               │
               ▼
            """
            triggers = [_ipc.TriggerServer() for _ in range(2)]

            wf = _example_wfs.serial_wf_with_file_triggers(
                [trigger.port for trigger in triggers], task_timeout=5.0
            ).model
            run_id = runtime.create_workflow_run(wf, None, False)

            assert runtime.get_workflow_run_status(run_id).status.state == State.RUNNING
            with pytest.raises(exceptions.WorkflowRunNotSucceeded):
                runtime.get_workflow_run_outputs_non_blocking(run_id)

            for trigger in triggers:
                trigger.trigger()
                trigger.close()

    class TestGetAvailableOutputs:
        """
        Tests that validate .get_available_outputs().
        """

        def test_after_a_failed_task(self, runtime: _dag.RayRuntime):
            """
            The workflow graph:

               [1]
                │
                ▼
               [2] => exception
                │
                ▼
               [3] => won't run
                │
                ▼
            [return]
            """
            wf_def = _example_wfs.exception_wf_with_multiple_values().model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            wf_run = runtime.get_workflow_run_status(run_id)
            inv_artifacts = runtime.get_available_outputs(run_id)
            if wf_run.status.state != State.FAILED:
                pytest.fail(
                    "The workflow was supposed to fail, but it didn't. "
                    f"Wf run: {wf_run}"
                )

            # Expect only one finished task invocation.
            assert len(inv_artifacts) == 1

            # The outputs dict has an entry for each task invocation. The value is
            # whatever the task returned; in this example it's a single int.
            task_output = list(inv_artifacts.values())[0]
            assert task_output == JSONResult(value="58")

        @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
        def test_after_one_task_finishes(self, runtime: _dag.RayRuntime, tmp_path):
            """
            Workflow graph in the scenario under test:
              [ ]  => finished
               │
               │
               ▼
              [ ]  => in progress
               │
               │
               ▼
              [ ]  => waiting
               │
               │
               ▼
            """
            triggers = [_ipc.TriggerServer() for _ in range(3)]

            wf = _example_wfs.serial_wf_with_file_triggers(
                [trigger.port for trigger in triggers], task_timeout=10.0
            ).model
            wf_run_id = runtime.create_workflow_run(wf, None, False)

            triggers[0].trigger()
            # Await completion of the first task
            loop_start = time.time()
            while True:
                wf_run = runtime.get_workflow_run_status(wf_run_id)
                if wf_run.status.state not in {State.RUNNING, State.SUCCEEDED}:
                    pytest.fail(f"Unexpected wf run state: {wf_run}")

                succeeded_runs = [
                    run
                    for run in wf_run.task_runs
                    if run.status.state == State.SUCCEEDED
                ]
                if len(succeeded_runs) >= 1:
                    break

                if time.time() - loop_start > 20.0:
                    pytest.fail(
                        f"Timeout when awaiting for workflow finish. Full run: {wf_run}"
                    )

                time.sleep(0.2)

            # When
            outputs_dict = runtime.get_available_outputs(wf_run_id)

            # Then
            assert len(outputs_dict) == 1
            assert len(succeeded_runs) == 1

            # let the workers complete the workflow
            triggers[1].trigger()
            triggers[2].trigger()

            for trigger in triggers:
                trigger.close()

    class TestGetOutput:
        """
        Tests that validate .get_output().
        """

        def test_after_a_failed_task(self, runtime: _dag.RayRuntime):
            """
            The workflow graph:

               [1]
                │
                ▼
               [2] => exception
                │
                ▼
               [3] => won't run
                │
                ▼
            [return]
            """
            wf_def = _example_wfs.exception_wf_with_multiple_values().model
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            wf_run = runtime.get_workflow_run_status(run_id)
            invocation_ids = [task.invocation_id for task in wf_run.task_runs]

            if wf_run.status.state != State.FAILED:
                pytest.fail(
                    "The workflow was supposed to fail, but it didn't. "
                    f"Wf run: {wf_run}"
                )

            assert runtime.get_output(run_id, invocation_ids[2]) == JSONResult(
                value="58"
            )
            with pytest.raises(exceptions.NotFoundError):
                assert not runtime.get_output(run_id, invocation_ids[1])
            with pytest.raises(exceptions.NotFoundError):
                assert not runtime.get_output(run_id, invocation_ids[0])

        @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
        def test_after_one_task_finishes(self, runtime: _dag.RayRuntime, tmp_path):
            """
            Workflow graph in the scenario under test:
              [ ]  => finished
               │
               │
               ▼
              [ ]  => in progress
               │
               │
               ▼
              [ ]  => waiting
               │
               │
               ▼
            """
            triggers = [_ipc.TriggerServer() for _ in range(3)]

            wf = _example_wfs.serial_wf_with_file_triggers(
                [trigger.port for trigger in triggers], task_timeout=10.0
            ).model
            wf_run_id = runtime.create_workflow_run(wf, None, False)

            triggers[0].trigger()
            # Await completion of the first task
            loop_start = time.time()
            while True:
                wf_run = runtime.get_workflow_run_status(wf_run_id)
                if wf_run.status.state not in {State.RUNNING, State.SUCCEEDED}:
                    pytest.fail(f"Unexpected wf run state: {wf_run}")

                succeeded_runs = [
                    run
                    for run in wf_run.task_runs
                    if run.status.state == State.SUCCEEDED
                ]
                if len(succeeded_runs) >= 1:
                    break

                if time.time() - loop_start > 20.0:
                    pytest.fail(
                        f"Timeout when awaiting for workflow finish. Full run: {wf_run}"
                    )

                time.sleep(0.2)
            invocation_ids = [task.invocation_id for task in wf_run.task_runs]

            assert runtime.get_output(wf_run_id, invocation_ids[2]) == JSONResult(
                value="58"
            )
            with pytest.raises(exceptions.NotFoundError):
                assert not runtime.get_output(wf_run_id, invocation_ids[1])
            with pytest.raises(exceptions.NotFoundError):
                assert not runtime.get_output(wf_run_id, invocation_ids[0])

            # let the workers complete the workflow
            triggers[1].trigger()
            triggers[2].trigger()

            for trigger in triggers:
                trigger.close()

        @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
        def test_multioutput_task(self, runtime: _dag.RayRuntime, tmp_path):
            wf = _example_wfs.multioutput_task_wf().model
            wf_run_id = runtime.create_workflow_run(wf, None, False)

            _wait_to_finish_wf(wf_run_id, runtime)

            wf_run = runtime.get_workflow_run_status(wf_run_id)

            invocation_ids = [task.invocation_id for task in wf_run.task_runs]
            expected_result = (
                '{"__tuple__": true, "__values__": ["Zapata", "Computing"]}'
            )

            artifacts = [
                runtime.get_output(wf_run_id, inv_id) for inv_id in invocation_ids
            ]

            assert len(artifacts) == 4
            assert all(
                [
                    artifact == JSONResult(value=expected_result)
                    for artifact in artifacts
                ]
            )

            assert all(
                [
                    deserialize(artifact) == ("Zapata", "Computing")
                    for artifact in artifacts
                ]
            )


@pytest.mark.slow
@pytest.mark.parametrize(
    "wf,expected_outputs,expected_intermediate",
    [
        (
            _example_wfs.greet_wf,
            (JSONResult(value='"yooooo emiliano from zapata computing"'),),
            {
                "invocation-0-task-make-greeting": (
                    JSONResult(value='"yooooo emiliano from zapata computing"')
                )
            },
        ),
        (
            _example_wfs.complicated_wf,
            (JSONResult(value='"yooooo emiliano Zapata from Zapata computing"'),),
            {
                "invocation-0-task-make-greeting": (
                    JSONResult(value='"yooooo emiliano Zapata from Zapata computing"')
                ),
                "invocation-1-task-capitalize": JSONResult(value='"Zapata computing"'),
                "invocation-2-task-concat": JSONResult(value='"emiliano Zapata"'),
                "invocation-3-task-capitalize": JSONResult(value='"Zapata"'),
            },
        ),
        (
            _example_wfs.multioutput_wf,
            (
                JSONResult(value='"Emiliano Zapata"'),
                JSONResult(value='"Zapata computing"'),
            ),
            {
                "invocation-0-task-capitalize": JSONResult(value='"Zapata computing"'),
                "invocation-1-task-make-company-name": JSONResult(
                    value='"zapata computing"'
                ),
                "invocation-2-task-concat": JSONResult(value='"Emiliano Zapata"'),
                "invocation-3-task-capitalize": JSONResult(value='"Zapata"'),
            },
        ),
        (
            _example_wfs.multioutput_task_wf,
            (
                # Unpacked outputs
                JSONResult(value='"Zapata"'),
                JSONResult(value='"Computing"'),
                # First output discarded
                JSONResult(value='"Computing"'),
                # Second output discarded
                JSONResult(value='"Zapata"'),
                # Returning both packed and unpacked outputs
                JSONResult(
                    value='{"__tuple__": true, "__values__": ["Zapata", "Computing"]}'
                ),
                JSONResult(value='"Zapata"'),
                JSONResult(value='"Computing"'),
            ),
            {
                # We expect all task outputs for each task invocation, regardless of
                # unpacking in the workflow. For more info, see
                # `RuntimeInterface.get_available_outputs()`.
                "invocation-0-task-multioutput-task": JSONResult(
                    value='{"__tuple__": true, "__values__": ["Zapata", "Computing"]}'
                ),
                "invocation-1-task-multioutput-task": JSONResult(
                    value='{"__tuple__": true, "__values__": ["Zapata", "Computing"]}'
                ),
                "invocation-2-task-multioutput-task": JSONResult(
                    value='{"__tuple__": true, "__values__": ["Zapata", "Computing"]}'
                ),
                "invocation-3-task-multioutput-task": JSONResult(
                    value='{"__tuple__": true, "__values__": ["Zapata", "Computing"]}'
                ),
            },
        ),
        (
            _example_wfs.greet_wf_kw,
            (JSONResult(value='"yooooo emiliano from zapata computing"'),),
            {
                "invocation-0-task-make-greeting": (
                    JSONResult(value='"yooooo emiliano from zapata computing"')
                )
            },
        ),
        (
            _example_wfs.wf_using_inline_imports,
            (
                JSONResult(value='"Emiliano Zapata"'),
                JSONResult(value='"Zapata computing"'),
            ),
            {
                "invocation-0-task-capitalize-inline": JSONResult(
                    value='"Zapata computing"'
                ),
                "invocation-1-task-make-company-name": JSONResult(
                    value='"zapata computing"'
                ),
                "invocation-2-task-concat": JSONResult(value='"Emiliano Zapata"'),
                "invocation-3-task-capitalize-inline": JSONResult(value='"Zapata"'),
            },
        ),
        (
            _example_wfs.wf_with_explicit_n_outputs,
            (JSONResult(value="true"),),
            {
                "invocation-0-task-task-with-single-output-explicit": JSONResult(
                    value="true"
                ),
            },
        ),
    ],
)
def test_run_and_get_output(
    runtime: _dag.RayRuntime, wf, expected_outputs, expected_intermediate
):
    """
    Verifies methods for getting outputs, both the "final" and "intermediate".
    """
    # Given
    run_id = runtime.create_workflow_run(wf.model, None, False)
    _wait_to_finish_wf(run_id, runtime)

    # When
    wf_results = runtime.get_workflow_run_outputs_non_blocking(run_id)
    intermediate_outputs = runtime.get_available_outputs(run_id)

    # Then
    assert wf_results == expected_outputs
    assert intermediate_outputs == expected_intermediate


# These tests are slow to run locally because:
# - It installs packages inside a separated venv. This includes cloning the driver
#   process' venv.
# - Installing packages takes some time in general.
# - Installing packages on company machines is very slow because of the antivirus
#   scanning.
@pytest.mark.slow
class Test3rdPartyLibraries:
    @staticmethod
    # We're reading a serialized workflow def. The SDK version inside that JSON is
    # likely to be different from the one we're using for development. The SDK shows a
    # warning when deserializing a workflow def like this.
    @pytest.mark.filterwarnings("ignore::orquestra.sdk.exceptions.VersionMismatch")
    @pytest.mark.skipif(
        sys.version_info < (3, 11), reason="Pickle internals changed in Python 3.11"
    )
    def test_constants_and_inline_imports(runtime: _dag.RayRuntime):
        """
        This test uses already generated workflow def from json file. If necessary, it
        can be recreated using ./data/python_package/original_workflow.py

        This function tests 2 things
        1. Proper package installation. Python Package defined in task definition should
           only be installed inside venv of ray task and shouldn't leak into the main
           env.
        2. We can embed constants and inline import that depend on 3rd-party libraries
           This checks that we don't need python package outside an actual task venv to
           deal with package-dependent constant values.

        We test those 2 things together as package installation is long, and we don't
        want to do that twice unnecessarily.
        """
        # Given
        # This package should not be installed before running test
        with pytest.raises(ModuleNotFoundError):
            import polars  # type: ignore # noqa

        path_to_json = Path(__file__).parent.joinpath(
            "data/python_package/python_package_dependent_workflow.json"
        )
        data = path_to_json.read_text()
        wf = ir.WorkflowDef.model_validate_json(data)

        # When
        run_id = runtime.create_workflow_run(wf, None, False)
        # This test is notoriously slow to run, especially on local machines.
        _wait_to_finish_wf(run_id, runtime, timeout=10 * 60.0)
        wf_result = runtime.get_workflow_run_outputs_non_blocking(run_id)

        # Then
        assert wf_result == (JSONResult(value=("21")),)

        # this package should be only used inside ray env
        with pytest.raises(ModuleNotFoundError):
            import polars  # type: ignore # noqa

    @staticmethod
    def test_git_import_inside_ray(monkeypatch, runtime: _dag.RayRuntime):
        """
        Verifies we can run tasks with GitImport dependencies not available at the time
        of workflow submit time.
        """
        # Given
        # This package should not be installed before running test
        with pytest.raises(ModuleNotFoundError):
            import piccup  # type: ignore # noqa

        monkeypatch.setenv(name="ORQ_RAY_DOWNLOAD_GIT_IMPORTS", value="1")

        # When
        run_id = runtime.create_workflow_run(
            _example_wfs.wf_using_git_imports.model, None, False
        )
        # This test is notoriously slow to run, especially on local machines.
        _wait_to_finish_wf(run_id, runtime, timeout=10 * 60.0)
        wf_result = runtime.get_workflow_run_outputs_non_blocking(run_id)

        # Then
        assert wf_result == (JSONResult(value=("2")),)

        # this package should be only used inside ray env
        with pytest.raises(ModuleNotFoundError):
            import piccup  # type: ignore # noqa

    @staticmethod
    def test_3rd_party_library_exception(monkeypatch, runtime: _dag.RayRuntime, capsys):
        """
        Verifies we can run tasks with GitImport dependencies not available at the time
        of workflow submit time.
        """
        # Given
        # This package should not be installed before running test
        with pytest.raises(ModuleNotFoundError):
            import inflect  # type: ignore # noqa

        # When
        run_id = runtime.create_workflow_run(
            _example_wfs.workflow_throwing_3rd_party_exception.model, None, False
        )
        # This test is notoriously slow to run, especially on local machines.
        _wait_to_finish_wf(run_id, runtime, timeout=10 * 60.0)

        # Then
        with pytest.raises(exceptions.WorkflowRunNotSucceeded):
            runtime.get_workflow_run_outputs_non_blocking(run_id)

        captured_stderr = capsys.readouterr().err

        # this package should be only used inside ray env
        assert "Failed to unpickle serialized exception" not in captured_stderr
        assert "No module named" not in captured_stderr
        # we should be able to see the original exception in the logs.
        assert "inflect.BadChunkingOptionError" in captured_stderr

        with pytest.raises(ModuleNotFoundError):
            import inflect  # type: ignore # noqa


@pytest.mark.slow
class TestRayRuntimeErrors:
    @pytest.mark.parametrize(
        "method",
        [
            _dag.RayRuntime.get_available_outputs,
            _dag.RayRuntime.get_workflow_run_outputs_non_blocking,
            _dag.RayRuntime.get_workflow_run_status,
            _dag.RayRuntime.stop_workflow_run,
        ],
    )
    def test_invalid_run_id(self, runtime: _dag.RayRuntime, method):
        run_id = "I hope this doesn't exist"

        with pytest.raises(exceptions.NotFoundError):
            _ = method(runtime, run_id)


def _run_and_await_wf(
    runtime: RuntimeInterface, wf: ir.WorkflowDef, timeout: float = 10.0
) -> WorkflowRunId:
    run_id = runtime.create_workflow_run(wf, project=None, dry_run=False)
    _wait_to_finish_wf(run_id, runtime, timeout=timeout)

    return run_id


@pytest.mark.slow
class TestDirectLogReader:
    """
    Verifies that our code can read log files produced by Ray.
    This class tests reading Ray log files directly; it doesn't test the solution based
    on FluentBit.

    The tests' boundary: `[DirectLogReader]-[task code]`
    """

    class TestGetWorkflowLogs:
        @staticmethod
        @pytest.mark.parametrize(
            "wf,tell_tale_out,tell_tale_err",
            [
                (
                    _example_wfs.wf_with_log(msg="hello, there!").model,
                    "hello, there!",
                    None,
                ),
                (
                    _example_wfs.exception_wf.model,
                    None,
                    "ZeroDivisionError: division by zero",
                ),
            ],
        )
        def test_per_task_content(
            shared_ray_conn,
            runtime,
            wf: ir.WorkflowDef,
            tell_tale_out: t.Optional[str],
            tell_tale_err: t.Optional[str],
        ):
            """
            Submit a workflow, wait for it to finish, get wf logs, look for the test
            message.
            """
            # Given
            ray_params = shared_ray_conn
            wf_run_id = _run_and_await_wf(runtime, wf)
            reader = _ray_logs.DirectLogReader(Path(ray_params._temp_dir))

            # When
            logs = reader.get_workflow_logs(wf_run_id=wf_run_id)

            # Then
            stdout_lines_joined = "".join(
                log_line
                for task_log_lines in logs.per_task.values()
                for log_line in task_log_lines.out
            )

            stderr_lines_joined = "".join(
                log_line
                for task_log_lines in logs.per_task.values()
                for log_line in task_log_lines.err
            )

            if tell_tale_out is not None:
                assert tell_tale_out in stdout_lines_joined

            if tell_tale_err is not None:
                assert tell_tale_err in stderr_lines_joined

        @staticmethod
        def test_env_setup_content(shared_ray_conn, runtime: _dag.RayRuntime):
            """
            Submit a workflow, wait for it to finish, get wf logs, look into env set up.
            """
            # Given
            ray_params = shared_ray_conn
            wf_def = _example_wfs.wf_using_python_imports(
                log_message="hello, there!"
            ).model
            # This workflow includes setting up specialized venv by Ray, so it's slow.
            wf_run_id = _run_and_await_wf(runtime, wf_def, timeout=10.0 * 60)
            reader = _ray_logs.DirectLogReader(Path(ray_params._temp_dir))

            # When
            logs = reader.get_workflow_logs(wf_run_id=wf_run_id)

            # Then
            assert len(logs.env_setup) > 0

            for tell_tale in ["Cloning virtualenv", "'pip', 'install'"]:
                matched_lines = [
                    line for line in logs.env_setup.out if tell_tale in line
                ]
                assert (
                    len(matched_lines) > 0
                ), f"No match. Full output:\n{logs.env_setup.out}"

    @staticmethod
    @pytest.mark.parametrize(
        "wf,tell_tale_out,tell_tale_err",
        [
            (
                _example_wfs.wf_with_log(msg="hello, there!").model,
                "hello, there!",
                None,
            ),
            (
                _example_wfs.exception_wf.model,
                None,
                "ZeroDivisionError: division by zero",
            ),
        ],
    )
    def test_get_task_logs(
        shared_ray_conn,
        runtime,
        wf: ir.WorkflowDef,
        tell_tale_out: t.Optional[str],
        tell_tale_err: t.Optional[str],
    ):
        """
        Submit a workflow, wait for it to finish, get task logs, look for the test
        message.
        """
        # Given
        ray_params = shared_ray_conn
        reader = _ray_logs.DirectLogReader(Path(ray_params._temp_dir))
        all_inv_ids = list(wf.task_invocations.keys())
        wf_run_id = _run_and_await_wf(runtime, wf)

        # When
        logs = reader.get_task_logs(wf_run_id=wf_run_id, task_inv_id=all_inv_ids[0])

        # Then
        stdout_lines_joined = "\n".join(logs.out)
        stderr_lines_joined = "\n".join(logs.err)
        if tell_tale_out is not None:
            assert tell_tale_out in stdout_lines_joined

        if tell_tale_err is not None:
            assert tell_tale_err in stderr_lines_joined


@pytest.mark.slow
def test_ray_direct_reader_no_duplicate_lines(
    shared_ray_conn,
    runtime,
):
    """
    This ensures that `session_latest` and `session_<current date>` are not searched
    twice.
    This is separate to the `TestDirectLogReader` tests because searching without the
    run id may show duplicate logs if different workflows print the same thing.
    """
    # Given
    wf = _example_wfs.wf_with_log("Unique log line").model
    tell_tale = "Unique log line"
    ray_params = shared_ray_conn
    reader = _ray_logs.DirectLogReader(Path(ray_params._temp_dir))

    run_id = runtime.create_workflow_run(wf, None, False)
    _wait_to_finish_wf(run_id, runtime)

    # When
    logs = reader.get_workflow_logs(wf_run_id=run_id)

    # Then
    # First check for the tell_tale in all log lines
    matches = [
        tell_tale in log_line
        for task_log_lines in logs.per_task.values()
        for log_line in task_log_lines.out
    ]

    # Assert the tell_tale was only found once
    assert matches.count(True) == 1


@pytest.mark.slow
def test_task_code_unavailable_at_building_dag(runtime: _dag.RayRuntime):
    """
    Verifies that the task code is required only at the task-execution time, and it is
    not required during building of the graph. If the source code of a task would be
    required during building DAG, create_workflow_run function would fail.
    In this test we expect that the tasks are started, but fail due to the missing task.
    This test is important for workflow-driver-ray, as tasks are installed on venvs
    of each individual task workers, not on global environment
    """
    # given
    wf_def = _example_wfs.greet_wf.model
    task_id = list(wf_def.tasks.keys())[0]

    # ignoring mypy, this is supposed to be inline fn
    wf_def.tasks[task_id].fn_ref.encoded_function = ["nope"]  # type: ignore

    # when
    # If this fails it means we try to deserialize function at DAG-create time
    # which is bad
    wf_id = runtime.create_workflow_run(wf_def, None, False)
    _wait_to_finish_wf(wf_id, runtime)

    # then
    assert runtime.get_workflow_run_status(wf_id).status.state == State.FAILED
    assert (
        runtime.get_workflow_run_status(wf_id).task_runs[0].status.state == State.FAILED
    )


@pytest.mark.slow
class TestGetCurrentIDs:
    def test_during_running_workflow(self, runtime: _dag.RayRuntime, tmp_path: Path):
        # Given
        output_path = (tmp_path / "output.json").absolute()

        @sdk.task(source_import=sdk.InlineImport())
        def dump_ids():
            # Separate import just to avoid weird global state passing via closure.
            from orquestra.workflow_runtime import get_current_ids

            (
                wf_run_id,
                task_inv_id,
                task_run_id,
            ) = get_current_ids()

            ids_dict = {
                "wf_run_id": wf_run_id,
                "task_inv_id": task_inv_id,
                "task_run_id": task_run_id,
            }

            output_path.write_text(json.dumps(ids_dict))

            return "done"

        @sdk.workflow
        def wf():
            return dump_ids()

        wf_model = wf().model

        # When
        # The function-under-test is called inside the workflow.
        wf_run_id = runtime.create_workflow_run(wf_model, None, False)
        _wait_to_finish_wf(wf_run_id, runtime)

        # Precondition
        wf_run = runtime.get_workflow_run_status(wf_run_id)
        assert wf_run.status.state == State.SUCCEEDED

        # Then
        ids_dict = json.loads(output_path.read_text())
        assert ids_dict["wf_run_id"] == wf_run_id

        task_inv_ids = list(wf_model.task_invocations.keys())
        assert ids_dict["task_inv_id"] in task_inv_ids

        task_run_ids = [task_run.id for task_run in wf_run.task_runs]
        assert ids_dict["task_run_id"] in task_run_ids

    def test_outside_workflow(self):
        # When
        ids = _build_workflow.get_current_ids()

        # Then
        assert ids == (None, None, None)


@pytest.mark.slow
class TestDictReturnValue:
    """
    Tasks that had dicts in return statement used to cause WF failures.
    This test might look trivial bul unless AST code was majorly refactored,
    please abstain from removing it (also refer to git history for the fix itself)
    """

    def test_dict_as_task_return_value(self, runtime: _dag.RayRuntime):
        @sdk.task
        def returns_dict():
            return {"a": "b", "c": "d"}

        @sdk.workflow
        def wf():
            return returns_dict()

        wf_model = wf().model

        # When
        # The function-under-test is called inside the workflow.
        wf_run_id = runtime.create_workflow_run(wf_model, project=None, dry_run=False)
        _wait_to_finish_wf(wf_run_id, runtime)

        # Precondition
        wf_run = runtime.get_workflow_run_status(wf_run_id)
        assert wf_run.status.state == State.SUCCEEDED


@pytest.mark.slow
class TestGraphComplexity:
    """
    Ray <2.9 had an issue where certain workflows with high graph complexity would
    evaluate exponentially long. This test ensures that we can resolve high-complexity
    graph workflows in reasonable time.
    """

    @pytest.mark.timeout(20)
    def test_high_graph_complexity_workflow(self, runtime: _dag.RayRuntime):
        @sdk.task
        def generic_task(*args):
            return 42

        @sdk.workflow
        def wf():
            reduced = generic_task()
            for _ in range(10):
                fanned_out = [generic_task(reduced) for _ in range(10)]
                reduced = generic_task(*fanned_out)
            return reduced

        wf_model = wf().model

        # When
        # The function-under-test is called inside the workflow.
        wf_run_id = runtime.create_workflow_run(wf_model, project=None, dry_run=False)

        _wait_to_finish_wf(wf_run_id, runtime)
        # Precondition
        wf_run = runtime.get_workflow_run_status(wf_run_id)
        assert wf_run.status.state == State.SUCCEEDED


@pytest.mark.slow
class TestRetries:
    """
    Test that retrying Ray Workers works properly
    """

    @pytest.mark.parametrize(
        "max_retries,should_fail",
        [
            (1, False),  # we should not fail with max_retries enabled
            (50, False),  # we should not fail with max_retries enabled
            (0, True),  # 0 means do not retry
            (None, True),  # We do not enable max_retries by default
        ],
    )
    def test_max_retries(self, runtime: _dag.RayRuntime, max_retries, should_fail):
        @sdk.task(max_retries=max_retries)
        def generic_task(*args):
            if hasattr(sdk, "l"):
                sdk.l.extend([0])  # type: ignore # noqa
            else:
                setattr(sdk, "l", [0])
            if len(sdk.l) == 2:  # type: ignore # noqa
                import os
                import signal

                os.kill(os.getpid(), signal.SIGTERM)

            return None

        @sdk.workflow
        def wf():
            task_res = None
            for _ in range(5):
                task_res = generic_task(task_res)
            return task_res

        wf_model = wf().model

        # When
        # The function-under-test is called inside the workflow.
        wf_run_id = runtime.create_workflow_run(wf_model, project=None, dry_run=False)

        # we can't base our logic on SDK workflow status because of:
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-1024
        # We can just look into the message at peek that the workflow actually failed
        # even tho we report is as RUNNING.
        import ray.workflow
        from ray.workflow.common import WorkflowStatus

        no_of_retries = 0

        while True:
            ray_status = ray.workflow.get_status(wf_run_id)
            if no_of_retries >= 30:
                break
            if ray_status == WorkflowStatus.RUNNING:
                time.sleep(1)
                no_of_retries += 1
                continue
            if (
                ray_status == WorkflowStatus.FAILED
                or ray_status == WorkflowStatus.SUCCESSFUL
            ):
                break

        if should_fail:
            assert ray_status == WorkflowStatus.FAILED
        else:
            assert ray_status == WorkflowStatus.SUCCESSFUL


@pytest.mark.slow
class TestEnvVars:
    def test_setting_env_vars_works(self, runtime: _dag.RayRuntime):
        @sdk.task(env_vars={"MY_UNIQUE_ENV": "SECRET"})
        def task():
            import os

            return os.getenv("MY_UNIQUE_ENV")

        @sdk.workflow
        def wf():
            inv1 = task()
            inv2 = task()
            inv3 = task().with_env_variables({"MY_UNIQUE_ENV": "NEW_SECRET"})
            return inv1, inv2, inv3

        wf_model = wf().model
        wf_run_id = runtime.create_workflow_run(wf_model, None, False)
        _wait_to_finish_wf(wf_run_id, runtime)
        results = runtime.get_workflow_run_outputs_non_blocking(wf_run_id)
        artifacts = [res.value for res in results]
        assert len(artifacts) == 3
        assert artifacts.count('"SECRET"') == 2
        assert '"NEW_SECRET"' in artifacts

    @pytest.mark.filterwarnings(
        "ignore::orquestra.sdk._client._base._workflow.NotATaskWarning"
    )
    def test_env_vars_are_set_before_task_executes(self, runtime: _dag.RayRuntime):
        wf = _example_wfs.get_env_before_task_executes_task().model
        wf_run_id = runtime.create_workflow_run(wf, None, False)
        _wait_to_finish_wf(wf_run_id, runtime)
        results = runtime.get_workflow_run_outputs_non_blocking(wf_run_id)
        artifacts = [res.value for res in results]
        assert len(artifacts) == 1
        assert artifacts == ['"MY_UNIQUE_VALUE"']

    def test_env_vars_iteration_with_already_exiting(
        self, runtime: _dag.RayRuntime, monkeypatch
    ):
        @sdk.task(env_vars={"DIFFERENT_ENV": "NOT_OVERWRITTEN"})
        def task_other_env():
            import os

            return os.getenv("MY_NEW_SECRET_ENV")

        @sdk.task(env_vars={"MY_NEW_SECRET_ENV": "OVERWRITTEN"})
        def task_same_env():
            import os

            return os.getenv("MY_NEW_SECRET_ENV")

        @sdk.workflow
        def wf():
            inv1 = task_same_env()  # this one overwrites intentionally the same env_var
            inv2 = task_other_env()  # this one uses different env var
            inv3 = task_other_env().with_env_variables(
                {"MY_NEW_SECRET_ENV": "OVERWRITTEN_IN_WF"}  # overwrites env var
            )
            inv4 = task_same_env().with_env_variables(
                {"DIFFERENT_ENV_VAR": "DIFFERENT_VALUE"}  # does not overwrite env var
            )
            return inv1, inv2, inv3, inv4

        os.environ["MY_NEW_SECRET_ENV"] = "ABC"
        wf_model = wf().model
        wf_run_id = runtime.create_workflow_run(wf_model, None, False)
        _wait_to_finish_wf(wf_run_id, runtime)
        results = runtime.get_workflow_run_outputs_non_blocking(wf_run_id)
        artifacts = [res.value for res in results]
        assert len(artifacts) == 4
        assert '"OVERWRITTEN_IN_WF"' in artifacts  # expected in inv3
        assert '"OVERWRITTEN"' in artifacts  # expected in inv1
        assert artifacts.count('"SET_BEFORE_RAY_STARTS"') == 2  # expected in inv2 and 4


@pytest.mark.slow
class TestGithubImportExtras:
    def test_passing_extras(self, runtime: _dag.RayRuntime, monkeypatch):
        @sdk.task(
            dependency_imports=[
                sdk.GithubImport(
                    repo="SebastianMorawiec/test_repo", package_name="test_repo"
                )
            ]
        )
        def task_no_extra():
            exception_happened = False

            try:
                import polars  # type: ignore # noqa
            except ModuleNotFoundError:
                exception_happened = True

            assert exception_happened

            return 21

        @sdk.task(
            dependency_imports=[
                sdk.GithubImport(
                    repo="SebastianMorawiec/test_repo",
                    package_name="test_repo",
                    extras="polars",
                )
            ]
        )
        def task_with_extra():
            import polars  # type: ignore # noqa

            return 36

        @sdk.workflow
        def wf():
            return task_no_extra(), task_with_extra()

        # Given
        # This package should not be installed before running test
        with pytest.raises(ModuleNotFoundError):
            import polars  # type: ignore # noqa
        monkeypatch.setenv(RAY_DOWNLOAD_GIT_IMPORTS_ENV, "1")

        wf_model = wf().model
        wf_run_id = runtime.create_workflow_run(wf_model, None, False)
        _wait_to_finish_wf(wf_run_id, runtime, timeout=120)

        results = runtime.get_workflow_run_outputs_non_blocking(wf_run_id)

        artifacts = [res.value for res in results]

        assert artifacts == ["21", "36"]
