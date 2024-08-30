import json
import os
import re
import time
import typing as t
from datetime import datetime, timezone
from pathlib import Path

import pydantic
import pytest
from freezegun import freeze_time
from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.abc import RuntimeInterface
from orquestra.workflow_shared.schema.configs import RuntimeConfiguration, RuntimeName
from orquestra.workflow_shared.schema.ir import WorkflowDef
from orquestra.workflow_shared.schema.responses import JSONResult
from orquestra.workflow_shared.schema.workflow_run import State, WorkflowRun
from orquestra.workflow_shared.serde import deserialize

from orquestra.workflow_runtime._ray import _client, _dag, _ray_logs
from orquestra.workflow_runtime._ray._env import RAY_TEMP_PATH_ENV
from orquestra.workflow_runtime._testing import _connections


# @pytest.fixture(autouse=True)
# def patch_orquestra_version(monkeypatch):
#     import orquestra.workflow_shared.packaging._versions
#
#     import orquestra.workflow_runtime._ray._build_workflow
#
#     monkeypatch.setattr(
#         orquestra.workflow_shared.packaging._versions,
#         "get_installed_version",
#         lambda _: "0.64.0",
#     )
#     monkeypatch.setattr(
#         orquestra.workflow_runtime._ray._build_workflow,
#         "get_installed_version",
#         lambda _: "0.64.0",
#     )


@pytest.fixture(autouse=True)
def set_orq_envs(monkeypatch):
    monkeypatch.setenv(name="ORQ_RAY_DOWNLOAD_GIT_IMPORTS", value="1")


@pytest.fixture(scope="module")
def shared_ray_conn():
    with _connections.make_ray_conn() as ray_params:
        yield ray_params


@pytest.fixture(scope="module")
def runtime(
    shared_ray_conn,
):
    # We need to set this env variable to let our logging code know the
    # tmp location has changed.
    old_env = os.getenv(RAY_TEMP_PATH_ENV)
    os.environ[RAY_TEMP_PATH_ENV] = str(shared_ray_conn._temp_dir)

    config = RuntimeConfiguration(
        config_name="local",
        runtime_name=RuntimeName.RAY_LOCAL,
        runtime_options={
            "address": "auto",
            "log_to_driver": False,
            "storage": None,
            "temp_dir": None,
            "configure_logging": False,
        },
    )
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


def _wait_to_finish_wf(run_id: str, runtime: RuntimeInterface, timeout=60.0):
    def _continue_condition():
        state = runtime.get_workflow_run_status(run_id).status.state
        return state in [
            State.WAITING,
            State.RUNNING,
        ]

    _poll_loop(_continue_condition, interval=0.1, timeout=timeout)


def _count_task_runs(wf_run, state: State) -> int:
    return len([run for run in wf_run.task_runs if run.status.state == state])


# For now, we are using hardcoded pickles which wont work on different python versions.
@pytest.mark.filterwarnings(
    "ignore::orquestra.workflow_shared.exceptions.VersionMismatch"
)
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
            path_to_json = Path(__file__).parent.joinpath("data/multioutput.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            run_id1 = runtime.create_workflow_run(wf_def, None, False)
            run_id2 = runtime.create_workflow_run(wf_def, None, False)

            assert run_id1 != run_id2

            _wait_to_finish_wf(run_id1, runtime)
            _wait_to_finish_wf(run_id2, runtime)
            output1 = runtime.get_workflow_run_outputs_non_blocking(run_id1)
            output2 = runtime.get_workflow_run_outputs_non_blocking(run_id2)

            assert output1 == output2

        def test_sets_context(self, runtime: _dag.RayRuntime):
            path_to_json = Path(__file__).parent.joinpath("data/wf_with_exec_ctx.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            # when
            run_id = runtime.create_workflow_run(wf_def, None, False)

            # then
            _wait_to_finish_wf(run_id, runtime)
            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)
            assert outputs == (JSONResult(value='"RAY"'),)

        def test_sets_run_id_from_env(
            self, monkeypatch: pytest.MonkeyPatch, runtime: _dag.RayRuntime
        ):
            path_to_json = Path(__file__).parent.joinpath("data/multioutput.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            # Appending the normal run ID to prevent collisions
            global_wf_run_id = "run_id_from_env" + _dag._generate_wf_run_id(wf_def)
            monkeypatch.setenv("GLOBAL_WF_RUN_ID", global_wf_run_id)

            run_id = runtime.create_workflow_run(wf_def, None, False)

            runtime.stop_workflow_run(run_id)
            assert run_id == global_wf_run_id

        def test_simple_workflow_dry_run(
            self, monkeypatch: pytest.MonkeyPatch, runtime: _dag.RayRuntime
        ):
            path_to_json = Path(__file__).parent.joinpath(
                "data/exception_wf_with_multiple_values.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            run_id = runtime.create_workflow_run(wf_def, project=None, dry_run=True)
            _wait_to_finish_wf(run_id, runtime)

            # normally this WF would fail, but as a dry-run, no task code is executed
            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)

            assert outputs == (JSONResult(value='"dry_run task output"'),)

        def test_unpacking_workflow_dry_run(
            self, monkeypatch: pytest.MonkeyPatch, runtime: _dag.RayRuntime
        ):
            path_to_json = Path(__file__).parent.joinpath(
                "data/multioutput_task_failed_wf.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
            path_to_json = Path(__file__).parent.joinpath("data/greet_wf.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
            path_to_json = Path(__file__).parent.joinpath("data/greet_wf.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
            path_to_json = Path(__file__).parent.joinpath(
                "data/serial_wf_with_slow_middle_task.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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

        def test_handles_ray_environment_setup_error(
            self, runtime: _dag.RayRuntime, shared_ray_conn
        ):
            # Given
            path_to_json = Path(__file__).parent.joinpath(
                "data/cause_env_setup_error.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
            path_to_json = Path(__file__).parent.joinpath(
                "data/exception_wf_with_multiple_values.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
            path_to_json = Path(__file__).parent.joinpath("data/greet_wf.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
            run_id1 = runtime.create_workflow_run(wf_def, None, False)
            run_id2 = runtime.create_workflow_run(wf_def, None, False)

            # When
            wf_runs = runtime.list_workflow_runs()

            # Then
            wf_run_ids = {run.id for run in wf_runs}
            assert run_id1 in wf_run_ids
            assert run_id2 in wf_run_ids
            assert all(isinstance(run, WorkflowRun) for run in wf_runs)

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
            path_to_json = Path(__file__).parent.joinpath("data/infinite_workflow.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            wf_run_id = runtime.create_workflow_run(wf_def, None, False)
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
            path_to_json = Path(__file__).parent.joinpath(
                "data/multioutput_task_wf.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            wf_run_id = runtime.create_workflow_run(wf_def, None, False)
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
            path_to_json = Path(__file__).parent.joinpath("data/greet_wf.json")
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)

            assert outputs == (
                JSONResult(value='"yooooo emiliano from zapata computing"'),
            )

        def test_failed_workflow(self, runtime: _dag.RayRuntime):
            path_to_json = Path(__file__).parent.joinpath(
                "data/exception_wf_with_multiple_values.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
            run_id = runtime.create_workflow_run(wf_def, None, False)

            _wait_to_finish_wf(run_id, runtime)

            with pytest.raises(exceptions.WorkflowRunNotSucceeded):
                runtime.get_workflow_run_outputs_non_blocking(run_id)

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
            path_to_json = Path(__file__).parent.joinpath(
                "data/exception_wf_with_multiple_values.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
            path_to_json = Path(__file__).parent.joinpath(
                "data/exception_wf_with_multiple_values.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
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
        def test_multioutput_task(self, runtime: _dag.RayRuntime, tmp_path):
            path_to_json = Path(__file__).parent.joinpath(
                "data/multioutput_task_wf.json"
            )
            with open(path_to_json, "r", encoding="utf-8") as f:
                wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))

            wf_run_id = runtime.create_workflow_run(wf_def, None, False)

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
