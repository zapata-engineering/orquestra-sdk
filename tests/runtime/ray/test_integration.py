################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Integration tests for our code that uses Ray. This file should be kept as small
as possible, because it's slow to run. Please consider using unit tests and
RuntimeInterface mocks instead of extending this file.
"""
import time
import typing as t
from pathlib import Path

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base._testing import _example_wfs
from orquestra.sdk._base._testing._connections import make_ray_conn
from orquestra.sdk._ray import _client, _dag, _ray_logs
from orquestra.sdk.schema import configs
from orquestra.sdk.schema.workflow_run import State


@pytest.fixture(scope="module")
def shared_ray_conn():
    with make_ray_conn() as ray_params:
        yield ray_params


@pytest.fixture(scope="module")
def runtime(shared_ray_conn, tmp_path_factory: pytest.TempPathFactory):
    project_dir = tmp_path_factory.mktemp("ray-integration")
    config = configs.RuntimeConfiguration(
        config_name="test-config",
        runtime_name=configs.RuntimeName.RAY_LOCAL,
    )
    client = _client.RayClient()
    rt = _dag.RayRuntime(client, config, project_dir)
    yield rt


def _poll_loop(
    continue_condition: t.Callable[[], bool], interval: float, timeout: float
):
    start_time = time.time()
    while continue_condition():
        if time.time() - start_time > timeout:
            raise TimeoutError("Polling timed out")

        time.sleep(interval)


def _wait_to_finish_wf(run_id: str, runtime: _dag.RayRuntime):
    def _continue_condition():
        state = runtime.get_workflow_run_status(run_id).status.state
        return state in [
            State.WAITING,
            State.RUNNING,
        ]

    _poll_loop(_continue_condition, interval=0.1, timeout=10.0)


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
            run_id1 = runtime.create_workflow_run(wf_def)
            run_id2 = runtime.create_workflow_run(wf_def)

            assert run_id1 != run_id2

            output1 = runtime.get_workflow_run_outputs(run_id1)
            output2 = runtime.get_workflow_run_outputs(run_id2)

            assert output1 == output2

        def test_sets_context(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.wf_with_exec_ctx().model

            # when
            run_id = runtime.create_workflow_run(wf_def)

            # then
            _wait_to_finish_wf(run_id, runtime)
            outputs = runtime.get_workflow_run_outputs(run_id)
            assert outputs == ("LOCAL_RAY",)

    class TestGetWorkflowRunStatus:
        """
        Tests that validate .get_workflow_run_status().
        """

        @pytest.mark.parametrize("trial", range(5))
        def test_status_right_after_start(self, runtime: _dag.RayRuntime, trial):
            """
            Verifies that we report status correctly before workflow ends.
            It's difficult to synchronize, so there's a bunch of ifs in this
            test.
            """
            # Given
            wf_def = _example_wfs.greet_wf.model
            run_id = runtime.create_workflow_run(wf_def)

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

        @pytest.mark.parametrize("trial", range(5))
        def test_status_after_awaiting(self, runtime: _dag.RayRuntime, trial):
            """
            Verifies that we report status correctly when all tasks are
            completed.

            Note: Ray sometimes suffers from race conditions between blocking
            on the output, and returning workflow status "completed". This is
            why this test is repeated a couple of times.
            """
            # Given
            wf_def = _example_wfs.greet_wf.model
            run_id = runtime.create_workflow_run(wf_def)

            # Block until wf completes
            _ = runtime.get_workflow_run_outputs(run_id)

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
            run_id = runtime.create_workflow_run(wf_def)

            _wait_to_finish_wf(run_id, runtime)

            wf_run = runtime.get_workflow_run_status(run_id)

            assert wf_run.status.state == State.FAILED
            assert _count_task_runs(wf_run, State.FAILED) == 1
            assert _count_task_runs(wf_run, State.SUCCEEDED) == 1
            assert _count_task_runs(wf_run, State.WAITING) == 1

    class TestGetAllWorkflowRunStatus:
        """
        Tests that validate .get_all_workflow_run_status().
        """

        def test_two_runs(self, runtime: _dag.RayRuntime):
            # GIVEN
            wf_def = _example_wfs.greet_wf.model
            run_id1 = runtime.create_workflow_run(wf_def)
            run_id2 = runtime.create_workflow_run(wf_def)

            # WHEN
            wf_runs = runtime.get_all_workflow_runs_status()

            # THEN
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
              [ ]  => waiting
               │
               │
               ▼
            """
            triggers = [tmp_path / f"trigger{i}.txt" for i in range(2)]

            wf = _example_wfs.serial_wf_with_file_triggers(
                triggers, task_timeout=2.0
            ).model
            wf_run_id = runtime.create_workflow_run(wf)
            wf_run = runtime.get_workflow_run_status(wf_run_id)
            assert wf_run.status.state == State.RUNNING

            runtime.stop_workflow_run(wf_run_id)

            wf_run = runtime.get_workflow_run_status(wf_run_id)
            assert wf_run.status.state == State.TERMINATED

        def test_on_finished_workflow(self, runtime: _dag.RayRuntime, tmp_path):

            wf = _example_wfs.multioutput_task_wf.model
            wf_run_id = runtime.create_workflow_run(wf)
            _ = runtime.get_workflow_run_outputs(wf_run_id)  # wait for it to finish
            # ensure wf has finished
            status = runtime.get_workflow_run_status(wf_run_id)
            assert status.status.state == State.SUCCEEDED

            runtime.stop_workflow_run(wf_run_id)
            status = runtime.get_workflow_run_status(wf_run_id)
            # cancel changes the state to terminated
            assert status.status.state == State.TERMINATED

    class TestGetWorkflowRunOutputsNonBlocking:
        """
        Tests that validate get_workflow_run_outputs_non_blocking
        """

        def test_happy_path(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.greet_wf.model
            run_id = runtime.create_workflow_run(wf_def)

            _wait_to_finish_wf(run_id, runtime)

            outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)

            assert outputs == ("yooooo emiliano from zapata computing",)

        def test_failed_workflow(self, runtime: _dag.RayRuntime):
            wf_def = _example_wfs.exception_wf_with_multiple_values().model
            run_id = runtime.create_workflow_run(wf_def)

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
            triggers = [tmp_path / f"trigger{i}.txt" for i in range(2)]

            wf = _example_wfs.serial_wf_with_file_triggers(
                triggers, task_timeout=2.0
            ).model
            run_id = runtime.create_workflow_run(wf)

            assert runtime.get_workflow_run_status(run_id).status.state == State.RUNNING
            with pytest.raises(exceptions.WorkflowRunNotSucceeded):
                runtime.get_workflow_run_outputs_non_blocking(run_id)

            # let the workers complete the workflow
            triggers[0].write_text("triggered")
            triggers[1].write_text("triggered")

    class TestGetAvailableOutputs:
        """
        Tests that validate .get_available_outputs().
        """

        def test_after_a_failed_task(self, runtime: _dag.RayRuntime, tmp_path):
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
            run_id = runtime.create_workflow_run(wf_def)

            _wait_to_finish_wf(run_id, runtime)

            wf_run = runtime.get_workflow_run_status(run_id)
            outputs = runtime.get_available_outputs(run_id)
            if wf_run.status.state != State.FAILED:
                pytest.fail(
                    "The workflow was supposed to fail, but it didn't. "
                    f"Wf run: {wf_run}"
                )

            # expect only one finished task
            assert len(outputs) == 1
            assert 58 in set(outputs.values())

        def test_before_any_task_finishes(self, runtime: _dag.RayRuntime, tmp_path):
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
            triggers = [tmp_path / f"trigger{i}.txt" for i in range(2)]

            wf = _example_wfs.serial_wf_with_file_triggers(
                triggers, task_timeout=2.0
            ).model
            wf_run_id = runtime.create_workflow_run(wf)

            outputs = runtime.get_available_outputs(wf_run_id)
            assert outputs == {}

            # let the workers complete the workflow
            triggers[0].write_text("triggered")
            triggers[1].write_text("triggered")

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
            # Given
            triggers = [tmp_path / f"trigger{i}.txt" for i in range(3)]

            # Let the first task finish quickly
            triggers[0].write_text("triggered")

            wf = _example_wfs.serial_wf_with_file_triggers(
                triggers, task_timeout=2.0
            ).model
            wf_run_id = runtime.create_workflow_run(wf)

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

                if time.time() - loop_start > 10.0:
                    pytest.fail(
                        f"Timeout when awaiting for workflow finish. Full run: {wf_run}"
                    )

                time.sleep(0.2)

            # When
            outputs = runtime.get_available_outputs(wf_run_id)

            # Then
            assert len(outputs) == len(succeeded_runs)

            # let the workers complete the workflow
            triggers[1].write_text("triggered")
            triggers[2].write_text("triggered")


@pytest.mark.slow
@pytest.mark.parametrize(
    "wf,expected_outputs",
    [
        (_example_wfs.greet_wf, ("yooooo emiliano from zapata computing",)),
        (
            _example_wfs.complicated_wf,
            ("yooooo emiliano Zapata from Zapata computing",),
        ),
        (_example_wfs.multioutput_wf, ("Emiliano Zapata", "Zapata computing")),
        (
            _example_wfs.multioutput_task_wf,
            ("Zapata", "Computing", "Computing", ("Zapata", "Computing")),
        ),
        (_example_wfs.greet_wf_kw, ("yooooo emiliano from zapata computing",)),
        (
            _example_wfs.wf_using_inline_imports,
            ("Emiliano Zapata", "Zapata computing"),
        ),
    ],
)
def test_run_and_get_output(runtime: _dag.RayRuntime, wf, expected_outputs):
    run_id = runtime.create_workflow_run(wf.model)
    wf_result = runtime.get_workflow_run_outputs(run_id)

    assert wf_result == expected_outputs


# This test is slow to run locally because:
# - It installs packages inside a separated venv.
# - Installing packages takes some time in general.
# - Installing packages on company machines is very slow because of the antivirus
#   scanning.
@pytest.mark.slow
def test_import_package_inside_ray(runtime: _dag.RayRuntime):
    # This package should not be installed before running test
    with pytest.raises(ModuleNotFoundError):
        import piccup  # type: ignore # noqa

    run_id = runtime.create_workflow_run(_example_wfs.wf_using_python_imports.model)
    wf_result = runtime.get_workflow_run_outputs(run_id)
    assert wf_result == (2,)

    # this package should be only used inside ray env
    with pytest.raises(ModuleNotFoundError):
        import piccup  # type: ignore # noqa


@pytest.mark.slow
class TestRayRuntimeErrors:
    @pytest.mark.parametrize(
        "method",
        [
            _dag.RayRuntime.get_available_outputs,
            _dag.RayRuntime.get_workflow_run_outputs,
            _dag.RayRuntime.get_workflow_run_outputs_non_blocking,
            _dag.RayRuntime.get_workflow_run_status,
            _dag.RayRuntime.stop_workflow_run,
        ],
    )
    def test_invalid_run_id(self, runtime: _dag.RayRuntime, method):
        run_id = "I hope this doesn't exist"

        with pytest.raises(exceptions.NotFoundError):
            _ = method(runtime, run_id)


@pytest.mark.slow
@pytest.mark.parametrize("with_run_id", [True, False])
@pytest.mark.parametrize(
    "wf,tell_tale",
    [
        (_example_wfs.wf_with_log(msg="hello, there!").model, "hello, there!"),
        (_example_wfs.exception_wf.model, "ZeroDivisionError: division by zero"),
    ],
)
class TestDirectRayReader:
    """
    Verifies that our code can read log files produced by Ray.
    This class tests reading Ray log files directly; it doesn't test the solution based
    on FluentBit.

    The tests' boundary: `[DirectRayReader]-[task code]`
    """

    def test_read_full_logs(
        self, shared_ray_conn, runtime, with_run_id: bool, wf, tell_tale: str
    ):
        """
        Submit a workflow, wait for it to finish, get full logs, look for the test
        message.
        """
        # Given
        ray_params = shared_ray_conn
        reader = _ray_logs.DirectRayReader(Path(ray_params._temp_dir))

        run_id = runtime.create_workflow_run(wf)
        _wait_to_finish_wf(run_id, runtime)

        query_run_id = run_id if with_run_id else None

        # When
        logs_dict = reader.get_full_logs(run_id=query_run_id)

        # Then
        log_lines_joined = "".join(
            log_line
            for task_log_lines in logs_dict.values()
            for log_line in task_log_lines
        )

        assert tell_tale in log_lines_joined

    def test_iter_logs(
        self, shared_ray_conn, runtime, with_run_id: bool, wf, tell_tale: str
    ):
        """
        Submit a workflow, wait for it to finish, get one batch of logs, look for
        the test message.
        """
        # Given
        ray_params = shared_ray_conn
        reader = _ray_logs.DirectRayReader(Path(ray_params._temp_dir))

        run_id = runtime.create_workflow_run(wf)
        _wait_to_finish_wf(run_id, runtime)

        query_run_id = run_id if with_run_id else None

        # When
        iterator = reader.iter_logs(run_id=query_run_id)
        logs_batch = next(iterator)

        # Then
        log_lines_joined = "".join(log_line for log_line in logs_batch)

        assert tell_tale in log_lines_joined


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

    # ignoring mypy, this is supposed to be module fn repo
    wf_def.tasks[task_id].fn_ref.module = "nope"  # type: ignore

    # when
    wf_id = runtime.create_workflow_run(wf_def)
    _wait_to_finish_wf(wf_id, runtime)

    # then
    assert runtime.get_workflow_run_status(wf_id).status.state == State.FAILED
    assert (
        runtime.get_workflow_run_status(wf_id).task_runs[0].status.state == State.FAILED
    )
