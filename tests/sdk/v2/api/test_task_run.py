################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._api._task_run.
"""

import typing as t
from unittest.mock import MagicMock, Mock, create_autospec

import pytest

from orquestra.sdk._base import _api, _workflow
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.exceptions import TaskRunNotFound
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import RunStatus, State
from orquestra.sdk.schema.workflow_run import TaskRun as TaskRunModel
from orquestra.sdk.schema.workflow_run import WorkflowRun as WorkflowRunModel

from ..data.complex_serialization.workflow_defs import capitalize, join_strings


class TestTaskRun:
    @staticmethod
    @pytest.fixture
    def sample_wf_def() -> _workflow.WorkflowDef:
        @_workflow.workflow
        def my_wf():
            # We need at least 4 invocations to use in our tests.
            text1 = capitalize(join_strings(["hello", "there"]))
            text2 = capitalize(join_strings(["general", "kenobi"]))

            return text1, text2

        return my_wf()

    @staticmethod
    @pytest.fixture
    def mock_runtime(sample_wf_def):
        runtime = MagicMock(RuntimeInterface)

        wf_def_model = sample_wf_def.model
        task_invs = list(wf_def_model.task_invocations.values())

        wf_run_model = create_autospec(WorkflowRunModel)
        wf_run_model.task_runs = [
            TaskRunModel(
                id="task_run1",
                invocation_id=task_invs[0].id,
                status=RunStatus(state=State.SUCCEEDED),
            ),
            TaskRunModel(
                id="task_run2",
                invocation_id=task_invs[1].id,
                status=RunStatus(state=State.FAILED),
            ),
            TaskRunModel(
                id="task_run3",
                invocation_id=task_invs[2].id,
                status=RunStatus(state=State.FAILED),
            ),
        ]
        runtime.get_workflow_run_status.return_value = wf_run_model

        return runtime

    @staticmethod
    @pytest.fixture
    def task_runs(sample_wf_def, mock_runtime) -> t.Sequence[_api.TaskRun]:
        wf_run_id = "wf.1"
        wf_run_model = mock_runtime.get_workflow_run_status(wf_run_id)
        task_run_models = wf_run_model.task_runs

        task_runs = [
            _api.TaskRun(
                task_run_id=run_model.id,
                task_invocation_id=run_model.invocation_id,
                workflow_run_id=wf_run_id,
                runtime=mock_runtime,
                wf_def=sample_wf_def.model,
            )
            for run_model in task_run_models
        ]

        return task_runs

    class TestInit:
        @staticmethod
        def test_init_simple_wf():
            # Given
            from ..data.task_run_workflow_defs import (
                return_num,
                simple_wf_one_task_two_invocations,
            )

            wf_def = simple_wf_one_task_two_invocations().model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            task_run_id = "task-run-1"
            wf_run_id = "wf.1"

            # When
            task_run = _api.TaskRun(
                task_run_id=task_run_id,
                task_invocation_id=inv_id,
                workflow_run_id=wf_run_id,
                runtime=Mock(),
                wf_def=wf_def,
            )

            # Then
            assert task_run.task_invocation_id == inv_id
            assert task_run.module is not None
            assert "task_run_workflow_defs" in task_run.module
            assert task_run.fn_name == return_num.__name__
            assert task_run.workflow_run_id == wf_run_id

        @staticmethod
        def test_init_simple_wf_inline_fn():
            # given
            from ..data.task_run_workflow_defs import (
                return_five_inline,
                simple_wf_one_task_inline,
            )

            wf_def = simple_wf_one_task_inline().model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            task_run_id = "task-run-1"
            wf_run_id = "wf.1"

            # when
            task_run = _api.TaskRun(
                task_run_id=task_run_id,
                task_invocation_id=inv_id,
                workflow_run_id=wf_run_id,
                runtime=Mock(),
                wf_def=wf_def,
            )

            # then
            assert task_run.task_invocation_id == inv_id
            assert task_run.module is None
            assert task_run.fn_name == return_five_inline.__name__
            assert task_run.workflow_run_id == wf_run_id

    class TestGetStatus:
        @staticmethod
        def test_get_status(task_runs):
            # When
            statuses = [task_run.get_status() for task_run in task_runs]

            # Then
            assert statuses == [State.SUCCEEDED, State.FAILED, State.FAILED]

    class TestGetLogs:
        @staticmethod
        def test_returns_plain_list(task_runs: t.Sequence[_api.TaskRun], mock_runtime):
            """
            Methods in RuntimeInterface return logs nested in dictionaries.
            _api.TaskRun.get_logs() should return a list of log lines.
            """
            # Given
            mock_runtime.get_task_logs.side_effect = [
                ["woohoo!"],
                ["another", "line"],
                # This task invocation was executed, but it produced no logs.
                [],
            ]

            # When
            log_lists = [task_run.get_logs() for task_run in task_runs]

            # Then
            assert log_lists[0] == ["woohoo!"]
            assert log_lists[1] == ["another", "line"]
            assert log_lists[2] == []

    class TestGetOutputs:
        @staticmethod
        @pytest.fixture
        def wf_def_model() -> ir.WorkflowDef:
            wf_def = create_autospec(ir.WorkflowDef)
            wf_def.task_invocations = {
                "inv1": ir.TaskInvocation(
                    id="inv1",
                    task_id="task_def1",
                    args_ids=[],
                    kwargs_ids={},
                    output_ids=["art1"],
                    resources=None,
                    custom_image=None,
                ),
                "inv2": ir.TaskInvocation(
                    id="inv2",
                    task_id="task_def1",
                    args_ids=[],
                    kwargs_ids={},
                    output_ids=["art2", "art3"],
                    resources=None,
                    custom_image=None,
                ),
            }
            task_def = create_autospec(ir.TaskDef)
            task_def.fn_ref = Mock()
            wf_def.tasks = {"task_def1": task_def}

            return wf_def

        @staticmethod
        @pytest.mark.parametrize(
            "inv_id,exp_output",
            [
                pytest.param("inv1", 42, id="single_output_invocation"),
                pytest.param("inv2", (21, 38), id="multi_param_invocation"),
            ],
        )
        def test_get_output_finished(wf_def_model, inv_id: str, exp_output):
            # Given
            runtime = create_autospec(RuntimeInterface)
            runtime.get_available_outputs.return_value = {
                "inv1": (42,),
                "inv2": (21, 38),
            }

            task_run = _api.TaskRun(
                task_run_id="a_run",
                task_invocation_id=inv_id,
                workflow_run_id="wf.1",
                runtime=runtime,
                wf_def=wf_def_model,
            )

            # When
            output = task_run.get_outputs()

            # Then
            assert output == exp_output

        @staticmethod
        def test_get_outputs_not_all_finished(wf_def_model):
            runtime = create_autospec(RuntimeInterface)
            # No outputs available
            runtime.get_available_outputs.return_value = {}

            task_run = _api.TaskRun(
                task_run_id="a_run",
                task_invocation_id="inv1",
                workflow_run_id="wf.1",
                runtime=runtime,
                wf_def=wf_def_model,
            )

            # Then
            with pytest.raises(TaskRunNotFound):
                # When
                _ = task_run.get_outputs()

    class TestGetParents:
        @staticmethod
        @pytest.mark.parametrize(
            "inv_id,expected_parent_ids",
            [
                # The IDs here are coupled with the workflow definition, but that's the
                # easiest way to set this up.
                pytest.param(
                    "invocation-2-task-return-num",
                    set(),
                    id="top_task",
                ),
                pytest.param(
                    "invocation-1-task-return-num",
                    {"invocation-2-task-return-num"},
                    id="middle_task",
                ),
                pytest.param(
                    "invocation-0-task-return-num",
                    {"invocation-3-task-return-num", "invocation-1-task-return-num"},
                    id="trailing_task",
                ),
            ],
        )
        def test_inv_ids_match(
            inv_id: ir.TaskInvocationId, expected_parent_ids: t.Set[ir.TaskInvocationId]
        ):
            """
            Workflow graph in the scenario under test:
               5
               │
              [x] <- 0 parents, 1 const input
             __│__
             │   │
             ▼   ▼
            [ ] [x]  7 <- each have 1 the same parent, the task with 0 parents
             │___│___│
                 │
                 ▼
                [x] <- 2 parents, each of the tasks with 1 parent
            """
            # Given
            from ..data.task_run_workflow_defs import wf_task_with_two_parents

            wf_def_model = wf_task_with_two_parents().model

            def _mock_task_run_model(i: int):
                model = create_autospec(TaskRunModel)
                model.id = f"run-{i}"

                # this has to match the definition
                model.invocation_id = f"invocation-{i}-task-return-num"

                return model

            wf_run_model: WorkflowRunModel = create_autospec(WorkflowRunModel)
            wf_run_model.task_runs = [_mock_task_run_model(i) for i in range(4)]

            runtime = create_autospec(RuntimeInterface)
            runtime.get_workflow_run_status.return_value = wf_run_model

            task_run = _api.TaskRun(
                task_run_id="run1",
                task_invocation_id=inv_id,
                workflow_run_id="wf.1",
                runtime=runtime,
                wf_def=wf_def_model,
            )

            # When
            parents = task_run.get_parents()

            # then
            assert {
                parent.task_invocation_id for parent in parents
            } == expected_parent_ids

        @staticmethod
        def test_other_ids_match():
            # Given
            from ..data.task_run_workflow_defs import wf_task_with_two_parents

            wf_def_model = wf_task_with_two_parents().model

            wf_run_id = "wf.1"

            task_run_model1: TaskRunModel = create_autospec(TaskRunModel)
            task_run_model1.id = "top-task-run"
            task_run_model1.invocation_id = "invocation-2-task-return-num"

            task_run_model2 = create_autospec(TaskRunModel)
            task_run_model2.id = "middle-task-run"
            task_run_model2.invocation_id = "invocation-1-task-return-num"

            wf_run_model: WorkflowRunModel = create_autospec(WorkflowRunModel)
            wf_run_model.task_runs = [task_run_model1, task_run_model2]

            runtime = create_autospec(RuntimeInterface)
            runtime.get_workflow_run_status.return_value = wf_run_model

            task_run = _api.TaskRun(
                task_run_id="middle-task-run",
                task_invocation_id="invocation-1-task-return-num",
                workflow_run_id=wf_run_id,
                runtime=runtime,
                wf_def=wf_def_model,
            )

            # When
            parents = task_run.get_parents()

            # Then
            assert len(parents) == 1
            parent = next(iter(parents))
            assert parent.task_run_id == "top-task-run"
            assert parent.workflow_run_id == wf_run_id

    class TestGetInput:
        @staticmethod
        @pytest.mark.parametrize(
            "workflow, expected_args, expected_kwargs",
            [
                ("simple_wf_one_task_inline", [], {}),
                ("wf_single_task_with_const_parent", [21], {}),
                ("wf_single_task_with_const_parent_kwargs", [], {"kwargs": 36}),
                ("wf_single_task_with_const_parent_args_kwargs", [21], {"kwargs": 36}),
            ],
        )
        def test_const_as_parent(workflow, expected_args, expected_kwargs):
            # given
            from ..data import task_run_workflow_defs

            wf_def = getattr(task_run_workflow_defs, workflow).model
            inv_id = next(iter(wf_def.task_invocations.keys()))
            wf_run_id = "wf.1"

            task_run = _api.TaskRun(
                task_run_id="run1",
                task_invocation_id=inv_id,
                workflow_run_id=wf_run_id,
                runtime=create_autospec(RuntimeInterface),
                wf_def=wf_def,
            )

            # when
            inputs = task_run.get_inputs()

            # then
            assert inputs.args == expected_args
            assert inputs.kwargs == expected_kwargs

        @staticmethod
        def _find_task_by_args_and_kwargs_number(
            arg_num, kwarg_num, wf_def
        ) -> ir.TaskInvocationId:
            return next(
                iter(
                    inv_id
                    for inv_id, inv in wf_def.task_invocations.items()
                    if len(inv.args_ids) == arg_num and len(inv.kwargs_ids) == kwarg_num
                )
            )

        def test_tasks_as_parents(self):
            """
            Workflow graph in the scenario under test:
                 5
                 │
             10,[ ] <- 0 parents, 1 const input
             __│__
             │   │
             │   │
             ▼   ▼
            [ ] [X]  5 <- each have 1 the same parent, the task with 0 parents
             │___│___│
                 │
                 ▼
                [X] <- 2 parents, each of the tasks with 1 parent and const value
            Tasks marked as X are mocked to be not finished yet
            """

            # given
            from ..data.task_run_workflow_defs import wf_for_input_test

            wf_def = wf_for_input_test().model
            # find the task with 1 arg and 0 kwarg args. It is the one at the top
            first_inv_id = self._find_task_by_args_and_kwargs_number(1, 0, wf_def)
            # find the task with 1 arg and 1 kwarg. 2nd task that finished
            second_inv_2 = self._find_task_by_args_and_kwargs_number(1, 1, wf_def)

            runtime = MagicMock(RuntimeInterface)
            runtime_outputs = {first_inv_id: (15,), second_inv_2: (25,)}
            runtime.get_available_outputs.return_value = runtime_outputs
            wf_run_id = "wf.3"

            task_runs = [
                _api.TaskRun(
                    task_run_id=f"run_{inv_id}",
                    task_invocation_id=inv_id,
                    workflow_run_id=wf_run_id,
                    runtime=runtime,
                    wf_def=wf_def,
                )
                for inv_id in wf_def.task_invocations.keys()
            ]

            # when
            inputs = [task_run.get_inputs() for task_run in task_runs]

            # then
            expected_inputs = [
                ({5}, {}),
                ({10}, {"kwarg": 15}),
                ({10}, {"kwarg": 15}),
                ({25, _api.TaskRun.INPUT_UNAVAILABLE, 7}, {}),
            ]
            assert len(inputs) == len(expected_inputs)
            for mapped in map(lambda input: (set(input.args), input.kwargs), inputs):
                assert mapped in expected_inputs

        def test_task_with_multiple_output_as_parent(self):
            # given
            from ..data.task_run_workflow_defs import wf_multi_output_task

            wf_def = wf_multi_output_task().model
            wf_run_id = "wf.4"

            # find the task with 0 arg and 0 kwarg args
            first_inv_id = self._find_task_by_args_and_kwargs_number(0, 0, wf_def)
            # find the task with 1 arg and 1 kwarg args. 2nd task with that finished
            second_inv_id = self._find_task_by_args_and_kwargs_number(1, 0, wf_def)

            runtime = MagicMock(RuntimeInterface)
            runtime_outputs = {first_inv_id: (21, 36), second_inv_id: (25,)}
            runtime.get_available_outputs.return_value = runtime_outputs

            task_run = _api.TaskRun(
                task_run_id=f"run_{second_inv_id}",
                task_invocation_id=second_inv_id,
                workflow_run_id=wf_run_id,
                runtime=runtime,
                wf_def=wf_def,
            )

            # when
            task_input = task_run.get_inputs()

            # then
            assert task_input.args == [21]
            assert task_input.kwargs == {}
