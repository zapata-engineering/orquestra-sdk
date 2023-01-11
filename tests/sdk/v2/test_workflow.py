################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from pathlib import Path

import pytest

import orquestra.sdk as sdk
from orquestra.sdk._base import _workflow, loader
from orquestra.sdk._base._dsl import InvalidPlaceholderInCustomTaskNameError
from orquestra.sdk.exceptions import WorkflowSyntaxError

DEFAULT_LOCAL_REPO_PATH = Path(__file__).parent.resolve()


@sdk.task
def _an_empty_task():
    pass


@sdk.task
def _task_with_args(a: int):
    return a + 1


@sdk.workflow
def _simple_workflow():
    foo = _an_empty_task()
    return [foo]


@sdk.workflow
def _parametrized_workflow(a: int):
    return [_task_with_args(a)]


@sdk.workflow
def _single_future_workflow():
    return _an_empty_task()


def undecorated():
    pass


@sdk.workflow
def undecorated_task_wf():
    return [undecorated()]


task = loader.FakeImportedAttribute()


@sdk.workflow
def faked_task_wf():
    return [task()]


def test_workflow_undecorated_task():
    with pytest.warns(_workflow.NotATaskWarning):
        _ = undecorated_task_wf()


def test_workflow_with_fake_imported_task():
    with pytest.raises(RuntimeError):
        _ = faked_task_wf()


class TestDataAggregationResources:
    @staticmethod
    @sdk.workflow(
        data_aggregation=sdk.DataAggregation(resources=sdk.Resources(gpu="1g"))
    )
    def _workflow_gpu_set_for_data_aggregation():
        return [_an_empty_task()]

    @staticmethod
    @sdk.workflow(data_aggregation=False)
    def _workflow_data_aggregation_false():
        return [_an_empty_task()]

    @staticmethod
    @sdk.workflow(data_aggregation=True)
    def _workflow_data_aggregation_true():
        return [_an_empty_task()]

    @staticmethod
    @sdk.workflow
    def _workflow_no_data_aggregation():
        return [_an_empty_task()]

    def test_workflow_with_gpu_set(self):
        with pytest.warns(Warning) as warns:
            wf = self._workflow_gpu_set_for_data_aggregation()
            assert len(warns.list) == 1
            assert wf.model.data_aggregation.resources.gpu == "0"

    def test_workflow_data_aggregation_false(self):
        wf = self._workflow_data_aggregation_false()
        assert wf.model.data_aggregation.run is False

    def test_workflow_data_aggregation_true(self):
        # verify that if user passes dataAggregation=True, it falls back to default
        # as if nothing was provided
        wf_data_aggregation_true = self._workflow_data_aggregation_true()
        wf_data_aggregation_default = self._workflow_no_data_aggregation()
        assert (
            wf_data_aggregation_true.model.data_aggregation
            == wf_data_aggregation_default.data_aggregation
        )


class TestModelsSerializeProperly:
    """The properties are Pydantic models. They would raise ValidationError if the
    serialization went wrong.
    """

    def test_workflow_def(self):
        _simple_workflow.model

    def test_workflow_single_future(self):
        _single_future_workflow.model

    def test_parametrized_workflow_model_fails(self):
        with pytest.raises(NotImplementedError):
            _parametrized_workflow.model

    def test_call_and_model_are_same_for_non_parametrized_workflows(self):
        called = _simple_workflow()
        model = _simple_workflow.model
        assert called.model == model

    @pytest.mark.filterwarnings('ignore:"task" is not a task')
    def test_fail_if_task_def_returned(self):
        @sdk.workflow
        def _returned_task_def_workflow():
            return sdk.task()(undecorated)

        with pytest.raises(WorkflowSyntaxError):
            _returned_task_def_workflow.model

    @pytest.mark.filterwarnings('ignore:"task" is not a task')
    def test_fail_if_task_def_passed_as_arg(self):
        @sdk.workflow
        def _returned_task_def_workflow():
            task = sdk.task()(undecorated)
            return _task_with_args(task)

        with pytest.raises(WorkflowSyntaxError):
            _returned_task_def_workflow.model


@pytest.mark.parametrize(
    "workflow, expected_is_parametrized",
    [
        (_simple_workflow, False),
        (_single_future_workflow, False),
        (_parametrized_workflow, True),
    ],
)
def test_workflow_template_is_parametrized(
    workflow: _workflow.WorkflowTemplate, expected_is_parametrized: bool
):
    assert workflow.is_parametrized == expected_is_parametrized


@pytest.mark.parametrize(
    "name, args, expected",
    [
        ("name", "", "name"),
        ("{args}", "abc", "abc"),
        ("name{args}name", "abc", "nameabcname"),
        ("float {args}", 1 / 3, "float 0.3333333333333333"),
        ("formatted {args:.2f}", 1 / 3, "formatted 0.33"),
    ],
)
def test_simple_custom_names_of_workflows(name, args, expected):
    @sdk.workflow(custom_name=name)
    def _local_workflow(args="default"):
        ...

    x = _local_workflow(args)
    assert x.name == expected


def test_simple_custom_names_of_workflows_default_value():
    @sdk.workflow(custom_name="{args}")
    def _local_workflow(args="default"):
        ...

    x = _local_workflow()
    assert x.name == "default"


@pytest.mark.parametrize(
    "task_name",
    [
        "{x}",
        "{args}{x}",
        "{args}normal_text{args}{argss}",
    ],
)
def test_error_case_custom_names_of_workflows(task_name):
    @sdk.workflow(custom_name=task_name)
    def _local_workflow(args):
        ...

    with pytest.raises(InvalidPlaceholderInCustomTaskNameError):
        _ = _local_workflow("")


def test_artifact_node_custom_names():
    @sdk.task
    def _local_task():
        ...

    @sdk.workflow(custom_name="My_custom_name_{x}")
    def _local_workflow(x):
        ...

    ret = _local_task()
    with pytest.raises(WorkflowSyntaxError):
        _ = _local_workflow(ret)


class TestWorkflowTemplate:
    @staticmethod
    def test_wraps_fn():
        assert _simple_workflow.__name__ == _simple_workflow._fn.__name__

    @staticmethod
    def test_parametrized_wf_with_no_args():
        with pytest.raises(WorkflowSyntaxError):
            _parametrized_workflow()


class TestGraph:
    """
    Tests for wf().graph
    """

    @staticmethod
    def test_is_exportable_to_dot(tmp_path):
        """
        Checks if the graph we build can be exported to a dot language.
        This should work even if graphviz isn't installed in the OS packages.
        """
        # Given
        dot_path = tmp_path / "graph.gv"

        # When
        graph = _simple_workflow().graph
        graph.save(dot_path)

        # Then
        assert dot_path.exists()
