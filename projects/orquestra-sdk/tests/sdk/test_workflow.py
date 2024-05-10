################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
import typing as t
from pathlib import Path

import pytest

import orquestra.sdk as sdk
from orquestra.sdk._client._base import _traversal, _workflow, loader
from orquestra.sdk._client._base._dsl import InvalidPlaceholderInCustomTaskNameError
from orquestra.sdk._shared.exceptions import WorkflowSyntaxError
from orquestra.sdk._shared.schema import ir

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


def another_one():
    pass


@sdk.workflow
def undecorated_task_wf():
    return [undecorated(), another_one()]


task = loader.FakeImportedAttribute()


@sdk.workflow
def faked_task_wf():
    return [task()]


def test_workflow_undecorated_task():
    with pytest.warns(_workflow.NotATaskWarning) as warnings:
        _ = undecorated_task_wf()

    assert len(warnings) == 1
    warning = str(warnings[0])
    assert "undecorated" in warning
    assert "another_one" in warning


def test_workflow_with_fake_imported_task():
    with pytest.raises(RuntimeError):
        _ = faked_task_wf()


@pytest.mark.parametrize(
    "data_agg",
    (
        True,
        False,
        sdk.DataAggregation(),
        sdk.DataAggregation(resources=sdk.Resources(cpu="1")),
    ),
)
def test_workflow_with_data_aggregation(data_agg):
    with pytest.warns(Warning) as warns:

        @sdk.workflow(
            data_aggregation=data_agg,
        )
        def _():
            return [_an_empty_task()]

    assert len(warns.list) == 1


class TestHeadNodeResources:
    @pytest.mark.parametrize(
        "head_node_resources, expected_data_agg",
        (
            (sdk.Resources(), sdk.DataAggregation(resources=sdk.Resources())),
            (None, None),
            (
                sdk.Resources(cpu="1"),
                sdk.DataAggregation(resources=sdk.Resources(cpu="1")),
            ),
        ),
    )
    def test_workflow_with_head_node_resources(
        self, head_node_resources, expected_data_agg
    ):
        @sdk.workflow(
            head_node_resources=head_node_resources,
        )
        def wf():
            return [_an_empty_task()]

        assert wf._data_aggregation == expected_data_agg

    FULL_RESOURCES = {"cpu": "2", "memory": "32Gi", "disk": "1Ti"}

    @pytest.mark.parametrize(
        "decorator_resources,override_resouces,expected_resources",
        [
            # Override
            (
                FULL_RESOURCES,
                {"cpu": "10"},
                ir.Resources(cpu="10", memory="32Gi", disk="1Ti"),
            ),
            (
                FULL_RESOURCES,
                {"memory": "100Gi"},
                ir.Resources(cpu="2", memory="100Gi", disk="1Ti"),
            ),
            (
                FULL_RESOURCES,
                {"disk": "1Gi"},
                ir.Resources(cpu="2", memory="32Gi", disk="1Gi"),
            ),
            # No kwargs means no-op
            ({}, {}, None),
            (
                FULL_RESOURCES,
                {},
                ir.Resources(cpu="2", memory="32Gi", disk="1Ti"),
            ),
            # Explicitly remove resources
            (FULL_RESOURCES, {"cpu": None, "memory": None, "disk": None}, None),
        ],
    )
    def test_with_head_resources_overrides(
        self,
        decorator_resources,
        override_resouces,
        expected_resources,
    ):
        @sdk.workflow(head_node_resources=sdk.Resources(**decorator_resources))
        def wf():
            return _an_empty_task()

        modified_model = wf().with_head_node_resources(**override_resouces).model

        assert modified_model.data_aggregation is not None
        assert modified_model.data_aggregation.resources == expected_resources


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

    def test_call_and_model_are_same_for_non_parametrized_workflows(self, monkeypatch):
        monkeypatch.setattr(_traversal, "_global_inline_import_identifier", lambda: 0)
        called = _simple_workflow()
        model = _simple_workflow.model
        assert called.model == model

    def test_fail_if_task_def_returned(self):
        @sdk.workflow
        def _returned_task_def_workflow():
            return sdk.task()(undecorated)

        with pytest.raises(WorkflowSyntaxError):
            with pytest.warns(_workflow.NotATaskWarning):
                _returned_task_def_workflow.model

    @pytest.mark.filterwarnings('ignore:"Functions task" are not a task')
    def test_fail_if_task_def_passed_as_arg(self):
        @sdk.workflow
        def _returned_task_def_workflow():
            task = sdk.task()(undecorated)
            return _task_with_args(task)

        with pytest.raises(WorkflowSyntaxError):
            with pytest.warns(_workflow.NotATaskWarning):
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
        wf: t.Any = _simple_workflow
        assert wf.__name__ == wf._fn.__name__

    @staticmethod
    def test_parametrized_wf_with_no_args():
        with pytest.raises(WorkflowSyntaxError):
            _parametrized_workflow()  # type: ignore


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
        graph.save(filename=dot_path)

        # Then
        assert dot_path.exists()


FULL_RESOURCES = {
    "cpu": "2",
    "memory": "32Gi",
    "disk": "1Ti",
    "gpu": "1",
    "nodes": 10,
}

NONE_RESOURCES = {
    "cpu": None,
    "memory": None,
    "gpu": None,
    "disk": None,  # noqa: F841
    "nodes": None,
}


class TestResources:
    @pytest.fixture
    def resourced_workflow(self):
        def _fixture(**kwargs):
            @sdk.workflow(resources=sdk.Resources(**kwargs))
            def wf():
                return _an_empty_task()

            return wf()

        return _fixture

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            # Single values
            ({"cpu": "1000m"}, ir.Resources(cpu="1000m")),
            ({"memory": "1000G"}, ir.Resources(memory="1000G")),
            ({"disk": "512Mi"}, ir.Resources(disk="512Mi")),
            ({"gpu": "1"}, ir.Resources(gpu="1")),
            ({"nodes": 10}, ir.Resources(nodes=10)),
            # Combination
            (
                FULL_RESOURCES,
                ir.Resources(cpu="2", memory="32Gi", disk="1Ti", gpu="1", nodes=10),
            ),
            # Empty resources
            ({}, None),
            (NONE_RESOURCES, None),
        ],
    )
    def test_resources_in_decorator(self, resourced_workflow, kwargs, expected):
        wf = resourced_workflow(**kwargs).model
        assert wf.resources == expected

    @pytest.mark.parametrize(
        "decorator_resources,override_resouces,expected_resources",
        [
            # Override
            (
                FULL_RESOURCES,
                {"cpu": "10"},
                ir.Resources(cpu="10", memory="32Gi", disk="1Ti", gpu="1", nodes=10),
            ),
            (
                FULL_RESOURCES,
                {"memory": "100Gi"},
                ir.Resources(cpu="2", memory="100Gi", disk="1Ti", gpu="1", nodes=10),
            ),
            (
                FULL_RESOURCES,
                {"disk": "1Gi"},
                ir.Resources(cpu="2", memory="32Gi", disk="1Gi", gpu="1", nodes=10),
            ),
            (
                FULL_RESOURCES,
                {"gpu": "0"},
                ir.Resources(cpu="2", memory="32Gi", disk="1Ti", gpu="0", nodes=10),
            ),
            (
                FULL_RESOURCES,
                {"nodes": 5},
                ir.Resources(cpu="2", memory="32Gi", disk="1Ti", gpu="1", nodes=5),
            ),
            # No kwargs means no-op
            ({}, {}, None),
            (
                FULL_RESOURCES,
                {},
                ir.Resources(cpu="2", memory="32Gi", disk="1Ti", gpu="1", nodes=10),
            ),
            # Explicitly remove resources
            (FULL_RESOURCES, NONE_RESOURCES, None),
        ],
    )
    def test_with_resources_overrides(
        self,
        resourced_workflow,
        decorator_resources,
        override_resouces,
        expected_resources,
    ):
        wf = (
            resourced_workflow(**decorator_resources)
            .with_resources(**override_resouces)
            .model
        )

        assert wf.resources == expected_resources


@pytest.mark.parametrize(
    "dependency_imports, expected_imports",
    [
        (None, None),
        (sdk.InlineImport(), (sdk.InlineImport(),)),
        (sdk.LocalImport("mod"), (sdk.LocalImport("mod"),)),
        (
            sdk.GitImport(repo_url="abc", git_ref="xyz"),
            (sdk.GitImport(repo_url="abc", git_ref="xyz"),),
        ),
        (
            sdk.GithubImport("abc"),
            (sdk.GithubImport("abc"),),
        ),
        (
            sdk.PythonImports("abc"),
            (sdk.PythonImports("abc"),),
        ),
    ],
)
def test_default_dependency_imports(dependency_imports, expected_imports):
    @sdk.workflow(default_dependency_imports=dependency_imports)
    def my_workflow():
        pass

    assert my_workflow._default_dependency_imports == expected_imports


def test_with_resources_copies_imports():
    @sdk.workflow(
        default_dependency_imports=[sdk.PythonImports("abc")],
        default_source_import=sdk.GitImport(repo_url="abc", git_ref="xyz"),
    )
    def my_workflow():
        pass

    initial_workflow = my_workflow()
    modified_workflow = initial_workflow.with_resources(cpu="xyz")

    assert (
        initial_workflow.default_dependency_imports
        == modified_workflow.default_dependency_imports
    )
    assert (
        initial_workflow.default_source_import
        == modified_workflow.default_source_import
    )
