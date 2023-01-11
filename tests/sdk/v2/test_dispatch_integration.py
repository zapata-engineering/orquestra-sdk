################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import json
import os
import sys
from unittest.mock import Mock

try:
    import importlib.metadata as metadata  # type: ignore
except ModuleNotFoundError:
    import importlib_metadata as metadata  # type: ignore

import pytest
import yaml

import orquestra.sdk as sdk
from orquestra.sdk._base import dispatch, loader
from orquestra.sdk._base._conversions import _yaml_exporter as yaml_converter
from orquestra.sdk.schema import ir

from .dirs import ch_temp_dir


@sdk.task(source_import=sdk.GitImport(repo_url="does_not.matter", git_ref="deadbeef"))
def uppercase(text):
    return text.upper()


@sdk.workflow
def wf_with_uppercasing():
    name = "emiliano zapata"
    name2 = uppercase(name)
    return [name2]


@sdk.task(source_import=sdk.GitImport(repo_url="does_not.matter", git_ref="deadbeef"))
def get_exec_ctx():
    """
    Validates that we set execution context for tasks on QE correctly.
    """
    import orquestra.sdk._base._exec_ctx

    return orquestra.sdk._base._exec_ctx.global_context.name


@sdk.workflow
def wf_with_ctx():
    return [get_exec_ctx()]


@pytest.mark.parametrize(
    "wf, expected_out",
    [
        # Checks that the task was executed correctly.
        (wf_with_uppercasing, '"EMILIANO ZAPATA"'),
        # Checks that we set the execution context flag correctly.
        (wf_with_ctx, '"PLATFORM_QE"'),
    ],
)
def test_execution(monkeypatch, wf, expected_out):
    """The dispatcher is complicated, with multiple layers of abstraction. This
    test verifies that something we output from the yaml converter can be
    executed by the dispatcher.

    This is how the System Under Test looks like::

                                      ┌────────┐
    ┌─────────────┐    ┌─────────┐    │QE v1   │    ┌───────────┐   ┌───────┐
    │@sdk.workflow├───►│yaml     ├───►│workflow├───►│dispatch.py├──►│files  │
    └─────────────┘    │converter│    │.yaml   │    └───────────┘   │on disk│
                       └─────────┘    └────────┘                    └───────┘
    """
    # 1. Get a QE v1 workflow representation
    # To maintain reproducible tests, we need to mock the response from importlib.
    # This is because when testing, we do not know the current state of the Git repo.
    # For example, we may have a dirty branch, have extra commits the release version
    # doesn't have, etc.
    version_mock = Mock(return_value="0.1.0")
    monkeypatch.setattr(metadata, "version", version_mock)
    yaml_model = yaml_converter.workflow_to_yaml(wf().model)

    # 2. Get into a single step specification
    yaml_str = yaml_converter.pydantic_to_yaml(yaml_model)
    yaml_wf = yaml.safe_load(yaml_str)
    assert len(yaml_wf["steps"]) == 1, "This test assumes a workflow with a single step"

    step_inputs = yaml_wf["steps"][0]["inputs"]

    # 3. Collect arguments in a similar way like python3-runtime would do. See also:
    # https://github.com/zapatacomputing/python3-runtime/blob/4f9eb3ff470683dbaaf8566ce9fc21db120d9c35/run#L300
    kwarg_inputs = {
        k: v
        for input_dict in step_inputs
        for k, v in input_dict.items()
        if not k == "type"
    }

    with ch_temp_dir() as temp_dir:
        # 4. Execute!
        dispatch.exec_task_fn(**kwarg_inputs)

        # 5. Validate outputs
        json_files = [
            f for f in os.listdir(temp_dir) if os.path.splitext(f)[1] == ".json"
        ]
        assert len(json_files) == 1

        with open(os.path.join(temp_dir, json_files[0])) as f:
            output_content = json.load(f)

        assert output_content == {
            "serialization_format": "JSON",
            "value": expected_out,
        }


class TestModuleCaching:
    module_name = "workflow_defs"
    project_dir = "tests/sdk/v2/data/sample_project"

    @classmethod
    def setup_class(cls):
        # Ensure clean state
        try:
            del sys.modules[cls.module_name]
        except KeyError:
            pass

        try:
            sys.path.remove(cls.project_dir)
        except ValueError:
            pass

    @classmethod
    def teardown_class(cls):
        # Ensure clean state
        try:
            del sys.modules[cls.module_name]
        except KeyError:
            pass

        try:
            sys.path.remove(cls.project_dir)
        except ValueError:
            pass

    def test_loader_does_not_affect_regular_locate(self):
        # 1. Simulate loading the module like `orq submit workflow-defs` would.
        module_with_fakes = loader.get_workflow_defs_module(
            self.project_dir, import_block_list=["helpers"]
        )

        # Verify that the import was faked.
        with pytest.raises(RuntimeError):
            _ = module_with_fakes.capitalize_task._TaskDef__sdk_task_body("hello")

        # 2. Simulate loading the module to execute the function like a Runtime would.
        dispatch.ensure_sys_paths([self.project_dir])

        fn = dispatch.locate_fn_ref(
            ir.ModuleFunctionRef(
                module=self.module_name,
                function_name="capitalize_task",
            )
        )
        # If this doesn't raise, we're good. It would raise a RuntimeError if
        # the module with fakes was cached.
        assert fn._TaskDef__sdk_task_body("hello") == "Hello"
