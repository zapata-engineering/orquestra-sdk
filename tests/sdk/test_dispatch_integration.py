################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import sys

import pytest

from orquestra.sdk._client._base import _dsl, loader
from orquestra.sdk.shared import dispatch
from orquestra.sdk.shared.schema import ir


class TestModuleCaching:
    module_name = "workflow_defs"
    project_dir = "tests/sdk/data/sample_project"

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
        assert isinstance(fn, _dsl.TaskDef)
        # If this doesn't raise, we're good. It would raise a RuntimeError if
        # the module with fakes was cached.
        assert fn._TaskDef__sdk_task_body("hello") == "Hello"
