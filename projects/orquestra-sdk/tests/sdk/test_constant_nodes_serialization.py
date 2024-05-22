################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import pytest
from orquestra.workflow_shared import exceptions

from orquestra.sdk._client._base._workflow import WorkflowDef
from orquestra.sdk._client._base.loader import (
    get_attributes_of_type,
    get_workflow_defs_module,
)


def test_constant_serialization_when_loading_from_file():
    module = get_workflow_defs_module("tests/sdk/data/complex_serialization")
    defs = get_attributes_of_type(module, WorkflowDef)
    for wf in defs:
        if "should_fail" in wf.name:
            with pytest.raises(exceptions.WorkflowSyntaxError):
                wf.model
        else:
            wf.model
