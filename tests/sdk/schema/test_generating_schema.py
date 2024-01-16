################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk.schema import configs, ir, workflow_run


@pytest.mark.parametrize(
    "root_cls",
    [
        ir.WorkflowDef,
        workflow_run.WorkflowRun,
        configs.RuntimeConfigurationFile,
    ],
)
def test_schema_can_be_generated(root_cls):
    # if we did something wrong this would raise an error
    root_cls.model_json_schema()
