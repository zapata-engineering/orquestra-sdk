################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from orquestra.sdk.schema.ir import ModuleFunctionRef, WorkflowDef

from .workflow_base import wf

wf.model = WorkflowDef(
    name="some_workflow",
    fn_ref=ModuleFunctionRef(
        module="not_real", function_name="not_real", type="MODULE_FUNCTION_REF"
    ),
    imports={},
    tasks={},
    artifact_nodes={},
    constant_nodes={},
    task_invocations={},
    output_ids=[],
)
