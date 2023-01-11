################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Contains utilities for setting up the testing environment to replicate a
user's project state.
"""

from orquestra.sdk.schema import ir

TINY_WORKFLOW_DEF = ir.WorkflowDef(
    name="single_invocation",
    fn_ref=ir.FileFunctionRef(
        file_path="empty.py",
        function_name="empty",
        line_number=0,
        type="FILE_FUNCTION_REF",
    ),
    imports={},
    tasks={},
    artifact_nodes={},
    constant_nodes={},
    task_invocations={},
    output_ids=[],
)
