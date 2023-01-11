################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from orquestra.sdk.schema.ir import ModuleFunctionRef, TaskDef

from .task_base import task

task.model = TaskDef(
    id="something",
    source_import_id="import-1",
    parameters=[],
    fn_ref=ModuleFunctionRef(
        module="not_real", function_name="not_real", type="MODULE_FUNCTION_REF"
    ),
)
