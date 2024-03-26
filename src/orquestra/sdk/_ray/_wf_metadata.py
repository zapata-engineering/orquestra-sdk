################################################################################
# © Copyright 2023 - 2024 Zapata Computing Inc.
################################################################################

import json
import typing as t

from .._base._storage import BaseModel
from ..schema import ir, workflow_run


class WfUserMetadata(BaseModel):
    """Information about a workflow run we store as a Ray metadata dict.

    Pydantic helps us check that the thing we read from Ray is indeed a dictionary we
    set (i.e. it has proper fields).
    """

    # Full definition of the workflow that's being run.
    workflow_def: ir.WorkflowDef


class InvUserMetadata(BaseModel):
    """Information about a task invocation we store as a Ray metadata dict.

    Pydantic helps us check that the thing we read from Ray is indeed a dictionary we
    set (i.e. it has proper fields).
    """

    # Invocation ID. Scoped to a single workflow def. Allows to distinguish
    # between multiple calls of as single task def inside a workflow.
    # Duplicated across workflow runs.
    task_invocation_id: ir.TaskInvocationId

    # (Hopefully) globally unique identifier of as single task execution. Allows
    # to distinguish invocations of the same task across workflow runs.
    task_run_id: workflow_run.TaskRunId


def pydatic_to_json_dict(pydantic_obj) -> t.Dict[str, t.Any]:
    """Produces a JSON-serializable dict."""
    return json.loads(pydantic_obj.model_dump_json())
