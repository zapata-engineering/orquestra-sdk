################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
from pydantic import BaseModel

from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.workflow_run import WorkflowRunId


class StoredWorkflowRun(BaseModel):
    """
    A StoredWorkflowRun is the information required to get a WorkflowRun and its
    results from a runtime via a configuration. This is stored with a project, for
    example in a SQLite database.
    In comparison, a WorkflowRun (orquestra.sdk.schema.workflow_run.WorkflowRun) is the
    actual WorkflowRun that is returned from a runtime.
    """

    workflow_run_id: WorkflowRunId
    config_name: str
    workflow_def: WorkflowDef
