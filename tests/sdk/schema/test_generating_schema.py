################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk.schema import (
    configs,
    ir,
    local_database,
    responses,
    workflow_run,
    yaml_model,
)


@pytest.mark.parametrize(
    "root_cls",
    [
        ir.WorkflowDef,
        yaml_model.Workflow,
        workflow_run.WorkflowRun,
        responses.GetWorkflowDefResponse,
        responses.GetTaskDefResponse,
        responses.ErrorResponse,
        responses.SubmitWorkflowDefResponse,
        responses.GetWorkflowRunResponse,
        responses.GetWorkflowRunResultsResponse,
        responses.GetLogsResponse,
        local_database.StoredWorkflowRun,
        configs.RuntimeConfigurationFile,
    ],
)
def test_schema_can_be_generated(root_cls):
    # if we did something wrong this would raise an error
    root_cls.schema_json()
