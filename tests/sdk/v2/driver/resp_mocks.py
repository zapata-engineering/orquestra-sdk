################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Recorded HTTP response data. Extracted from the test file because this usually
takes a lot of lines. Kept as a Python file for some DRY-ness.
"""


from typing import Any, List

from orquestra.sdk._base._driver._models import (
    TaskInvocationID,
    TaskRunID,
    WorkflowDefID,
    WorkflowRunID,
)
from orquestra.sdk._base.serde import result_from_artifact
from orquestra.sdk.schema.ir import ArtifactFormat, WorkflowDef
from orquestra.sdk.schema.workflow_run import RunStatus, TaskRun

# --- Helpers ---
# These helpers are used to reduce code duplication when creating test responses

PLATFORM_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def _wf_def_resp(id_: WorkflowDefID, wf_def: WorkflowDef):
    return {
        "id": id_,
        "created": "2022-11-23T18:58:13.86752161Z",
        "owner": "evil/emiliano.zapata@zapatacomputing.com",
        "workflow": wf_def.dict(),
    }


def _status_resp(status: RunStatus):
    _status = {"state": status.state.value}
    if status.start_time is not None:
        _status["startTime"] = status.start_time.strftime(PLATFORM_TIME_FORMAT)
    if status.end_time is not None:
        _status["endTime"] = status.end_time.strftime(PLATFORM_TIME_FORMAT)
    return _status


def _task_run_resp(
    id_: TaskRunID,
    task_invocation_id: TaskInvocationID,
    status: RunStatus,
):
    return {
        "id": id_,
        "invocationId": task_invocation_id,
        "status": _status_resp(status),
    }


def _wf_run_resp(
    id_: WorkflowRunID,
    workflow_def_id: WorkflowDefID,
    status: RunStatus,
    task_runs: List[TaskRun],
):
    return {
        "id": id_,
        "definitionId": workflow_def_id,
        "status": _status_resp(status),
        "owner": "evil/emiliano.zapata@zapatacomputing.com",
        "taskRuns": [
            _task_run_resp(t.id, t.invocation_id, t.status) for t in task_runs
        ],
    }


def _list_wf_run_resp(
    id_: WorkflowRunID,
    workflow_def_id: WorkflowDefID,
):
    return {
        "id": id_,
        "definitionId": workflow_def_id,
    }


# --- Workflow Definitions ---


def make_get_wf_def_response(id_: WorkflowDefID, wf_def: WorkflowDef):
    """
    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/schemas/WorkflowDefinition.yaml
    """
    return {
        "data": _wf_def_resp(id_, wf_def),
    }


def make_list_wf_def_response(ids: List[WorkflowDefID], wf_defs: List[WorkflowDef]):
    return {
        "data": [_wf_def_resp(id_, wf_def) for id_, wf_def in zip(ids, wf_defs)],
    }


def make_list_wf_def_paginated_response(
    ids: List[WorkflowDefID], wf_defs: List[WorkflowDef]
):
    return {
        "data": [_wf_def_resp(id_, wf_def) for id_, wf_def in zip(ids, wf_defs)],
        "meta": {
            "nextPageToken": "1989-12-13T00:00:00.000000Z,"
            "00000000-0000-0000-0000-0000000000000",
        },
    }


def make_create_wf_def_response(id_: WorkflowDefID):
    """
    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/responses/CreateWorkflowDefinitionResponse.yaml
    """
    return {"data": {"id": id_}}


def make_error_response(message: str, detail: str):
    """
    Based on:
    https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/schemas/Error.yaml
    """

    return {
        "message": message,
        "detail": detail,
    }


# --- Workflow Runs ---


def make_submit_wf_run_response(id_: WorkflowRunID):
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/responses/CreateWorkflowRunResponse.yaml
    """
    return {"data": {"id": id_}}


def make_get_wf_run_response(
    id_: WorkflowRunID,
    workflow_def_id: WorkflowDefID,
    status: RunStatus,
    task_runs: List[TaskRun],
):
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/schemas/WorkflowRun.yaml
    """
    return {"data": _wf_run_resp(id_, workflow_def_id, status, task_runs)}


def make_get_wf_run_missing_task_run_status(
    id_: WorkflowRunID, workflow_def_id: WorkflowDefID, status: RunStatus
):
    wf_run = {"data": _wf_run_resp(id_, workflow_def_id, status, [])}
    wf_run["data"]["taskRuns"].append({"id": "xyz", "invocationId": "abc"})
    return wf_run


def make_list_wf_run_response(
    ids: List[WorkflowRunID],
    workflow_def_ids: List[WorkflowDefID],
):
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/workflow-runs.yaml#L1
    """
    # Assume empty task runs for now
    return {
        "data": [
            _list_wf_run_resp(id_, wf_def_id)
            for id_, wf_def_id, in zip(ids, workflow_def_ids)
        ]
    }


def make_list_wf_run_paginated_response(
    ids: List[WorkflowRunID],
    workflow_def_ids: List[WorkflowDefID],
):
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/workflow-runs.yaml#L1
    """
    # Assume empty task runs for now
    return {
        "data": [
            _list_wf_run_resp(id_, wf_def_id)
            for id_, wf_def_id in zip(ids, workflow_def_ids)
        ],
        "meta": {
            "nextPageToken": "1989-12-13T00:00:00.000000Z,"
            "00000000-0000-0000-0000-0000000000000",
        },
    }


def make_get_wf_run_artifacts_response():
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/artifacts.yaml#L15
    """
    return {
        "data": {
            "task-1": ["artifact-1"],
            "task-2": ["artifact-2", "artifact-3"],
            "task-3": ["artifact-4"],
        }
    }


def make_get_wf_run_artifact_response(result_obj: Any):
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/artifact.yaml#L13
    """

    return result_from_artifact(result_obj, ArtifactFormat.AUTO).dict()


def make_get_wf_run_results_response():
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/run-results.yaml#L15
    """
    return {
        "data": [
            "80ba3786-70c0-11ed-a1eb-0242ac120002",
            "80ba3a88-70c0-11ed-a1eb-0242ac120002",
            "80ba3c7c-70c0-11ed-a1eb-0242ac120002",
        ]
    }


def make_get_wf_run_result_response(result_obj: Any):
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/run-result.yaml#L13
    """

    return result_from_artifact(result_obj, ArtifactFormat.AUTO).dict()


def make_get_wf_run_logs_response():
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/workflow-run-logs.yaml#L13
    """

    return "bytes"


def make_get_task_run_logs_response():
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/task-run-logs.yaml#L13
    """

    return "bytes"
