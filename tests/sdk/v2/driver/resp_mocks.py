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


def make_get_wf_run_logs_response_with_content():
    return b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xec\\mo\xdb\xba\x15\xce\xe7\xfd\n\xc2_\xeav\xb6D\xea\xd52\x96a\x1d\x9a\xde\x15\xb8\xcb\x1d\xb2\x14\xc3\x10\x05\x02E\x1d\xd9jd\xd2#\xa9\xb8\xde\xc5\xfd\xef\x83,\xdbql9\xb7i\xf3F/\xfcbX\x92\xa5\xf3<|\xce!yxd\xa5a\xda/\xc5H\x1d=^\xc3\x18\xe3\x00\xe3\xc5'\xde\xf9\xf4<'\xf4\x8f\x88\xe7a\x8f`'\x08\xdc#L\x9c\xd0\xc1G\x08?\xa2M\xebV)M\xe5\x11\xc6R\x08}\xd7u\xbfw~\x1b\x9c!\xed\xe2\x82\x04\xe1`\x80=\x0f;\x16&\xfe  \xbd_;\x9a\x8e:\xc3\xceL\xc8\xab\xbc\x143\xab\xd6\x87%\xe9\xdc\x9a\xe5I\x06y\xffc\xce\xcf\xff\xdb\x97\x18\xe3N\xaf#\xe9<\xc9\x8b\x128\x9d@g\xd8\xb1\xf5djK:\xb7\x15(U\x08\x9e\x94T\x83\xd2v}\x0f\xbb\xbe#\xc8\xbeO\xc1\xa74\x8c|\x878\x0eIS\xea\x844%\x830\xcf\x06\x98\xd0\x80:\x91\x17B\x1a\xa5)\xcbR\x96E\x11\xf5R?\xc4N\xd0\xc7\xa4a\xb7\xef\xe0\xd0\x02);\xbdN)jS\x87\x94i!\x93\xda\x86\xe1\xdf)\xa7#\x90\x9d\xdf.{\xdb\xe0\"S\xb0\x89J\xdf\x1b\xdb\x13\x80\xa3\x01&\x91Kh\xca2\xec\x11\x82S\x97\x84\x01\xce\x1d\xcf\x07\x8f\x05$#,\xcd0\xce\xfd\xc8ss\x9af>\x00\xa3$\x8b`\x03\x9c\xeb\xdf\xee8M\xd5U\x83\x8dI\xa0\x1a\x92\xda\xac\x15\x80\xc3\xc1\xe9`\xc7\xedc\xb7O<D\xbc\xa1\xeb\x0c\xfd\xb0\xe7\xfb^\xac?\x9d~\xfc\x05\xd1iaM\xe7C\x07\xbb\xa8\xdfG\xffZ\xc2B_D\x8a\x1aV2\x0b]\x14\xd9q\xdc\x99\xe5K\x8c\x96\xcb\xfc<r\x07q\xe7\xd2j\xa1\xc9H\x926\xc4\xb0\x96@\xb28\x06_\x81U\xb5\x17H\x98\x08\r\x87\x02\xb8M\x15\x91\xeb.Uq\x0by\xad\x8fpP\xcb\xe3\x9c\xaa+\xa44\xd5\x95B\x17g\x9fOO?\x9d\xfet\x19\xeb\x8b\x1di\xfc\xa5\xe0\xd7\x82Q]\x08\xde\xc7\xfd\xfan\xfdL\xf4\xf5\xb8\xe0\xa3\xcb6\x06\xf1\x13\x84\xfe\x87\xe7\xf0\xd7\xb8\xa3\x8b\t(M'\xd3\xb83D\xf1\x06\xab\xe7+V\xad\xc8\xf5}\xd7\xff#\xc6C\x8c\xe3N\x0f\xc5\x9d\x12\xae\xa1l~prv\xf6\xcbYst\x05\xa79\x91dtT\x13O\"\xbf9=\x01\xa5\xe8hy\xf6\\R\x06)eW\xa8;\x11J#\t\x0c\xb8F\x8c\x96%*\xa9\xd2o\x87q\xcc\x11\xfaX\x94\x80\xe28\xee\xd8c1\x01[\xc8\xffT\xa0\xb4\xa4\xf65\xf0k\xbb,R{:\xd7c\xc1]+\xb2U\xa1\xa1?\xa5\xec\x8a\x8e@m\\\xaa\xb2+\xbb\x8e\x8c\xf6\xd2\xa2\xfav=T\x16\x1c\x10\x89\x9c\x1e*8Z\x04\xce\xc6=\x16\x8fEH\x82\xae$G3I\xa7S\xc8\xba\xef\n\xceA&T\x8eT\x0f\xbd[~\xbb\x9a\xd5\xdf\xdf>\x89\xa1^\xd8\x18\x9a\xd4\x0c%\xc9m+\x15\x94\xb9\x95\xe4\xbc\xfb\xae\xe2\xf5}![[\xba>\xd0n\xecg\x05R\xd9)\xf0/tR\xf0I5\x99\x80\x9c\xdb\x1f\x04\xab&\xc0\xb5\xb2\xff!\xc5\x17`z\xc3\xca\xfeJ\xde\xfd\xda\\\xcb\xb2\x15\x93T\xb3\xf1\xeas\xcb\xee\x86\xdfL$\x0b\xd7\x89c~\xf2\x95\xc1\xb4v\xab!\xfakI\xc7q\xcc\x1bu\xcc\xf2DV<)\xb2F\x1f-\xb1\xba\xbej\xe1\xd6\x9b\xd7\xedw\xd3\xb8\xf3[\xab\xa3\xba&:\xea\xef\xbb\xcb\xe1`]\xe9\xf3A]\xe9\x0e\x8f?$\xe6\xee\x15\xb8\x0e\t\xf8cJf+\xf6\x1e\x12m\xdf3\x84\xb4\xe2\xf7\xcc\xc4\xbf\x92\xcd\xe3\x8c\x83{F\xc1\xc3!pk0?\x1c`\xad\x8b\x0b\xcf\x8b\xf5b\xba\x8b\x9a\xe7\xd6\xb3[7\x8a\xeau\xc5g>\xa6<+!C \xa5\x90\xa8\xab\xaa\xe9T\x82RhV\xe81zs\xf6\xfe\xdf\xc9\xa7\x9fN\x7f9;I>\x9f\xfe\xed\xfd\xe9\x87\x9fO>$\x8b{\xfd\xf3\x98\xbcy;Dq\x851I/\xdc`\"\xe9|8\\-c\x9b\x9cE\xad\xc1\xf7\xac^\xc84+\x1aX\xaf\xf0\xbaoW?\x8c&\xa8;-\xb2c\x12\x0ez\xa8\x98\x1e\x13l9\x18[\xc4\xf5,\x12\xf5\x90\x84\xa9<\xfe\xd3\x82\xf3U/\xacW\x89\x941P\xca\xda\xf3L$\xd2Z\xfa\x88j\x84\xbf\x869\xf3\x98\xe7\x0c\x06a\x80\xff\xdc\x1e\t|\x13\xfb\xbb6\x0eVbV\xd6\x19\x9d\xd7+\xc5\x93\xba/w\xfa\xe6\xee\xe5\xf5n\x8f8\xae\xbf\xdb#\x07\xc4\xdd\xf7\x0f\xbe\xb5\xa9+\xa0\xf6\xf6\x8a\xfd&z\x12\xbf\x19\x7f\xef\x9d\xd6\xc0\xa1\x99|\xaeG\xe5=\x90\xbb\x87\x04\xf6q\xc53\xf0\xee\xd2N+\x8f\x033yDH\xd2B\x01j\xf7\x04CA=\xae8\xc2\xe8\xffG\x1c\xa2\xd2\xd3J\xa3c\x94W\x9cu\xdf\xad&\xf7w\xcd\xe9\r\xc5\xfa\xa8\xd9\x83\xe8\x9b\xb2\x07\x862ww\x081r\x97\xe0E$\x93\x0ce\xee\xc7\x93I\x86\x02\x7f\xe6d\x92\x91[I\x0f\x97K2\x14\xfe\x8bI%\x19\xc9\xdf7d\x92\x8c\xc4u>\x06DSq\rh\x9d^@3\xaa\x90\x1e\x03\xca\n\tL#F+\x05H\xe4\x8bc\xb9(K1+\xf8\xe8\xe6\xfa\xd6\xad\x1db$\x1b\x07\x93\xe52\x93\xfe\x9b U)i\x97\x82\xd1rk<c\x82\xb3JJ\xe0\xda\xce+]IPv\x92R\x05\x9b!\xc8s\x9bI\xb0\x04U\x95\xfa\x90\xd8\xd9\x1a\xc1\x92\x11\xe8\xa4A\xd9}U\xc1\x96\n\xdc\x88,'27,\xb5r\xe4\x98\xc9\xd1j-\xd4Ha\x1d\x8c\x0f\t\xe2\x83$W\xb6\xe3\xeb\x86D\x1c\xdc\x04\x8a\xedp\xde\xc6\xa1c\xa8+!Dg\xb4\xd0h\x9d^\x92\x15O*\xae\x8b2ab2-AC\xf7\x8bH\x93\"\xeb!&\xb8\x86\xaf\xba\x87fy\xa2\xb4\x90\xd0\x1aTLe\xe2a\xd5\xd4\xba\x11\xb0\xd4\xd3.\xc5\x87\xc4\xe3JQT\xcd9+\x845\xa2z\x0c\xed)\x7f\xe75\xf0\xec\x91\x8a\xeb\x07\xcd\xe8\xd4l\xcb&\x12h6_\xa4w[y4r\x8fz#_'[\x93\xd5f\xc2\xba5{\xdf\xd8\x93]M\xdcO\x16\xdd]\x08\xbe\xdc\x9d]\x1d\xbf(\xb2\xe3\x9d\xaa\xc1K\x94\xd3\xa2\x84\x0ce\x95l\x16U\xcb\x1f\xb7\xd6}\xbb\xc6\xec\xbf\xdez\xc7\xe1\xbe\xaf\x01\x98\x8f\xf3\xfe[\xc1\xde\x13\x14h\xa5\x9e?\xf0\xf2\x90\x10\xc8\xfc\xc0\x0bC\x16B\xc80P/%AHBp\x03\xe6\xfaYF\\\nA\x90\x06\x9e\x8b\xa9\x0b>\x1d\xdc@&\xe1`\xef\xab9{\x16\xae\x87\x03\xb6\xb5\xe6\xc6Y\x15\xf4\xb7\x85\xfb\xe1 \xd8y\xe7cO\x18P\x9aJ\rY\xab\xdb{O\x10'\x9f\x88.w\xb0Y\xa2\xb4\xc3\x97\x8b\x83\x9d\xb7 >\xbe\xff\xf4\xf3\xc9\x87K\x94U\x80\xb4@\x94od\xaa\x16\xc3K\x86\xd2\xf9\"7U\xfb\x9a\xf5\x10/K\x1c\x10\xe1\x9es7\xe1\xbesK\xa1ov\xb8{s3B-\xf8\x7f\xb2b#3;\xe1\xe5\x16\x1b\x99\xca\xe7w\x15\x1b\x99\n\xf6\xa5\x15\x1byO0\x13{$\xd1\xec\xaf\x140\x15\xd4K+62\x95\xc7\xef)62\x15\xeb\xf3\x17\x1by\x81\x99\xcc\xdd\x1dB\x0c\x05\xf5\xfc\xc5F\xa62\xf7\xc3\xc5F\xa6\x02\x7f\xe6b#\xef\t\xca\xc6\x1fU/?Xmd*\xfe\x17Snd&\x81\xdfPo\xe4\x1b\x13P\xf6\xfd\xe3\xcd\xfe\x94\xe1\xe5\x1f\x9e\xfb\xcf\x93^\xdbk{m\xaf\xcd\xe0\xf6\xbf\x00\x00\x00\xff\xff3\xce\x1fN\x00P\x00\x00"  # noqa: E501


def make_get_task_run_logs_response():
    """
    Based on:
        https://github.com/zapatacomputing/workflow-driver/blob/main/openapi/src/resources/task-run-logs.yaml#L13
    """

    return "bytes"
