################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import os
from unittest import mock

from ray.workflow import workflow_context

from orquestra.sdk import wfprint, workflow_logger
from orquestra.sdk._base._log_adapter import (
    get_argo_backend_ids,
    get_argo_step_name,
    get_ray_backend_ids,
    is_argo_backend,
)

LOCAL_UNIFIED_ID = "wf.local_run.0000000@tsk.task_local-0.00000"
TEST_ARGO_NODE_ID = "hello-orquestra-nk148-r000-4219834842"
TEST_ARGO_TEMPLATE = """{
    "name": "hello",
    "inputs": "",
    "outputs": ""
}"""
TEST_WF_ID = "hello-orquestra-nk148"
TEST_TASK_ID = "some-task-id-xyz"


def test_workflow_logger(caplog):
    log = workflow_logger()
    log.info("test test test")
    assert LOCAL_UNIFIED_ID in caplog.text


def test_wfprint(caplog):
    wfprint("hello orquestra")
    assert LOCAL_UNIFIED_ID in caplog.text


@mock.patch.dict(os.environ, {"ARGO_NODE_ID": TEST_ARGO_NODE_ID})
def test_is_argo_backend():
    assert is_argo_backend()


def test_not_argo_backend():
    assert not is_argo_backend()


@mock.patch.dict(os.environ, {"ARGO_NODE_ID": TEST_ARGO_NODE_ID})
def test_get_argo_backend_ids():
    workflow_id, task_id = get_argo_backend_ids()
    assert workflow_id == TEST_WF_ID
    assert task_id == TEST_ARGO_NODE_ID


@mock.patch.dict(os.environ, {"ARGO_TEMPLATE": TEST_ARGO_TEMPLATE})
def test_get_argo_step_name():
    assert get_argo_step_name() == "hello"


def test_get_ray_backend_ids():
    with workflow_context.workflow_task_context(
        context=workflow_context.WorkflowTaskContext(
            workflow_id=TEST_WF_ID,
            task_id=TEST_TASK_ID,
        )
    ):
        workflow_id, task_id = get_ray_backend_ids()
        assert workflow_id == TEST_WF_ID
        assert task_id == TEST_TASK_ID
