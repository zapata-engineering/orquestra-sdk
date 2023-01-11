################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Log adapter adds a workflow context to logs, Workflow ID and Task ID.
"""

import json
import logging
import os
from typing import Tuple

from orquestra.sdk.schema.workflow_run import TaskRunId, WorkflowRunId

RAY_LOGS_FORMAT = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "filename": "%(filename)s:%(lineno)s", "message": %(message)s}'  # noqa
logging.basicConfig(
    format=RAY_LOGS_FORMAT,
    level=logging.INFO,
)


class TaggedWorkflowTaskLogger(logging.LoggerAdapter):
    """
    Adding a workflow/task context to log outputs.
    """

    def process(self, msg, kwargs):
        return (
            json.dumps(
                {
                    "run_id": f'{self.extra["workflow_run_id"]}@{self.extra["task_run_id"]}',  # noqa: E501
                    "logs": msg,
                }
            ),
            kwargs,
        )


def is_argo_backend():
    """
    Quantum Engine backend test.
    Argo Workflows are executed in pods, where ARGO_NODE_ID corresponds
    to the workflow step ID.
    """
    return "ARGO_NODE_ID" in os.environ


def get_argo_backend_ids() -> Tuple[WorkflowRunId, TaskRunId]:
    node_id = os.environ["ARGO_NODE_ID"]
    # Argo Workflow ID is the left part of the step ID
    # [wf-id]-[retry-number]-[step-number]
    workflow_id = "-".join(node_id.split("-")[:-2])
    return (workflow_id, node_id)


def get_argo_step_name():
    argo_template = json.loads(os.environ["ARGO_TEMPLATE"])
    return argo_template["name"]


def get_ray_backend_ids() -> Tuple[WorkflowRunId, TaskRunId]:
    from ray.workflow import workflow_context

    return (
        workflow_context.get_current_workflow_id(),
        workflow_context.get_current_task_id(),
    )


def workflow_logger():
    """
    Returns a Logger instance with a context of current workflow/task.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Fallback workflow and task IDs
    workflow_run_id = "wf.local_run.0000000"
    task_run_id = "tsk.task_local-0.00000"

    if is_argo_backend():
        # Workflow is running in the Orquestra QE environment
        workflow_run_id, task_run_id = get_argo_backend_ids()
    else:
        try:
            # Workflow may be running in the Ray environment
            workflow_run_id, task_run_id = get_ray_backend_ids()
        except (ModuleNotFoundError, AssertionError):
            # Ray is not installed or not in a running Ray workflow:
            # Workflow running via `local_run()` or InProcessRuntime
            # Continue with fallback
            pass

    return TaggedWorkflowTaskLogger(
        logger,
        {
            "workflow_run_id": workflow_run_id,
            "task_run_id": task_run_id,
        },
    )


def wfprint(*values):
    """
    This function wraps prints from workflow tasks.
    """
    logger = workflow_logger()
    logger.info(msg=" ".join(values))
