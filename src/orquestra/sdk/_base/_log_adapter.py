################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Log adapter adds a workflow context to logs, Workflow ID and Task ID.
"""

import json
import logging
import os
import typing as t
from datetime import datetime, timezone

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import TaskRunId, WorkflowRunId

# NOTE: the `message`, `wf_run_id`, and `task_run_id` value placeholders don't come with
# "" quotes. We already add them when we json.dumps() the value. This ensure proper JSON
# escaping and handling null values.
FORMAT = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "filename": "%(filename)s:%(lineno)s", "message": %(message)s, "wf_run_id": %(wf_run_id)s, "task_inv_id": %(task_inv_id)s, "task_run_id": %(task_run_id)s}'  # noqa


class TaggedWorkflowTaskLogger(logging.LoggerAdapter):
    """
    Adding a workflow/task context to log outputs.
    """

    def process(
        self, msg, kwargs: t.Mapping[str, t.Any]
    ) -> t.Tuple[str, t.MutableMapping[str, t.Any]]:
        """
        Args:
            msg: message passed to the standard logger method calls. The type of this
                object depends on internals of ``logging``, but we're assuming it's a
                string, or something close to it.
            kwargs: custom kwargs passed to the standard logger method calls.
        """

        # Ensure the message can be stored as a JSON. This will escape any
        # JSON-forbidden chars.
        new_msg = json.dumps(str(msg))

        # Workaround cases where run IDs are nulls.
        old_extra = self.extra or {}
        new_extra = dict(old_extra)
        for key in ["wf_run_id", "task_inv_id", "task_run_id"]:
            new_extra[key] = json.dumps(old_extra[key])

        # Mimic default behavior of logging.LoggerAdapter. Note we keep run IDs as
        # extras.
        new_kwargs = {**kwargs, "extra": new_extra}
        return new_msg, new_kwargs


def is_argo_backend():
    """
    Quantum Engine backend test.
    Argo Workflows are executed in pods, where ARGO_NODE_ID corresponds
    to the workflow step ID.
    """
    return "ARGO_NODE_ID" in os.environ


def get_argo_backend_ids() -> t.Tuple[WorkflowRunId, TaskInvocationId, TaskRunId]:
    node_id = os.environ["ARGO_NODE_ID"]
    # Argo Workflow ID is the left part of the step ID
    # [wf-id]-[retry-number]-[step-number]
    wf_run_id = "-".join(node_id.split("-")[:-2])
    task_run_id = node_id

    argo_template = json.loads(os.environ["ARGO_TEMPLATE"])
    # Looks like the template name on Argo matches our task invocation ID. Not sure how
    # good this assumption is.
    task_inv_id = argo_template["name"]

    return wf_run_id, task_inv_id, task_run_id


def get_ray_backend_ids() -> (
    t.Tuple[
        t.Optional[WorkflowRunId], t.Optional[TaskInvocationId], t.Optional[TaskRunId]
    ]
):
    try:
        # Deferred import because Ray isn't installed when running on QE.
        import orquestra.sdk._ray._dag

    except ModuleNotFoundError:
        # Ray is not installed
        return None, None, None

    return orquestra.sdk._ray._dag.get_current_ids()


class ISOFormatter(logging.Formatter):
    """
    Overrides the default date formatting to produce ISO 8601 strings.
    """

    def formatTime(self, record, datefmt=None) -> str:
        """
        Override for logging.Formatter.formatTime.

        Example output: ``2023-02-02T11:45:21.504754+00:00``
        """
        utc_timestamp = record.created
        instant = datetime.fromtimestamp(utc_timestamp, timezone.utc)
        return instant.isoformat()


def _make_logger(
    wf_run_id: t.Optional[WorkflowRunId],
    task_inv_id: t.Optional[TaskInvocationId],
    task_run_id: t.Optional[TaskRunId],
):
    # Note: there are two loggers: "nested logger" and "main logger".
    #
    # The "nested logger" is a singleton managed by `logging`. It's likely to be
    # retained across task and workflow runs, as long as the worker process lives.
    # It knows about the log message formats and log levels.
    #
    # The "main logger" is the one we expose to the user. It wraps the "nested logger"
    # to inject contextual information that changes _often_. We use it to pass
    # `wf_run_id` and `task_run_id` because these values can be different for each
    # executed task run. The "main logger" should not be retained. We create it every
    # time user asks us.
    nested_logger = logging.getLogger(__name__)

    # Note: `logging.basicConfig` does a similar thing like the following few lines. We
    # can't use the shorthand because it would configure the root logger. Here, we only
    # want to affect the workflow logger.
    formatter = ISOFormatter(FORMAT)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # We need to ensure that a proper handler with proper formatter is hooked up with
    # the logger. The only way to that is to remove all handlers from the logger and add
    # the one we care about.
    nested_logger.handlers.clear()
    nested_logger.addHandler(handler)

    nested_logger.setLevel(logging.INFO)

    main_logger = TaggedWorkflowTaskLogger(
        nested_logger,
        extra={
            "wf_run_id": wf_run_id,
            "task_inv_id": task_inv_id,
            "task_run_id": task_run_id,
        },
    )

    return main_logger


def workflow_logger() -> logging.LoggerAdapter:
    """
    Returns a Logger instance with a context of current workflow/task.

    Each call of this function creates a new object. It shouldn't be retained across
    task runs.
    """
    wf_run_id: t.Optional[WorkflowRunId]
    task_inv_id: t.Optional[TaskInvocationId]
    task_run_id: t.Optional[TaskRunId]
    if is_argo_backend():
        # Workflow is running in the Orquestra QE environment
        wf_run_id, task_inv_id, task_run_id = get_argo_backend_ids()
    else:
        # We assume the workflow is running on Ray
        wf_run_id, task_inv_id, task_run_id = get_ray_backend_ids()

    logger = _make_logger(
        wf_run_id=wf_run_id, task_inv_id=task_inv_id, task_run_id=task_run_id
    )

    return logger


def wfprint(*values):
    """
    This function wraps prints from workflow tasks.
    """
    logger = workflow_logger()
    logger.info(msg=" ".join(values))
