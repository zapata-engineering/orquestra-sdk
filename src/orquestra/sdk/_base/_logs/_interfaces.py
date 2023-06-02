################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Logs-related interfaces.
"""
import typing as t
from dataclasses import dataclass

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import WorkflowRunId


@dataclass(frozen=True)
class WorkflowLogs:
    per_task: t.Mapping[TaskInvocationId, t.Sequence[str]]
    """
    A mapping with task logs. Each key-value pair corresponds to one task
    invocation.
    - key: task invocation ID (see
        orquestra.sdk._base.ir.WorkflowDef.task_invocations)
    - value: log lines from running this task invocation
    """

    env_setup: t.Sequence[str]


class LogReader(t.Protocol):
    """
    A component that reads logs produced by tasks and workflows.
    """

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:  # pragma: no cover
        """
        Reads all available logs, specific to a single task invocation/run.

        Returns:
            Log lines printed when running this task invocation. If the task didn't
            produce any logs this should be an empty list.
        """
        # pragma: no cover
        ...

    def get_workflow_logs(
        self, wf_run_id: WorkflowRunId
    ) -> WorkflowLogs:  # pragma: no cover
        """
        Reads all available logs printed during execution of this workflow run.
        """
        ...
