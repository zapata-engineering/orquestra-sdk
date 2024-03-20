################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Logs-related interfaces."""
import typing as t
from dataclasses import dataclass
from enum import Enum

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import WorkflowRunId


@dataclass(frozen=True)
class LogOutput:
    out: t.List[str]
    err: t.List[str]

    def __len__(self):
        return len(self.out) + len(self.err)


@dataclass(frozen=True)
class WorkflowLogs:
    per_task: t.Mapping[TaskInvocationId, LogOutput]
    """
    A mapping with task logs.

    Each key-value pair corresponds to one task invocation.

    * key: task invocation ID (see
      ``orquestra.sdk.schema.ir.WorkflowDef.task_invocations``)

    * value: log lines from running this task invocation
    """

    env_setup: LogOutput
    """Logs related to setting up execution environment."""

    system: LogOutput
    """Logs relating to the execution environment."""

    other: LogOutput
    """Log lines that don't match any other category we support at the moment.

    If this contains useful information, please consider upgrading with
    ``pip install --upgrade orquestra-sdk`` or report your use case to the SDK Team at
    Zapata Computing.
    """

    def get_log_type(
        self, log_type
    ) -> t.Union[t.Mapping[TaskInvocationId, LogOutput], LogOutput]:
        """Return the specified log type.

        This method wraps the regular attribute getters in order to allow parametrised
        access to individual log types.
        """
        if log_type == self.WorkflowLogTypeName.PER_TASK:
            return self.per_task
        elif log_type == self.WorkflowLogTypeName.SYSTEM:
            return self.system
        elif log_type == self.WorkflowLogTypeName.ENV_SETUP:
            return self.env_setup
        elif log_type == self.WorkflowLogTypeName.OTHER:
            return self.other
        raise ValueError(f"Unknown workflow log type '{log_type}'.")

    class WorkflowLogTypeName(Enum):
        """Enum for specifying the individual types of Workflow log."""

        PER_TASK = "per_task"
        SYSTEM = "system"
        ENV_SETUP = "env_setup"
        OTHER = "other"


class LogReader(t.Protocol):
    """A component that reads logs produced by tasks and workflows."""

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> LogOutput:  # pragma: no cover
        """Reads all available logs, specific to a single task invocation/run.

        Args:
            wf_run_id: ID of the workflow run containing the task.
            task_inv_id: ID of the task invocation for which we want the logs.

        Returns:
            Log lines printed when running this task invocation.
                If the task didn't produce any logs this should be an empty list.
        """
        # pragma: no cover
        ...

    def get_workflow_logs(
        self, wf_run_id: WorkflowRunId
    ) -> WorkflowLogs:  # pragma: no cover
        """Reads all available logs printed during execution of this workflow run."""
        ...
