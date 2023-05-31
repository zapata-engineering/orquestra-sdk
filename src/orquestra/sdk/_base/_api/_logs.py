################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t
from dataclasses import dataclass
from orquestra.sdk.schema import ir

from orquestra.sdk.schema.workflow_run import WorkflowRunId

from orquestra.sdk._base.abc import RuntimeInterface


@dataclass(frozen=True)
class WorkflowLogs:
    per_task: t.Mapping[ir.TaskInvocationId, t.Sequence[str]]
    env_setup: t.Sequence[str]


@dataclass(frozen=True)
class Logs:
    per_task: t.Mapping[ir.TaskInvocationId, t.Sequence[str]]
    env_setup: t.Sequence[str]
    system: t.Sequence[str]


class LogsClient:
    def __init__(self, wf_run_id: WorkflowRunId, runtime: RuntimeInterface):
        self._wf_run_id = wf_run_id
        self._runtime = runtime

    def get_env_setup(self):
        pass

    def get_all_standard(self):
        pass

    def get_all_system(self):
        pass

    def stream_all_standard(self) -> t.Iterable[str]:
        return []

    def stream_all_system(self) -> t.Iterable[str]:
        return []
