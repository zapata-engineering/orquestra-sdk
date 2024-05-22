################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""UI models. Common data structures between the "data" and "view" layers.

Classes here are containers for information we show to the users in the CLI UI.
Ideally, most data transformations would be already done (like counting the number of
inished tasks) but should be easy to assert on in tests (e.g. we prefer ``Instant``
objects instead of date strings).
"""
import typing as t
from dataclasses import dataclass

from orquestra.workflow_shared.dates import Instant
from orquestra.workflow_shared.schema import ir
from orquestra.workflow_shared.schema.workflow_run import RunStatus, WorkflowRunId


@dataclass(frozen=True)
class WFRunSummary:
    """UI model for ``orq wf view``."""

    @dataclass(frozen=True)
    class TaskRow:
        task_fn_name: str
        "Name of the function wrapped in the `@task` decorator"

        inv_id: ir.TaskInvocationId
        status: RunStatus
        message: t.Optional[str]
        "Misc runtime-provided message. We use this to tell the user sth wrong"

    wf_def_name: ir.WorkflowDefName
    wf_run_id: WorkflowRunId
    wf_run_status: RunStatus
    task_rows: t.Sequence[TaskRow]
    n_tasks_succeeded: int
    n_task_invocations_total: int


@dataclass(frozen=True)
class WFList:
    """UI model for ``orq wf list``."""

    @dataclass(frozen=True)
    class WFRow:
        workflow_run_id: str
        status: str
        tasks_succeeded: str
        start_time: t.Optional[Instant]
        owner: t.Optional[str] = None

    wf_rows: t.Sequence[WFRow]
