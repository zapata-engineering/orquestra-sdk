################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import json
import pathlib
import typing as t
from datetime import datetime
from functools import singledispatch

import pydantic
import tabulate

from orquestra.sdk.schema import ir, responses, workflow_run


@singledispatch
def pretty_print_response(
    response: pydantic.BaseModel,
    project_dir: t.Optional[str],
):
    """
    Selects a designated formatter for the response model, with a fallback to
    colorized JSON.

    Args:
        response: the Pydantic model to output
        project_dir: project directory. Required for rich formatting of some
            outputs. None if the CLI parser didn't define this arg.
    """
    # Deferred import for optimizing CLI start time.
    import orquestra.sdk._base.cli._corq._format.color

    orquestra.sdk._base.cli._corq._format.color.print_colorized_json(response)


@pretty_print_response.register
def _print_logs(
    response: responses.GetLogsResponse,
    project_dir: t.Optional[str],
):
    import orquestra.sdk._base.cli._corq._format.color

    for line in response.logs:
        if isinstance(line, pydantic.BaseModel):
            orquestra.sdk._base.cli._corq._format.color.print_colorized_json(line)
        else:
            print(line)


@pretty_print_response.register
def _print_dict(
    response: responses.GetArtifactsResponse,
    project_dir: t.Optional[str],
):
    print(tabulate.tabulate(response.artifacts.items()))


@pretty_print_response.register
def _print_submit(
    response: responses.SubmitWorkflowDefResponse,
    project_dir: t.Optional[str],
):
    assert len(response.workflow_runs) == 1
    (run,) = response.workflow_runs
    print(f"Workflow submitted! Run ID: {run.id}")


@pretty_print_response.register
def _print_list_runs(
    response: responses.ListWorkflowRunsResponse,
    project_dir: t.Optional[str],
):
    n_runs = len(response.workflow_runs)
    print(
        f"Found {n_runs} workflow run{'' if n_runs == 1 else 's'} matching parameters:"
        f"\n - prefix      : {response.filters['prefix'] or 'Any'}"
        f"\n - max_age     : {response.filters['max_age'] or 'Any'}"
        f"\n - status      : {response.filters['status'] or 'Any'}"
        f"\n - config_name : {response.filters['config_name'] or 'Any'}"
        f"\nfrom the following project directories:"
        + "".join([f"\n - {dir}" for dir in response.filters["project_directories"]])
    )
    if n_runs == 1:
        _print_single_run(response.workflow_runs[0], project_dir)
    elif n_runs > 1:
        _format_multiple_runs(response.workflow_runs)


@pretty_print_response.register
def _print_runs(
    response: responses.GetWorkflowRunResponse,
    project_dir: t.Optional[str],
):
    if len(response.workflow_runs) == 0:
        print("No workflow runs found.")
    elif len(response.workflow_runs) == 1:
        _print_single_run(response.workflow_runs[0], project_dir)
    else:
        _format_multiple_runs(response.workflow_runs)


def _get_wf_def(run_id: str, project_dir: str) -> ir.WorkflowDef:
    # Deferred import for CLI performance
    import orquestra.sdk._base._db

    project_path = pathlib.Path(project_dir)

    with orquestra.sdk._base._db.WorkflowDB.open_project_db(project_path) as db:
        stored_run = db.get_workflow_run(run_id)

    return stored_run.workflow_def


class _WorkflowDefAnalyzer:
    """
    Reads stored workflow def and parses information useful for pretty printing
    CLI responses.
    """

    def __init__(self, wf_def):
        self._wf_def = wf_def

        self._invocation_by_artifact_id = {
            output_id: inv
            for inv in self._wf_def.task_invocations.values()
            for output_id in inv.output_ids
        }

    @classmethod
    def read_from_project(cls, wf_run_id: str, project_dir: str):
        wf_def = _get_wf_def(wf_run_id, project_dir)
        return _WorkflowDefAnalyzer(wf_def)

    @property
    def wf_output_ids(self) -> t.Sequence[str]:
        return self._wf_def.output_ids

    @property
    def wf_def_name(self) -> str:
        return self._wf_def.name

    def fn_name_by_artifact(self, artifact_id: str) -> str:
        invocation = self._invocation_by_artifact_id[artifact_id]
        task_def = self._wf_def.tasks[invocation.task_id]
        return task_def.fn_ref.function_name

    def fn_name_by_invocation(self, invocation_id: str) -> str:
        invocation = self._wf_def.task_invocations[invocation_id]
        task_def = self._wf_def.tasks[invocation.task_id]
        return task_def.fn_ref.function_name


@pretty_print_response.register
def _print_results(
    response: responses.GetWorkflowRunResultsResponse,
    project_dir: t.Optional[str],
):
    assert project_dir is not None

    analyzer = _WorkflowDefAnalyzer.read_from_project(
        wf_run_id=response.workflow_run_id, project_dir=project_dir
    )

    print(f"Results of {response.workflow_run_id}")

    for res_i, (res, output_id) in enumerate(
        zip(response.workflow_results, analyzer.wf_output_ids)
    ):
        fn_name = analyzer.fn_name_by_artifact(output_id)
        print(
            f"""
Workflow result {res_i}, returned from {fn_name}(), presented as
{res.serialization_format.value}"
            """
        )
        if isinstance(res, responses.JSONResult):
            print(json.dumps(json.loads(res.value), indent=2))

        elif isinstance(res, responses.PickleResult):
            print("Please use Python API to load this result.")
            print(f"Serialized length: {sum(len(chunk) for chunk in res.chunks)}")

        else:
            print(res)

        print()


def _format_datetime(dt: t.Optional[datetime]) -> str:
    if dt is None:
        # Print empty table cell
        return ""

    return dt.isoformat()


def _print_single_run(run: responses.WorkflowRun, project_dir: t.Optional[str]):
    analyzer = _WorkflowDefAnalyzer(run.workflow_def)

    print("Workflow overview")
    print(
        tabulate.tabulate(
            [
                ["workflow def name", analyzer.wf_def_name],
                ["run ID", run.id],
                ["status", run.status.state.value],
                ["start time", _format_datetime(run.status.start_time)],
                ["end time", _format_datetime(run.status.end_time)],
                ["tasks succeeded", _tasks_number_summary(run)],
            ]
        )
    )
    print()

    task_rows = [
        ["function", "task run ID", "status", "start_time", "end_time", "message"]
    ]
    for task_run in run.task_runs:
        task_rows.append(
            [
                analyzer.fn_name_by_invocation(task_run.invocation_id),
                task_run.id,
                task_run.status.state.value,
                _format_datetime(task_run.status.start_time),
                _format_datetime(task_run.status.end_time),
                task_run.message or "",
            ]
        )
    print("Task details")
    print(tabulate.tabulate(task_rows, headers="firstrow"))


def _format_multiple_runs(runs: t.Sequence[responses.WorkflowRun]):
    headers = ["workflow run ID", "status", "tasks succeeded"]
    rows = [
        [run.id, run.status.state.value, _tasks_number_summary(run)] for run in runs
    ]
    print(tabulate.tabulate(rows, headers=headers))


def _tasks_number_summary(wf_run: responses.WorkflowRun) -> str:
    total = len(wf_run.task_runs)
    finished = sum(
        1
        for task_run in wf_run.task_runs
        if task_run.status.state == workflow_run.State.SUCCEEDED
    )
    return f"{finished}/{total}"
