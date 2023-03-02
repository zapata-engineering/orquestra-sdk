################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Utilities for presenting human-readable text output from dorq commands. These are
mostly adapters over the corq's formatters.
"""
import pprint
import sys
import typing as t
import webbrowser
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, List

import click
from tabulate import tabulate

from orquestra.sdk._base import _services, serde
from orquestra.sdk.schema import responses
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.workflow_run import (
    TaskInvocationId,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunOnlyID,
)

from ..._corq._format import per_command
from . import _errors
from . import _models as ui_models


class WrappedCorqOutputPresenter:
    """
    Uses corq's responses and formatters for pretty-printing dorq data.
    """

    def show_wf_runs_list(self, wf_runs: List[WorkflowRun]):
        resp = responses.GetWorkflowRunResponse(
            meta=responses.ResponseMetadata(
                success=True,
                code=responses.ResponseStatusCode.OK,
                message="Success",
            ),
            workflow_runs=wf_runs,
        )
        per_command.pretty_print_response(resp, project_dir=None)

    def show_submitted_wf_run(self, wf_run_id: WorkflowRunId):
        resp = responses.SubmitWorkflowDefResponse(
            meta=responses.ResponseMetadata(
                success=True,
                code=responses.ResponseStatusCode.OK,
                message="Success",
            ),
            workflow_runs=[WorkflowRunOnlyID(id=wf_run_id)],
        )
        per_command.pretty_print_response(resp, project_dir=None)

    def show_stopped_wf_run(self, wf_run_id: WorkflowRunId):
        click.echo(f"Workflow run {wf_run_id} stopped.")

    def show_dumped_wf_logs(self, path: Path):
        click.echo(f"Workflow logs saved at {path}")

    @staticmethod
    def _format_log_dict(logs: t.Mapping[TaskInvocationId, t.Sequence[str]]):
        return [
            line
            for invocation_id, invocation_lines in logs.items()
            for line in (f"task-invocation-id: {invocation_id}", *invocation_lines)
        ]

    def show_logs(self, logs: t.Mapping[TaskInvocationId, t.Sequence[str]]):
        resp = responses.GetLogsResponse(
            meta=responses.ResponseMetadata(
                success=True,
                code=responses.ResponseStatusCode.OK,
                message="Successfully got workflow run logs.",
            ),
            logs=self._format_log_dict(logs),
        )
        per_command.pretty_print_response(resp, project_dir=None)

    def show_error(self, exception: Exception):
        status_code = _errors.pretty_print_exception(exception)

        sys.exit(status_code.value)


class ArtifactPresenter:
    def show_task_outputs(
        self,
        values: t.Sequence[t.Any],
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
    ):
        """
        Prints a preview of the values produced by a task run.

        Args:
            values: plain, deserialized artifact values.
        """
        click.echo(
            f"In workflow {wf_run_id}, task invocation {task_inv_id} produced "
            f"{len(values)} outputs."
        )

        for value_i, value in enumerate(values):
            click.echo()
            click.echo(f"Output {value_i}. Object type: {type(value)}")
            click.echo("Pretty printed value:")
            click.echo(pprint.pformat(value))

    def show_workflow_outputs(
        self, values: t.Sequence[t.Any], wf_run_id: WorkflowRunId
    ):
        """
        Prints a preview of the output values produced by a workflow.

        Args:
            values: plain, deserialized artifact values.
        """
        click.echo(f"Workflow run {wf_run_id} has {len(values)} outputs.")

        for value_i, value in enumerate(values):
            click.echo()
            click.echo(f"Output {value_i}. Object type: {type(value)}")
            click.echo("Pretty printed value:")
            click.echo(pprint.pformat(value))

    def show_dumped_artifact(self, dump_details: serde.DumpDetails):
        """
        Prints summary after an artifact was stored on disk. Suitable for both workflow
        outputs and task outputs.
        """
        format_name: str
        if dump_details.format == ArtifactFormat.JSON:
            format_name = "a text json file"
        elif dump_details.format == ArtifactFormat.ENCODED_PICKLE:
            # Our enum case name is ENCODED_PICKLE, but this isn't entirely consistent
            # with the file contents. Here, we don't base64-encode the pickle bytes, we
            # just dump them directly to the file. Custom caption should help users
            # avoid the confusion.
            format_name = "a binary pickle file"
        else:
            format_name = dump_details.format.name

        click.echo(f"Artifact saved at {dump_details.file_path} " f"as {format_name}.")


class ServicePresenter:
    @contextmanager
    def show_progress(
        self, services: List[_services.Service], *, label: str
    ) -> Iterator[Iterable[_services.Service]]:
        """
        Starts a progress bar on the context enter.

        Yields an iterable of services; when you iterate over it, the progress bar is
        advanced.
        """
        with click.progressbar(
            services,
            show_eta=False,
            item_show_func=lambda svc: f"{label} {svc.name}"
            if svc is not None
            else None,
        ) as bar:
            yield bar

    def show_services(self, services: List[responses.ServiceResponse]):
        click.echo(
            tabulate(
                [
                    [
                        click.style(svc.name, bold=True),
                        click.style("Running", fg="green")
                        if svc.is_running
                        else click.style("Not Running", fg="red"),
                        svc.info,
                    ]
                    for svc in services
                ],
                colalign=("right",),
                tablefmt="plain",
            ),
        )


class LoginPresenter:
    def prompt_for_login(self, login_url, url, ce):
        click.echo("We were unable to automatically log you in.")
        click.echo("Please login to your Orquestra account using the following URL.")
        click.echo(login_url)
        click.echo(
            (
                "Then save the token using command: \n"
                f"orq login -s {url} -t <paste your token here>"
            )
            + (" --ce" if ce else "")
        )

    def prompt_config_saved(self, url, config_name):
        click.echo("Token saved in config file.")
        click.echo(f"Configuration name for {url} is {config_name}")

    def print_login_help(self):
        click.echo(
            "Continue the login process in your web browser. Press [ctrl-c] to cancel."
        )

    def open_url_in_browser(self, url) -> bool:
        return webbrowser.open(url)


def _format_datetime(dt: t.Optional[datetime]) -> str:
    if dt is None:
        # Print empty table cell
        return ""

    return dt.isoformat()


def _format_tasks_succeeded(summary: ui_models.WFRunSummary) -> str:
    return f"{summary.n_tasks_succeeded} / {summary.n_task_invocations_total}"


class WFRunPresenter:
    def show_wf_run(self, summary: ui_models.WFRunSummary):
        click.echo("Workflow overview")
        click.echo(
            tabulate(
                [
                    ["workflow def name", summary.wf_def_name],
                    ["run ID", summary.wf_run_id],
                    ["status", summary.wf_run_status.state.name],
                    [
                        "start time",
                        _format_datetime(summary.wf_run_status.start_time),
                    ],
                    [
                        "end time",
                        _format_datetime(summary.wf_run_status.end_time),
                    ],
                    ["tasks succeeded", _format_tasks_succeeded(summary)],
                ]
            )
        )
        click.echo()

        task_rows = [
            ["function", "invocation ID", "status", "start_time", "end_time", "message"]
        ]
        for task_row in summary.task_rows:
            task_rows.append(
                [
                    task_row.task_fn_name,
                    task_row.inv_id,
                    task_row.status.state.value,
                    _format_datetime(task_row.status.start_time),
                    _format_datetime(task_row.status.end_time),
                    task_row.message or "",
                ]
            )
        click.echo("Task details")
        click.echo(tabulate(task_rows, headers="firstrow"))
