################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
"""Utilities for presenting human-readable text output from dorq commands.

These are mostly adapters over the corq's formatters.
"""
import os
import sys
import typing as t
import webbrowser
from contextlib import contextmanager
from functools import singledispatchmethod
from pathlib import Path
from typing import List, Optional, Sequence

import click
from graphviz import Digraph  # type: ignore
from graphviz.exceptions import ExecutableNotFound  # type: ignore
from orquestra.workflow_shared import serde
from orquestra.workflow_shared.dates import Instant, from_unix_time
from orquestra.workflow_shared.logs import LogOutput, WorkflowLogs
from orquestra.workflow_shared.schema import responses
from orquestra.workflow_shared.schema.configs import (
    ConfigName,
    RuntimeConfiguration,
    RuntimeName,
)
from orquestra.workflow_shared.schema.ir import ArtifactFormat, TaskInvocationId
from orquestra.workflow_shared.schema.workflow_run import WorkflowRunId
from rich.box import SIMPLE_HEAVY
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.pretty import Pretty
from rich.rule import Rule
from rich.spinner import Spinner
from rich.table import Column, Table
from tabulate import tabulate

from ...._base import _env
from . import _errors
from . import _models as ui_models


class RichPresenter:
    def __init__(self, console: Optional[Console] = None):
        self._console = console or Console()

    @contextmanager
    def progress_spinner(self, spinner_label: str = "Loading"):
        with Live(
            Spinner("dots", spinner_label), console=self._console, transient=True
        ) as live:
            yield live


class LogsPresenter(RichPresenter):
    """Present workflow and task logs."""

    @singledispatchmethod
    def _rich_logs(*args) -> RenderableType:
        """Format the logs into a list of strings to be printed."""
        raise NotImplementedError(
            f"No log lines constructor for args {args}"
        )  # pragma: no cover

    @_rich_logs.register(dict)
    @staticmethod
    def _(logs: dict) -> RenderableType:
        from rich.console import Group

        renderables = []
        for invocation_id, invocation_logs in logs.items():
            renderables.append(
                Group(
                    f"[bold]{invocation_id}[/bold]",
                    LogsPresenter._rich_logs(invocation_logs),
                )
            )
        return Group(*renderables)

    @_rich_logs.register(LogOutput)
    @staticmethod
    def _(logs: LogOutput) -> RenderableType:
        table = Table("Stream", "Content", show_header=False, box=SIMPLE_HEAVY)
        if len(logs.out) > 0:
            table.add_row("[bold blue]stdout[/bold blue]", "\n".join(logs.out))
        if len(logs.err) > 0:
            table.add_row("[bold red]stderr[/bold red]", "\n".join(logs.err))
        return table

    def show_logs(
        self,
        logs: t.Union[t.Mapping[TaskInvocationId, LogOutput], LogOutput],
        log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None,
    ):
        """Present logs to the user.

        Args:
            logs: The logs to display, this may be in a dictionary or as a plain
                LogOutput object.
            log_type: An optional name used to split multiple log types.
        """
        _logs = self._rich_logs(logs)
        renderables = [_logs]

        if log_type:
            _log_type = f"{log_type.value} logs".replace("_", " ")
            renderables.insert(0, Rule(_log_type, align="left"))
            renderables.append(Rule())
        self._console.print(Group(*renderables))

    def show_dumped_wf_logs(
        self, path: Path, log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None
    ):
        """Tell the user where logs have been saved.

        Args:
            path: The path to the dump file.
            log_type: additional information identify the type of logs saved.
        """
        self._console.print(
            f"Workflow {log_type.value + ' ' if log_type else ''}"
            f"logs saved at [bold]{path}[/bold]"
        )


class WrappedCorqOutputPresenter:
    """Uses corq's responses and formatters for pretty-printing dorq data."""

    def show_stopped_wf_run(self, wf_run_id: WorkflowRunId):
        click.echo(f"Workflow run {wf_run_id} stopped.")

    def show_error(self, exception: Exception):
        status_code = _errors.pretty_print_exception(exception)
        sys.exit(status_code.value)

    def show_message(self, message: str):
        click.echo(message=message)


class ArtifactPresenter(RichPresenter):
    def _values_table(self, values: t.Sequence[t.Any]) -> Table:
        table = Table("Index", "Type", "Pretty Printed", box=SIMPLE_HEAVY)
        for i, value in enumerate(values):
            table.add_row(str(i), f"{type(value)}", Pretty(value))
        return table

    def show_task_outputs(
        self,
        values: t.Sequence[t.Any],
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
    ):
        """Prints a preview of the values produced by a task run.

        Args:
            values: plain, deserialized artifact values.
            wf_run_id: ID of the workflow run, displayed as part of the summary.
            task_inv_id: ID of the task invocation, displayed as part of the summary.
        """
        header = (
            f"In workflow {wf_run_id}, task invocation {task_inv_id} "
            f"produced {len(values)} outputs."
        )
        self._console.print(Group(header, self._values_table(values)))

    def show_workflow_outputs(
        self, values: t.Sequence[t.Any], wf_run_id: WorkflowRunId
    ):
        """Prints a preview of the output values produced by a workflow.

        Args:
            values: plain, deserialized artifact values.
            wf_run_id: ID of the workflow run, displayed as part of the summary.
        """
        header = f"Workflow run {wf_run_id} has {len(values)} outputs."
        self._console.print(Group(header, self._values_table(values)))

    def show_dumped_artifact(self, dump_details: serde.DumpDetails):
        """Prints summary after an artifact was stored on disk.

        Suitable for both workflow outputs and task outputs.
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

        self._console.print(
            f"Artifact saved at {dump_details.file_path} " f"as {format_name}."
        )


class ServicePresenter(RichPresenter):
    def show_services(self, services: Sequence[responses.ServiceResponse]):
        status_table = Table(
            Column("Service", style="bold"),
            Column("Status"),
            Column("Info", style="blue"),
            box=SIMPLE_HEAVY,
            show_header=False,
        )
        for svc in services:
            status_table.add_row(
                svc.name,
                (
                    "[green]Running[/green]"
                    if svc.is_running
                    else "[red]Not Running[/red]"
                ),
                svc.info or "",
            )
        self._console.print(status_table)

    def show_failure(self, service_responses: Sequence[responses.ServiceResponse]):
        self.show_services(service_responses)

        sys.exit(responses.ResponseStatusCode.SERVICES_ERROR.value)


class LoginPresenter:
    """User-facing presentation for the steps of the login process."""

    def prompt_for_login(self, login_url, url):
        """Instruct the user how to log in manually."""
        click.echo("We were unable to automatically log you in.")
        click.echo("Please login to your Orquestra account using the following URL.")
        click.echo(login_url)
        click.echo(
            (
                "Then save the token using command: \n"
                f"orq login -s {url} -t <paste your token here>"
            )
        )

    def prompt_config_saved(self, url, config_name, runtime_name):
        """Report that the config has been successfully saved."""
        click.echo("Token saved in config file.")
        click.echo(
            f"Configuration name for {url} with runtime {runtime_name} "
            f"is '{config_name}'."
        )

    def print_login_help(self):
        """Direct the user to their browser for the login process."""
        click.echo(
            "Continue the login process in your web browser. Press [ctrl-c] to cancel."
        )

    def open_url_in_browser(self, url) -> bool:
        """Open the login url in the user's browser."""
        return webbrowser.open(url)


class ConfigPresenter:
    """Present config information to the user."""

    def print_configs_list(
        self,
        configs: t.Sequence[RuntimeConfiguration],
        status: t.Mapping[ConfigName, bool],
        message: t.Optional[str] = "Stored configs:",
    ):
        """Print a list of stored configs."""
        if not configs:
            click.echo(
                "No remote configs available. Create new config using "
                "`orq login -s <remote uri>` command"
            )
            return

        click.echo(message)
        click.echo(
            tabulate(
                [
                    [
                        # show config name
                        click.style(config.config_name, bold=True),
                        #
                        # show runtime name, colour coded blue for CE and red for QE
                        (
                            click.style(config.runtime_name, fg="blue")
                            if config.runtime_name == RuntimeName.CE_REMOTE
                            else click.style(config.runtime_name, fg="red")
                        ),
                        #
                        # show cluster URI
                        config.runtime_options["uri"],
                        #
                        # show a green tick if the token is current, and a red cross if
                        # it is not.
                        (
                            click.style("\u2713", fg="green")
                            if status[config.config_name]
                            else click.style("\u2A09", fg="red")
                        ),
                    ]
                    for config in configs
                ],
                colalign=("left",),
                tablefmt="plain",
                headers=[
                    click.style("Config Name", underline=True),
                    click.style("Runtime", underline=True),
                    click.style("Server URI", underline=True),
                    click.style("Current Token", underline=True),
                ],
            ),
        )


def _format_datetime(dt: t.Optional[Instant]) -> str:
    return dt.astimezone().replace(tzinfo=None).ctime() if dt else ""


def _format_tasks_succeeded(summary: ui_models.WFRunSummary) -> str:
    return f"{summary.n_tasks_succeeded} / {summary.n_task_invocations_total}"


class WFRunPresenter(RichPresenter):
    def show_submitted_wf_run(self, wf_run_id: WorkflowRunId):
        self._console.print(
            f"[green]Workflow Submitted![/green] Run ID: [bold]{wf_run_id}[/bold]"
        )

    def get_wf_run(self, summary: ui_models.WFRunSummary):
        summary_table = Table(
            Column(style="bold", justify="right"),
            Column(),
            show_header=False,
            box=SIMPLE_HEAVY,
        )
        summary_table.add_row("Workflow Def Name", summary.wf_def_name)
        summary_table.add_row("Run ID", summary.wf_run_id)
        summary_table.add_row("Status", summary.wf_run_status.state.name)
        summary_table.add_row(
            "Start Time", _format_datetime(summary.wf_run_status.start_time)
        )
        summary_table.add_row(
            "End Time", _format_datetime(summary.wf_run_status.end_time)
        )
        summary_table.add_row("Succeeded Tasks", _format_tasks_succeeded(summary))
        task_details = Table(
            "Function",
            "Invocation ID",
            "Status",
            "Start Time",
            "End Time",
            "Message",
            box=SIMPLE_HEAVY,
        )
        for task_row in summary.task_rows:
            task_details.add_row(
                task_row.task_fn_name,
                task_row.inv_id,
                task_row.status.state.name,
                _format_datetime(task_row.status.start_time),
                _format_datetime(task_row.status.end_time),
                task_row.message,
            )
        title = Rule("Workflow Overview", align="left")
        task_title = Rule("Task Details", align="left")
        return Group(title, summary_table, task_title, task_details)

    def show_wf_run(self, summary: ui_models.WFRunSummary):
        self._console.print(self.get_wf_run(summary))

    def show_wf_list(self, summary: ui_models.WFList):
        """Display a list of workflow runs.

        The list is shown as a table with the following headings:
        - Workflow Run ID
        - Status
        - Succeeded Tasks
        - Start Time
        - [Owner]

        The 'Owner' column is displayed as long as at least one run in the list has a
        populated ``owner`` field.

        Example output::

            Workflow Run ID   Status     Succeeded Tasks   Start Time                 Owner
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            wf1               SUCCEEDED  1/1               Fri Feb 24 08:26:07 2023   taylor.swift@zapatacomputing.com
            wf2               WAITING    0/1                                          taylor.swift@zapatacomputing.com

        Args:
            summary: A list of workflow run summaries to be displayed.
        """  # noqa: E501
        show_owner: bool = any(row.owner for row in summary.wf_rows)

        table = Table(
            "Workflow Run ID",
            "Status",
            "Succeeded Tasks",
            "Start Time",
            box=SIMPLE_HEAVY,
        )
        if show_owner:
            table.add_column(header="Owner")

        for run in summary.wf_rows:
            values: List[Optional[str]] = [
                run.workflow_run_id,
                run.status,
                run.tasks_succeeded,
                _format_datetime(run.start_time),
            ]
            if show_owner:
                values.append(run.owner)

            table.add_row(*values)

        self._console.print(table)


class PromptPresenter:
    def wf_list_for_prompt(self, wfs):
        # Create labels of wf that are printed by prompter
        # Label is <wf_id> <start_time> tabulated nicely to create good-looking
        # table
        # There is also expectations that labels correspond to matching wfs list indices
        wfs = sorted(
            wfs,
            key=lambda wf: (
                wf.status.start_time if wf.status.start_time else from_unix_time(0)
            ),
            reverse=True,
        )

        labels = [[wf.id, _format_datetime(wf.status.start_time)] for wf in wfs]
        tabulated_labels = tabulate(labels, tablefmt="plain").split("\n")

        return wfs, tabulated_labels

    def workspaces_list_to_prompt(self, workspaces):
        # Create labels of workspaces that are printed by prompter
        # Label is <display_name> <id> tabulated nicely to create good-looking
        # table
        labels = [[ws.name, ws.workspace_id] for ws in workspaces]

        try:
            curr_ws = os.environ[_env.CURRENT_WORKSPACE_ENV]
            for index, label in enumerate(labels):
                if label[1] == curr_ws:
                    label.append("(CURRENT WORKSPACE)")
                    # put current workspace at the top of the list so it is
                    # auto-selected
                    labels.insert(0, labels.pop(index))
                    workspaces = workspaces[:]
                    workspaces.insert(0, workspaces.pop(index))
                    break
        except KeyError:
            pass

        tabulated_labels = tabulate(labels, tablefmt="plain").split("\n")

        return tabulated_labels, workspaces

    def project_list_to_prompt(self, projects):
        # Create labels of projects that are printed by prompter
        # Label is <display_name> <id> tabulated nicely to create good-looking
        # table
        labels = [[p.name, p.project_id] for p in projects]

        try:
            curr_proj = os.environ[_env.CURRENT_PROJECT_ENV]
            for index, label in enumerate(labels):
                if label[1] == curr_proj:
                    label.append("(CURRENT PROJECT)")
                    # put current project at the top of the list so it is
                    # auto-selected
                    labels.insert(0, labels.pop(index))
                    projects = projects[:]
                    projects.insert(0, projects.pop(index))
                    break
        except KeyError:
            pass

        tabulated_labels = tabulate(labels, tablefmt="plain").split("\n")

        return tabulated_labels, projects


class GraphPresenter:
    """User-facing presentation for the graph representation of a workflow def."""

    def view(self, graph: Digraph, file: Optional[Path]):
        """Display the graph in a popup window.

        Args:
            graph: The graph to be shown.
            file: If specified, the graph will be saved to the specified location.

        Raises:
            ExecutableNotFound: when there is not a global GraphViz install.
        """
        try:
            graph.view(filename=file, cleanup=True)
        except ExecutableNotFound:
            raise
