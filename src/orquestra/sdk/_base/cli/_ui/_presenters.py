################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Utilities for presenting human-readable text output from dorq commands. These are
mostly adapters over the corq's formatters.
"""
import os
import pprint
import sys
import typing as t
import webbrowser
from contextlib import contextmanager
from functools import singledispatchmethod
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence

import click
from tabulate import tabulate

from orquestra.sdk._base import _config, _dates, _env, _services, serde
from orquestra.sdk._base._dates import Instant
from orquestra.sdk._base._logs._interfaces import WorkflowLogs
from orquestra.sdk.schema import responses
from orquestra.sdk.schema.configs import ConfigName, RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.workflow_run import (
    TaskInvocationId,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunOnlyID,
)

from . import _errors
from . import _models as ui_models
from ._corq_format import per_command


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

    def show_dumped_wf_logs(
        self, path: Path, log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None
    ):
        """
        Tell the user where logs have been saved.

        Args:
            path: The path to the dump file.
            log_type: additional information identify the type of logs saved.
        """
        click.echo(
            f"Workflow {log_type.value + ' ' if log_type else ''}logs saved at {path}"
        )

    @singledispatchmethod
    @staticmethod
    def _format_logs(*args) -> t.List[str]:
        """
        Format the logs into a list of strings to be printed.
        """
        raise NotImplementedError(
            f"No log lines constructor for args {args}"
        )  # pragma: no cover

    @_format_logs.register(dict)
    @staticmethod
    def _(logs: dict):
        return [
            line
            for invocation_id, invocation_lines in logs.items()
            for line in (f"task-invocation-id: {invocation_id}", *invocation_lines)
        ]

    @_format_logs.register(list)
    @staticmethod
    def _(logs: list):
        return logs

    def show_logs(
        self,
        logs: t.Union[t.Mapping[TaskInvocationId, t.Sequence[str]], t.Sequence[str]],
        log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None,
    ):
        """
        Present logs to the user.
        """
        _logs = self._format_logs(logs)

        resp = responses.GetLogsResponse(
            meta=responses.ResponseMetadata(
                success=True,
                code=responses.ResponseStatusCode.OK,
                message="Successfully got workflow run logs.",
            ),
            logs=_logs,
        )

        if log_type:
            _log_type = f"{log_type.value} logs".replace("_", " ")
            click.echo(f"=== {_log_type.upper()} " + "=" * (75 - len(_log_type)))
        per_command.pretty_print_response(resp, project_dir=None)
        if log_type:
            click.echo("=" * 80 + "\n\n")

    def show_error(self, exception: Exception):
        status_code = _errors.pretty_print_exception(exception)

        sys.exit(status_code.value)

    def show_message(self, message: str):
        click.echo(message=message)


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
        self, services: Sequence[_services.Service], *, label: str
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

    def show_services(self, services: Sequence[responses.ServiceResponse]):
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

    def show_failure(self, service_responses: Sequence[responses.ServiceResponse]):
        self.show_services(service_responses)

        sys.exit(responses.ResponseStatusCode.SERVICES_ERROR.value)


class LoginPresenter:
    """User-facing presentation for the steps of the login process."""

    def prompt_for_login(self, login_url, url, ce):
        """Instruct the user how to log in manually."""
        click.echo("We were unable to automatically log you in.")
        click.echo("Please login to your Orquestra account using the following URL.")
        click.echo(login_url)
        click.echo(
            (
                "Then save the token using command: \n"
                f"orq login -s {url} -t <paste your token here>"
            )
            + (" --ce" if ce else " --qe")
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
    """
    Present config information to the user.
    """

    def print_configs_list(
        self,
        configs: t.Sequence[RuntimeConfiguration],
        status: t.Mapping[ConfigName, bool],
        message: t.Optional[str] = "Stored configs:",
    ):
        """
        Print a list of stored configs.
        """
        click.echo(message)
        click.echo(
            tabulate(
                [
                    [
                        # show config name
                        click.style(config.config_name, bold=True),
                        #
                        # show runtime name, colour coded blue for CE and green for QE
                        click.style(config.runtime_name, fg="blue")
                        if config.runtime_name == RuntimeName.CE_REMOTE
                        else click.style(config.runtime_name, fg="green"),
                        #
                        # show cluster URI
                        config.runtime_options["uri"],
                        #
                        # show a green tick if the token is current, and a red cross if
                        # it is not.
                        click.style("\u2713", fg="green")
                        if status[config.config_name]
                        else click.style("\u2A09", fg="red"),
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

    def show_wf_list(self, summary: ui_models.WFList):
        rows = [["Workflow Run ID", "Status", "Tasks Succeeded", "Start Time"]]
        for model_row in summary.wf_rows:
            rows.append(
                [
                    model_row.workflow_run_id,
                    model_row.status,
                    model_row.tasks_succeeded,
                    _format_datetime(model_row.start_time),
                ]
            )
        click.echo(tabulate(rows, headers="firstrow"))


class PromptPresenter:
    def wf_list_for_prompt(self, wfs):
        # Create labels of wf that are printed by prompter
        # Label is <wf_id> <start_time> tabulated nicely to create good-looking
        # table
        # There is also expectations that labels correspond to matching wfs list indices
        wfs = sorted(
            wfs,
            key=lambda wf: wf.status.start_time
            if wf.status.start_time
            else _dates.from_unix_time(0),
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
