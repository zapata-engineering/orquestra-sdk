################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Utilities for presenting human-readable text output from dorq commands. These are
mostly adapters over the corq's formatters.
"""
import sys
from contextlib import contextmanager
from typing import Iterable, Iterator, List, Optional

import click
from tabulate import tabulate

import orquestra.sdk._base._services as _services
from orquestra.sdk.schema import responses
from orquestra.sdk.schema.workflow_run import (
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
)

from ..._corq._format import per_command
from . import _errors


class WrappedCorqOutputPresenter:
    """
    Uses corq's responses and formatters for pretty-printing dorq data.
    """

    def show_wf_run(self, wf_run: WorkflowRun):
        resp = responses.GetWorkflowRunResponse(
            meta=responses.ResponseMetadata(
                success=True,
                code=responses.ResponseStatusCode.OK,
                message="Success",
            ),
            workflow_runs=[wf_run],
        )
        per_command.pretty_print_response(resp, project_dir=None)

    def show_submitted_wf_run(self, wf_run_id: WorkflowRunId):
        resp = responses.SubmitWorkflowDefResponse(
            meta=responses.ResponseMetadata(
                success=True,
                code=responses.ResponseStatusCode.OK,
                message="Success",
            ),
            workflow_runs=[WorkflowRunMinimal(id=wf_run_id)],
        )
        per_command.pretty_print_response(resp, project_dir=None)

    def show_stopped_wf_run(self, wf_run_id: WorkflowRunId):
        click.echo(f"Workflow run {wf_run_id} stopped.")

    def show_error(self, exception: Exception):
        status_code = _errors.pretty_print_exception(exception)

        sys.exit(status_code.value)


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
