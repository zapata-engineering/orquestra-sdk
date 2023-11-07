################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import sys
import traceback
from functools import singledispatch

import click
from rich.box import SIMPLE_HEAVY
from rich.console import Console
from rich.table import Column, Table

from orquestra.sdk import exceptions
from orquestra.sdk._base._config import IN_PROCESS_CONFIG_NAME, RAY_CONFIG_NAME_ALIAS
from orquestra.sdk.schema.responses import ResponseStatusCode


def _print_traceback(e: Exception):
    # Newer Python versions like 3.10 allow passing just the exception object to
    # traceback.format_exception(). Python 3.8 requires an explicit 3-argument form.

    tb_lines = traceback.format_exception(type(e), e, e.__traceback__)
    click.secho("".join(tb_lines), fg="red", file=sys.stderr)


@singledispatch
def pretty_print_exception(e: Exception) -> ResponseStatusCode:
    # The default case
    _print_traceback(e)
    click.echo(
        "Something unexpected happened. Please consider reporting this error to the "
        "SDK team at Zapata Computing."
    )

    return ResponseStatusCode.UNKNOWN_ERROR


@pretty_print_exception.register
def _(
    e: exceptions.WorkflowDefinitionModuleNotFound,
) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(
        f"Error: couldn't find workflow definitions module '{e.module_name}'. "
        f"Searched at {e.sys_path}"
    )
    return ResponseStatusCode.INVALID_WORKFLOW_DEF


@pretty_print_exception.register
def _(
    e: exceptions.NoWorkflowDefinitionsFound,
) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(f"Error: couldn't find any workflow definitions in '{e.module_name}'")
    return ResponseStatusCode.INVALID_WORKFLOW_DEF


@pretty_print_exception.register
def _(e: exceptions.UnauthorizedError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo("Error: Authorization failed. Please log in again.")
    return ResponseStatusCode.UNAUTHORIZED


@pretty_print_exception.register
def _(e: exceptions.WorkflowSyntaxError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(f"Invalid workflow syntax. {e.msg}")
    return ResponseStatusCode.INVALID_WORKFLOW_DEFS_SYNTAX


@pretty_print_exception.register
def _(e: exceptions.WorkflowRunNotSucceeded) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(
        "This action only works with succeeded workflows. However, the selected run "
        f"is {e.state.name}."
    )
    return ResponseStatusCode.INVALID_WORKFLOW_RUN


@pretty_print_exception.register
def _(e: exceptions.WorkflowRunNotFinished) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(
        "This action only works with finished workflows. However, the selected run "
        f"is {e.state.name}."
    )
    return ResponseStatusCode.INVALID_WORKFLOW_RUN


@pretty_print_exception.register
def _(e: exceptions.RayNotRunningError) -> ResponseStatusCode:
    click.echo(
        "Could not find any running Ray instance. "
        "You can use 'orq status' to check the status of the ray service. "
        "If it is not running, it can be started with the `orq up` command."
    )
    return ResponseStatusCode.CONNECTION_ERROR


@pretty_print_exception.register
def _(e: ConnectionError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(f"{e}")
    return ResponseStatusCode.CONNECTION_ERROR


@pretty_print_exception.register
def _(e: exceptions.LoginURLUnavailableError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(
        f"The login URL for '{e.base_uri}' is unavailable. "
        "Try checking your network connection and the cluster URL.",
    )
    return ResponseStatusCode.CONNECTION_ERROR


@pretty_print_exception.register
def _(_: exceptions.UserCancelledPrompt) -> ResponseStatusCode:
    return ResponseStatusCode.USER_CANCELLED


@pretty_print_exception.register
def _(_: exceptions.InProcessFromCLIError) -> ResponseStatusCode:
    click.echo(
        (
            'The "{0}" runtime is designed for debugging and testing '
            "via the Python API only. The results and workflow states are not "
            "persisted.\n\nYou may want to:\n"
            ' - Use the Python API to debug workflows with the "{0}" runtime.\n'
            ' - Try the "{1}" runtime if you want to run a workflow locally via'
            " the CLI."
        ).format(IN_PROCESS_CONFIG_NAME, RAY_CONFIG_NAME_ALIAS),
    )
    return ResponseStatusCode.USER_CANCELLED


@pretty_print_exception.register
def _(e: exceptions.ConfigNameNotFoundError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(e.message)
    return ResponseStatusCode.NOT_FOUND


@pretty_print_exception.register
def _(e: exceptions.NoOptionsAvailableError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(f"{e.message}:\nNo options are available.")
    return ResponseStatusCode.NOT_FOUND


@pretty_print_exception.register
def _(e: exceptions.LocalConfigLoginError) -> ResponseStatusCode:
    click.echo(e.message)
    return ResponseStatusCode.INVALID_CLI_COMMAND_ERROR


@pretty_print_exception.register
def _(e: exceptions.InvalidTokenError) -> ResponseStatusCode:
    click.echo("The auth token is not valid.\n" "Please try logging in again.")
    return ResponseStatusCode.UNAUTHORIZED


@pretty_print_exception.register
def _(e: exceptions.ExpiredTokenError) -> ResponseStatusCode:
    click.echo("The auth token has expired.\n" "Please try logging in again.")
    return ResponseStatusCode.UNAUTHORIZED


@pretty_print_exception.register
def _(e: exceptions.WorkflowRunNotStarted) -> ResponseStatusCode:
    click.echo(e)
    return ResponseStatusCode.INVALID_WORKFLOW_RUN


@pretty_print_exception.register
def _(e: exceptions.QERemoved) -> ResponseStatusCode:
    click.echo(e)
    return ResponseStatusCode.CONNECTION_ERROR


@pretty_print_exception.register
def _(e: exceptions.RuntimeQuerySummaryError) -> ResponseStatusCode:
    first_string = (
        "Couldn't find a config that knows about workflow run ID "
        f"[bold]{e.wf_run_id}[/bold]"
    )
    summary_table = Table(
        Column("Config name", style="bold", justify="right"),
        Column("Reason"),
        box=SIMPLE_HEAVY,
    )

    for config in e.not_found_runtimes:
        if config.config_name is not None:
            summary_table.add_row(config.config_name, "Workflow run not found")

    for config in e.unauthorized_runtimes:
        if config.config_name is not None:
            summary_table.add_row(
                config.config_name, "Authorization error " "- Login expired"
            )

    for config in e.not_running_runtimes:
        if config.config_name is not None:
            summary_table.add_row(config.config_name, "Not running")

    last_string = (
        "Please make sure that the workflow ID is correct, "
        "and you're logged in to the correct cluster.\n"
    )

    Console().print(first_string, summary_table, last_string)

    return ResponseStatusCode.WORKFLOW_RUN_NOT_FOUND
