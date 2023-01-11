################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import sys
import traceback
from functools import singledispatch

import click

from orquestra.sdk import exceptions
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
def _print_no_wf_module_found(
    e: exceptions.WorkflowDefinitionModuleNotFound,
) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(
        f"Error: couldn't find workflow definitions module '{e.module_name}'. "
        f"Searched at {e.sys_path}"
    )
    return ResponseStatusCode.INVALID_WORKFLOW_DEF


@pretty_print_exception.register
def _print_no_wf_defs_found(
    e: exceptions.NoWorkflowDefinitionsFound,
) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(f"Error: couldn't find any workflow definitions in '{e.module_name}'")
    return ResponseStatusCode.INVALID_WORKFLOW_DEF


@pretty_print_exception.register
def _print_unauthorized(e: exceptions.UnauthorizedError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo("Error: Authorization failed. Please log in again.")
    return ResponseStatusCode.UNAUTHORIZED


@pretty_print_exception.register
def _print_syntax_error(e: exceptions.WorkflowSyntaxError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo(f"Invalid workflow syntax. {e.msg}")
    return ResponseStatusCode.INVALID_WORKFLOW_DEFS_SYNTAX


@pretty_print_exception.register
def _print_connection_error(e: ConnectionError) -> ResponseStatusCode:
    _print_traceback(e)
    click.echo("Unable to connect to Ray.")
    return ResponseStatusCode.CONNECTION_ERROR
