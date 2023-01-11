#!/usr/bin/env python
################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Orquestra CLI entry point. Command line script `orq.sh` invokes actions from this file.

CLI provides a way to interact with the Quantum Engine Server, or local runtime.
 * submit workflows
 * get workflow details and results
 * download artifacts
 * get logs
"""

import argparse
import datetime
import logging
import os
import re
import sys
import traceback
import typing as t

import argcomplete  # type: ignore

from orquestra.sdk._base._config import (
    CONFIG_FILE_NAME,
    RUNTIME_OPTION_NAMES,
    _get_config_file_path,
)
from orquestra.sdk.schema.configs import RuntimeName
from orquestra.sdk.schema.responses import ErrorResponse, ResponseFormat
from orquestra.sdk.schema.workflow_run import State

from . import action, services
from .action import _exception_handling

logging.basicConfig(level=logging.INFO)


def _get_parent_parsers() -> t.Dict[str, argparse.ArgumentParser]:
    format_parser = argparse.ArgumentParser(add_help=False)
    format_parser.add_argument(
        "-o",
        "--output-format",
        type=ResponseFormat,
        help=(
            "The output format for the response. "
            f'Defaults to "{ResponseFormat.DEFAULT.value}".'
        ),
        default=ResponseFormat.DEFAULT,
    )

    directory_parser = argparse.ArgumentParser(add_help=False)
    directory_parser.add_argument(
        "-d",
        "--directory",
        help="The directory to of the project. "
        "Defaults to the current working directory",
        default=os.getcwd(),
    )

    workflow_run_id_parser = argparse.ArgumentParser(add_help=False)
    workflow_run_id_parser.add_argument(
        "workflow_run_id",
        help="The ID of workflow run.",
    )

    runtime_configuration_parser = argparse.ArgumentParser(add_help=False)
    runtime_configuration_parser.add_argument(
        "-c",
        "--config",
        help=(
            "Name of the runtime configuration to use. Runtime configurations are "
            "created using `orq login` and are kept under "
            f"~/.orquestra/{CONFIG_FILE_NAME}. If this arg is omitted, the "
            "default config will be used. See also `orq {get,set} default-config` "
        ),
    )

    return {
        "format": format_parser,
        "directory": directory_parser,
        "workflow_run_id": workflow_run_id_parser,
        "runtime_config": runtime_configuration_parser,
    }


def make_v2_parser():
    parents = _get_parent_parsers()

    parser = argparse.ArgumentParser(
        prog="orq",
        description=(
            "Controls Orquestra Workflow execution and allows to collect results."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        const=logging.DEBUG,
        default=logging.INFO,
        help="Verbosity level. If set, detailed information will be printed to stderr.",
    )
    subparsers = parser.add_subparsers()

    # --------------- level 1 prefixes --------------
    get_sub_parser = subparsers.add_parser("get").add_subparsers()
    submit_sub_parser = subparsers.add_parser("submit").add_subparsers()
    stop_sub_parser = subparsers.add_parser("stop").add_subparsers()
    set_sub_parser = subparsers.add_parser("set").add_subparsers()
    list_sub_parser = subparsers.add_parser("list").add_subparsers()
    services.add_subparsers(subparsers)

    # ----------------- v2 commands -----------------
    # orq login
    login_parser = subparsers.add_parser("login")
    login_parser.add_argument(
        "-s",
        "--server_uri",
        help="The Quantum Engine server URI to use",
        default=None,
    )
    login_parser.add_argument(
        "-r",
        "--runtime",
        help="Runtime to interact with specified server.",
        choices=[None, "QE_REMOTE"],
        default="QE_REMOTE",
    )
    login_parser.add_argument(
        "-c",
        "--config",
        help=(
            "Configuration name to which to store this login. "
            "If not set, the subdomain of the server uri will be used."
        ),
        default=None,
    )
    login_parser.add_argument(
        "-D",
        "--default-config",
        help="Log in using the current default configuration.",
        default=False,
        action="store_true",
    )
    login_parser.set_defaults(func=action.orq_login)

    # orq set token
    set_token_parser = set_sub_parser.add_parser("token", parents=[parents["format"]])
    set_token_parser.add_argument(
        "-s",
        "--server_uri",
        help="""
            The Quantum Engine server URI to use. Can be omitted if it was
            already saved for this config.
        """,
        default=None,
    )

    # Note: there's no '-r/--runtime' parameter because tokens only make sense
    # for QE_REMOTE.

    set_token_parser.add_argument(
        "-c",
        "--config",
        help="""
            Configuration name to store the token to. If omitted, uses the
            default config set by the last "orq login" or "orq set
            default-config.
            """,
        default=None,
    )

    set_token_parser.add_argument(
        "-t",
        "--token_file",
        help="""
            Path to the file with auth token. If equal to "-", orq will read
            the token from the standard input.
            """,
        required=True,
    )
    set_token_parser.set_defaults(func=action.orq_set_token)

    # orq submit workflow-def
    submit_workflow_def_parser = submit_sub_parser.add_parser(
        "workflow-def",
        parents=[
            parents["format"],
            parents["directory"],
            parents["runtime_config"],
        ],
    )
    submit_workflow_def_parser.add_argument(
        "--force",
        help="Use this to submit a workflow even if there are unpushed changes",
        default=False,
        action="store_true",
    )
    submit_workflow_def_parser.add_argument(
        "workflow_def_name",
        nargs="?",
        help=(
            "Name of the workflow function. Can be omitted if the project contains "
            "only a single workflow function. CLI fails if the name is omitted and the "  # noqa: E501
            "project contains multiple workflow defs."
        ),
        default=None,
    )
    submit_workflow_def_parser.set_defaults(func=action.orq_submit_workflow_def)

    # orq get workflow-def
    get_workflow_def_parser = get_sub_parser.add_parser(
        "workflow-def", parents=[parents["format"], parents["directory"]]
    )
    get_workflow_def_parser.set_defaults(func=action.orq_get_workflow_def)

    # orq get task-def
    get_task_def_parser = get_sub_parser.add_parser(
        "task-def", parents=[parents["format"], parents["directory"]]
    )
    get_task_def_parser.set_defaults(func=action.orq_get_task_def)

    # orq get workflow-run
    get_workflow_run_parser = get_sub_parser.add_parser(
        "workflow-run",
        parents=[
            parents["format"],
            parents["directory"],
            parents["runtime_config"],
        ],
    )
    get_workflow_run_parser.add_argument(
        "workflow_run_id",
        nargs="?",
        help="The ID of the workflow run. "
        "If omitted, all workflow runs will be returned.",
        default=None,
    )
    get_workflow_run_parser.set_defaults(func=action.orq_get_workflow_run)

    # orq get workflow-run-results
    get_workflow_run_results_parser = get_sub_parser.add_parser(
        "workflow-run-results",
        parents=[
            parents["workflow_run_id"],
            parents["format"],
            parents["directory"],
            parents["runtime_config"],
        ],
    )
    get_workflow_run_results_parser.set_defaults(
        func=action.orq_get_workflow_run_results
    )

    # orq get artifacts
    get_workflow_artifacts = get_sub_parser.add_parser(
        "artifacts",
        parents=[
            parents["workflow_run_id"],
            parents["directory"],
            parents["runtime_config"],
            parents["format"],
        ],
    )
    get_workflow_artifacts.set_defaults(func=action.orq_get_artifacts)
    get_workflow_artifacts.add_argument(
        "task_id", nargs="?", default=None, help="Task Invocation ID of a specific task"
    )

    # orq get logs V2 SDK
    get_logs_parser = get_sub_parser.add_parser(
        "logs",
        parents=[
            parents["format"],
            parents["directory"],
            parents["runtime_config"],
        ],
        description=(
            "Get logs from a workflow or a single task run inside a workflow."
        ),
    )
    get_logs_parser.set_defaults(func=action.orq_get_logs)
    get_logs_parser.add_argument(
        "workflow_or_task_run_id",
        help=(
            "The ID of workflow/task run. Can be omitted if using "
            f"{RuntimeName.RAY_LOCAL} with --follow flag."
        ),
        nargs="?",
    )
    get_logs_parser.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="Follow (stream) logs. Keeps running and writing upcoming logs to "
        f"STDOUT. Currently, only {RuntimeName.RAY_LOCAL} supports it.",
    )

    # orq set_default_config
    set_default_config_parser = set_sub_parser.add_parser(
        "default-config", parents=[parents["format"]]
    )
    set_default_config_parser.add_argument(
        "config_name",
        help="Default configuration name to be set: remote or local",
        default=None,
    )
    set_default_config_parser.set_defaults(func=action.orq_set_default_config)

    # orq get_default_config
    get_default_config_parser = get_sub_parser.add_parser(
        "default-config", parents=[parents["format"]]
    )
    get_default_config_parser.set_defaults(func=action.orq_get_default_config)

    # orq cancel workflow-run
    stop_workflow_run_parser = stop_sub_parser.add_parser(
        "workflow-run",
        parents=[
            parents["workflow_run_id"],
            parents["format"],
            parents["directory"],
            parents["runtime_config"],
        ],
    )
    stop_workflow_run_parser.set_defaults(func=action.orq_stop_workflow_run)

    # orq list worklfow-run
    list_workflow_runs_parser = list_sub_parser.add_parser(
        "workflow-runs",
        parents=[
            parents["format"],
            parents["directory"],
        ],
    )
    list_workflow_runs_parser.add_argument(
        "-l",
        "--limit",
        default=20,
        type=int,
        help=(
            "The maximum number of workflows to return. "
            "Example: `orq list workflow-runs -l 5` will return the 5 most recent "
            "workflow runs (or less, if fewer than 5 matching workflow runs exist). "
            "Defaults to 20. "
            "Workflows with no start time are listed last."
        ),
    )
    list_workflow_runs_parser.add_argument(
        "-p",
        "--prefix",
        type=str,
        help=(
            "Limit returned workflow runs to those whose ID starts with the specified "
            "prefix."
            "Example: `orq list workflow-runs -p hello` "
            "will return only workflow runs whose IDs begin with `hello`."
        ),
    )
    list_workflow_runs_parser.add_argument(
        "-a",
        "--max-age",
        type=lambda s: _parse_age_clarg(list_workflow_runs_parser, s),
        help=(
            "Limit returned workflow runs to those younger than the specified maximum "
            "age. "
            "Example: `orq list workflow-runs -a 2d` "
            "will return only workflow runs created within the last 2 days. "
            "Workflows with no start time are omitted."
        ),
    )
    list_workflow_runs_parser.add_argument(
        "-s",
        "--status",
        type=lambda s: _parse_state_clarg(list_workflow_runs_parser, s),
        help=(
            "Limit returned workflow runs to those whose status matches that specified."
            " Example: `orq list workflow-runs -s FAILED` "
            "will return only workflow runs whose status is `FAILED`."
        ),
    )
    list_workflow_runs_parser.add_argument(
        "-c",
        "--config",
        type=str,
        help=(
            "Limit returned workflow run IDs to runs utilising the specified runtime "
            "configuration name. "
            "Example: `orq list workflow-runs --config remote` "
            "will return only workflow runs that used the `remote` configuration. "
            "If omitted, default-config will be used. Use --all to return runs from "
            "all configs"
        ),
    )
    list_workflow_runs_parser.set_defaults(func=action.orq_list_workflow_runs)
    list_workflow_runs_parser.add_argument(
        "--additional-project-dirs",
        type=str,
        nargs="+",
        help=(
            "Additional project directories to be searched for matching workflows. "
            "If omitted, only workflow runs from the current project directory are "
            "searched."
        ),
        default=[],
    )
    list_workflow_runs_parser.add_argument(
        "--all",
        help="Use this to return runs from all configs available. This overrides -c "
        "option",
        default=False,
        action="store_true",
    )

    return parser


def _dictify_args(cli_args: argparse.Namespace) -> t.Mapping:
    return vars(cli_args)


def _print_response(
    response,
    output_format: t.Optional[ResponseFormat],
    directory: t.Optional[str],
):
    """
    Args:
        response: the Pydantic model to output
        output_format: None if the command didn't define this CLI arg.
        directory: None if the command didn't define this CLI arg.
    """
    # 1. If '-o json' was requested -> print the full response as JSON
    if output_format == ResponseFormat.JSON:
        print(response.json())
        return

    # 2. Regular, default behavior
    # Deferred import for CLI latency reasons.
    import orquestra.sdk._base.cli._corq._format.per_command

    orquestra.sdk._base.cli._corq._format.per_command.pretty_print_response(
        response, project_dir=directory
    )


def _parse_age_clarg(parser: argparse.ArgumentParser, age: str) -> datetime.timedelta:
    """Parse a string specifying an age into a timedelta object.
    If the string cannot be parsed, displays an informative error through the parser.

    Args:
        parser: the ArgumentParser handling this arg. Used to raise an appropriate error
        when required.
        age: the string to be parsed.

    Returns:
        datetime.timedelta: the age specified by the 'age' string, as a timedelta.
    """
    time_params = {}

    # time in format "{days}d{hours}h{minutes}m{seconds}s"

    # match one or more digits "\d+?" followed by a single character from the 'units'
    # list. Capture the digits in a group labelled 'name'
    time_capture_group = r"(?P<{name}>\d+?)[{units}]"

    re_string = ""
    name_units = (
        ("days", "dD"),
        ("hours", "hH"),
        ("minutes", "mM"),
        ("seconds", "sS"),
    )
    for name, units in name_units:
        # match each potential time unit capture group between 0 and 1 times.
        re_string += rf"({time_capture_group.format(name=name, units=units)})?"

    if parts := re.compile(re_string).fullmatch(age):
        for name, param in parts.groupdict().items():
            if param:
                time_params[name] = int(param)
        if len(time_params) > 0:
            return datetime.timedelta(**time_params)

    parser.error(
        'Time strings must be in the format "{days}d{hours}h{minutes}m{seconds}s". '
        "Accepted units are:\n"
        "- d or D: days\n"
        "- h or H: hours\n"
        "- m or M: minutes\n"
        "- s or S: seconds\n"
        "For example:\n"
        '- "9h32m" = 9 hours and 32 minutes,\n'
        '- "8H6S" = 8 hours and 6 seconds,\n'
        '- "10m" = 10 minutes,\n'
        '- "3D6h8M13s" = 3 days, 6 hours, 8 minutes and 13 seconds.'
    )


def _parse_state_clarg(parser: argparse.ArgumentParser, state_str: str) -> State:
    """Parse a string specifying a workflowrun state into a State object.
    If the string cannot be parsed, displays an informative error through the parser.

    Args:
        parser: the ArgumentParser handling this arg. Used to raise an appropriate error
        when required.
        state_str: the string to be parsed.

    Returns:
        State: the corresponding state object.
    """
    try:
        return State(state_str.upper())
    except ValueError:
        parser.error(
            f'"{state_str}" is not a valid status. Status must be one of:'
            + "".join([f"\n- {e.value}" for e in State])
            + "\n(matching is case-insensitive)"
        )


def v2_cli():
    parser = make_v2_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    try:
        fn = args.func
    except AttributeError:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # This gives us the minimal v2 response for type checking here
    response: t.Optional[ErrorResponse]

    try:
        response = fn(args)
    except KeyboardInterrupt:
        response = None
    except Exception as e:
        exc_logger: logging.Logger = logging.getLogger("CLI Exceptions")
        exc_logger.setLevel(args.verbose)
        trace = traceback.format_exc()
        exc_logger.debug(trace)
        response = _exception_handling.handle_action_exception(e, args, trace)

    if response is not None:
        # If we got here, we assume it's a v2 action and display the result
        cli_args_dict = _dictify_args(args)
        _print_response(
            response,
            output_format=cli_args_dict.get("output_format"),
            directory=cli_args_dict.get("directory"),
        )

        sys.exit(response.meta.code.value)


def main_cli():
    v2_cli()


if __name__ == "__main__":
    main_cli()
