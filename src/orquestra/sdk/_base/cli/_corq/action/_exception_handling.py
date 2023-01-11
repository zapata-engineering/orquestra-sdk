################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import argparse
from functools import singledispatch

import orquestra.sdk._base._config as _config
from orquestra.sdk import exceptions
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    ResponseMetadata,
    ResponseStatusCode,
)


def handle_action_exception(
    exception: Exception, args: argparse.Namespace, traceback: str
) -> ErrorResponse:
    """Wrapper for the singledispatch _handle_exception function that calls
    _construct_message_parameters and passes that dict as an argument to
    _handle_exception rather than the args and traceback.

    We do this here rather than in the specific functions to avoid repeating code.

    Args:
        exception: The exception to be handled.
        args: The parsed CLI arguments (used to provide context to exceptions)
        traceback: The traceback of the exception

    Returns:
        ErrorResponse
    """
    message_parameters = _construct_message_parameters(args, traceback)
    return _handle_exception(exception, message_parameters)


def _construct_message_parameters(args: argparse.Namespace, traceback: str) -> dict:
    """Construct a dict of all of the parameters we use in return object messages.

    Where a suitable parameter does not exists, we instead insert a placeholder string
    so that trying to create messages does not cause an AttributeError.

    Args:
        args: The parsed CLI arguments (used to provide context to exceptions)
        traceback: The traceback of the exception

    Returns:
        dict: a dict of strings containing the following keys:
        - BUILT_IN_CONFIG_NAME
        - config_name
        - directory
        - project_name
        - traceback
    """
    message_parameters: dict = {}

    message_parameters["traceback"] = traceback
    message_parameters["BUILT_IN_CONFIG_NAME"] = _config.BUILT_IN_CONFIG_NAME

    try:
        message_parameters["config_name"] = args.config
    except AttributeError:
        message_parameters["config_name"] = "<ERROR: No config name set.>"

    try:
        message_parameters["run_id"] = args.workflow_run_id
    except AttributeError:
        message_parameters["run_id"] = "<ERROR: No run_id set.>"

    try:
        message_parameters["task_id"] = args.task_id
    except AttributeError:
        message_parameters["task_id"] = "<ERROR: No task_id set.>"

    try:
        message_parameters["directory"] = args.directory
    except AttributeError:
        message_parameters["directory"] = "<ERROR: no directory set>"

    try:
        message_parameters["project_name"] = args.project_name
    except AttributeError:
        message_parameters["project_name"] = "<ERROR: no project name set>"

    return message_parameters


@singledispatch
def _handle_exception(exception, message_parameters: dict) -> ErrorResponse:
    """Handle exceptions that aren't caught elsewhere.

    Args:
        exception: The exception that triggered this handler to be called
        args: The parsed CLI arguments (used to provide context to exceptions)
        traceback: The traceback of the exception (to be included in the response
        message)

    Returns:
        ErrorResponse
    """
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.UNKNOWN_ERROR,
            message=(
                f"Unknown error {str(exception)}\n"
                f"Additional info: {message_parameters['traceback']}"
            ),
        ),
    )


@_handle_exception.register
def _(exception: NotImplementedError, message_parameters: dict) -> ErrorResponse:
    """Handle exceptions relating to functions we haven't implemented yet."""

    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_FOUND,
            message=(
                "The function you have tried to access is not yet implemented.\n"
                f"Additional info: {str(exception)}"
            ),
        )
    )


@_handle_exception.register
def _(
    exception: exceptions.ConfigFileNotFoundError, message_parameters: dict
) -> ErrorResponse:
    """Handle exceptions relating to problems with config files."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_FOUND,
            message=(
                "No configuration file found.\n"
                f"Tried: {_config._get_config_directory() / _config.CONFIG_FILE_NAME}\n"
                f"Requested configuration: {message_parameters['config_name']}. "
                "Have you tried logging in with `orq login`? "
                f"To run locally, use the '{_config.BUILT_IN_CONFIG_NAME}' "
                "configuration."
                f"\nAdditional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register
def _(
    exception: exceptions.ConfigNameNotFoundError, message_parameters: dict
) -> ErrorResponse:
    """Handle exceptions relating to problems with config files."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_FOUND,
            message=(
                "No configuration with that name found.\n"
                f"Requested configuration: {message_parameters['config_name']}. "
                "Have you tried logging in with `orq login`? "
                "To run locally, use the "
                f"'{message_parameters['BUILT_IN_CONFIG_NAME']}' configuration."
                f"\nAdditional info: {str(exception)}"
            ),
        )
    )


@_handle_exception.register
def _(
    exception: exceptions.InvalidWorkflowDefinitionError, message_parameters: dict
) -> ErrorResponse:
    """Handle exceptions relating to problems with workflow definitions files."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_WORKFLOW_DEF,
            message=(
                "The workflow definition is invalid.\n"
                f"Additional info: {str(exception)}"
            ),
        )
    )


@_handle_exception.register
def _(
    exception: exceptions.WorkflowTooLargeError, message_parameters: dict
) -> ErrorResponse:
    """Handle exceptions relating to the workflow being too large."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_WORKFLOW_DEF,
            message=(
                "The workflow is too large to be executed.\n"
                "Large numbers of task invocations and pickled constants can often "
                "result in large workflows, consider modifying the workflow definition "
                "to reduce the numbers of these.\n"
                f"Additional info: {str(exception)}"
            ),
        )
    )


@_handle_exception.register
def _(
    exception: exceptions.WorkflowDefinitionSyntaxError, message_parameters: dict
) -> ErrorResponse:
    """Handle exceptions relating to problems with workflow definitions files."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_WORKFLOW_DEFS_SYNTAX,
            message=(
                "Syntax error in workflow definition.\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register
def _(
    exception: exceptions.InvalidTaskDefinitionError, message_parameters: dict
) -> ErrorResponse:
    """Handle exceptions relating to problems with workflow definitions files."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_TASK_DEF,
            message=("Invalid task definition.\n" f"Additional info: {str(exception)}"),
        ),
    )


@_handle_exception.register(exceptions.WorkflowNotFoundError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    """Handle exceptions relating to problems with workflows."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.WORKFLOW_RUN_NOT_FOUND,
            message=(
                f"Workflow run with ID {message_parameters['run_id']} not found.\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.InvalidWorkflowRunError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    """Handle exceptions relating to problems with workflows."""

    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_WORKFLOW_RUN,
            message=(
                f"Workflow run with ID {message_parameters['run_id']} exists, "
                "but is invalid.\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.WorkflowRunNotSucceeded)
def _(exception, message_parameters: dict) -> ErrorResponse:
    """Handle exceptions relating to problems with workflows."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.WORKFLOW_RUN_NOT_FOUND,
            message=(
                f"Workflow {message_parameters['run_id']} has not succeeded. "
                f"Status: {exception.state}.\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.WorkflowRunNotFinished)
def _(exception, message_parameters: dict) -> ErrorResponse:
    """Handle exceptions relating to problems with workflows."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.WORKFLOW_RUN_NOT_FOUND,
            message=(
                f"Workflow {message_parameters['run_id']} has not finished. "
                f"Status: {exception.state}.\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(PermissionError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.PERMISSION_ERROR,
            message=(
                f"Unable to access {exception.filename}\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(FileNotFoundError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_FOUND,
            message=(
                f'File "{exception.filename}" not found.\n'
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(ModuleNotFoundError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_FOUND,
            message=(
                f'Module "{exception.name}" not found.\n'
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.UnauthorizedError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.UNAUTHORIZED,
            message=(
                "Authorization failed. Please log in again.\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(
    exceptions.MalformedProjectError,
)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_PROJECT,
            message=(
                "A project file was found, but it is malformed. "
                f"Check {message_parameters['directory']}\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(NotADirectoryError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_A_DIRECTORY,
            message=(
                "Please remove the .orquestra file in "
                f"{message_parameters['directory']}\n"
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.InvalidProjectError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_PROJECT,
            message=(
                "Not an Orquestra project directory. "
                f'(Directory: "{message_parameters["directory"]}")\n'
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.PreexistingProjectError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.PROJECT_EXISTS,
            message=(
                f'Project "{message_parameters["project_name"]}" already exists in '
                f'"{message_parameters["directory"]}"\n'
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(ConnectionError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.CONNECTION_ERROR,
            message=(
                f'Unable to connect to Ray: "{exception}"\n'
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.RayActorNameClashError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.CONNECTION_ERROR,
            message=(
                f'Ray Actor name clash: "{exception}"\n'
                f"Additional info: {str(exception)}"
            ),
        ),
    )


@_handle_exception.register(exceptions.InvalidTaskIDError)
def _(exception, message_parameters: dict) -> ErrorResponse:
    """Handle exception relating to invalid task invocation ID."""
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.NOT_FOUND,
            message=(
                f"Workflow {message_parameters['run_id']} does not contain task "
                f"invocation with ID: {message_parameters['task_id'] }"
            ),
        ),
    )


@_handle_exception.register(exceptions.DirtyGitRepoError)
def _(exception: exceptions.DirtyGitRepoError, _: dict) -> ErrorResponse:
    return ErrorResponse(
        meta=ResponseMetadata(
            success=False,
            code=ResponseStatusCode.INVALID_TASK_DEF,
            message=(
                f"{exception}\n"
                "Commit and push your changes and try submitting again.\n"
                "If you still want to submit this workflow, try again with --force.\n"
                "On remote runtimes, --force will use the last pushed changes to the "
                "remote Git repository.\n"
                "For example: orq submit workflow-def --force"
            ),
        ),
    )
