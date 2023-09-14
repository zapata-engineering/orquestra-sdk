################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""Custom exceptions for the SDK."""

import typing as t

from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import State, TaskInvocationId, WorkflowRunId


class WorkflowSyntaxError(Exception):
    """Raised when there is a syntax error in the workflow definition."""

    def __init__(self, msg: str):
        super().__init__(msg)
        self.msg = msg


class VersionMismatch(Warning):
    def __init__(self, msg: str, actual: ir.Version, needed: t.Optional[ir.Version]):
        super().__init__(msg)
        self.actual = actual
        self.needed = needed


class DirtyGitRepo(Warning):
    """Raised when there are uncommitted changes in the git repo."""

    pass


class BaseRuntimeError(Exception):
    """Base class for runtime errors."""

    def __init__(self, message: t.Optional[str] = None):
        super().__init__(message)
        self.message = message


# Generic
class NotFoundError(BaseRuntimeError):
    """Raised when the specified runtime, workflow, result, or artifact is not found."""

    pass


class UserTaskFailedError(BaseRuntimeError):
    """Raised when a task run fails during execution.

    The actual exception that stopped the task from execution is chained as
    ``raise TaskRunFailedError(...) from e``. This is a workaround for
    de/serialization of exceptions defined in 3rd-party libraries.
    """

    def __init__(
        self,
        msg: str,
        wf_run_id: WorkflowRunId = "",
        task_inv_id: TaskInvocationId = "",
    ):
        super().__init__(msg)
        self.wf_run_id = wf_run_id
        self.task_inv_id = task_inv_id


# Config Errors
class ConfigFileNotFoundError(BaseRuntimeError):
    """Raised when the configuration file cannot be identified."""

    pass


class ConfigNameNotFoundError(BaseRuntimeError):
    """Raised if the specified config is not stored in the config file."""

    pass


class RuntimeConfigError(BaseRuntimeError):
    """Raised when one or more configuration options do not relate to the runtime."""

    pass


class LocalConfigLoginError(BaseRuntimeError):
    """Raised when trying to log in using a config that relates to local execution."""

    pass


class QERemoved(BaseRuntimeError):
    """Raised when attempting to use QE."""


# Workflow Definition Errors
class WorkflowDefinitionModuleNotFound(NotFoundError):
    """Raised when loading workflow definitions module failed."""

    def __init__(self, module_name: str, sys_path: t.Sequence[str]):
        self.module_name = module_name
        # 'sys.path' is often mutated. Let's guard against this action-at-a-distance.
        self.sys_path = list(sys_path)


class NoWorkflowDefinitionsFound(NotFoundError):
    """Raised when there's no workflow definitions in the specified module."""

    def __init__(self, module_name: str):
        self.module_name = module_name


class InvalidWorkflowDefinitionError(BaseRuntimeError):
    """Raised when a workflow definition is invalid.

    This may be due to errors in the definition, or failure to meet runtime
    requirements.
    """

    pass


# Task Definition Errors
class InvalidTaskDefinitionError(BaseRuntimeError):
    """Raised when a task definition is invalid."""

    pass


class NodesInTaskResourcesWarning(Warning):
    """Raised when a "nodes" resource is passed to a Task.

    Nodes currently only apply to workflows and this option will be ignored.
    """


# Workflow Errors
class InvalidWorkflowRunLogsError(BaseRuntimeError):
    """Raised when workflow logs cannot be decoded."""

    pass


class TaskRunLogsNotFound(NotFoundError):
    """Raised when a task run logs cannot be found, or the ID is invalid."""

    def __init__(self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
        self.wf_run_id = wf_run_id
        self.task_inv_id = task_inv_id
        super().__init__()


class WorkflowRunNotSucceeded(BaseRuntimeError):
    """Raised when a succeeded workflow is required but it is found in another state."""

    def __init__(self, message: str, state: State):
        super().__init__(message)
        self.state = state


class WorkflowRunNotFinished(BaseRuntimeError):
    """Raised when a finished workflow is required but an unfinished one is found."""

    def __init__(self, message: str, state: State):
        super().__init__(message)
        self.state = state


class WorkflowRunCanNotBeTerminated(BaseRuntimeError):
    """Raised when an attempt to terminate a workflow run fails."""

    pass


class WorkflowRunNotFoundError(NotFoundError):
    """Raised when no run with the specified ID is found."""

    pass


class WorkflowRunNotStarted(WorkflowRunNotFoundError):
    """Raised when a started workflow is required but an unstarted one is found."""

    pass


class TaskRunNotFound(NotFoundError):
    """Raised when a task hasn't completed yet, or the ID is invalid."""

    pass


class TaskInvocationNotFoundError(NotFoundError):
    """Raised when we can't find a Task Invocation that matches the provided ID."""

    def __init__(self, invocation_id: ir.TaskInvocationId):
        super().__init__()
        self.invocation_id = invocation_id


class WorkflowResultsNotReadyError(NotFoundError):
    """Raised when a workflow has succeeded, but the results are not ready yet."""


class WorkflowRunIDNotFoundError(NotFoundError):
    """Raised when we can't recover the ID for this Workflow Run."""

    # Note that this is different to the WorkflowRunNotFoundError.
    # We have an ID but can't find the workflow: WorkflowRunNotFoundError
    # We have a workflow but can't recover the ID: WorkflowRunIDNotFoundError


# Auth Errors
class UnauthorizedError(BaseRuntimeError):
    """Raised when the remote cluster rejects the auth token."""

    pass


class ExpiredTokenError(BaseRuntimeError):
    """Raised when the auth token is expired."""

    pass


class InvalidTokenError(BaseRuntimeError):
    """Raised when an auth token is not a JWT."""

    pass


# Ray Errors
class RayActorNameClashError(BaseRuntimeError):
    """Raised when multiple Ray actors exist with the same name."""

    pass


class RayNotRunningError(ConnectionError):
    """Raised when there isn't a running ray instance."""

    pass


# CLI Exceptions
class UserCancelledPrompt(BaseRuntimeError):
    """Raised when the user cancels a CLI prompt."""

    pass


class LoginURLUnavailableError(BaseRuntimeError):
    """Raised when the login URL for is unavailable."""

    def __init__(self, base_uri: str):
        self.base_uri = base_uri


class NoOptionsAvailableError(NotFoundError):
    """Raised when the user would choose options, but no options are available."""

    pass


class InProcessFromCLIError(NotFoundError):
    """Raised when the user requests the in-process runtime when using the CLI."""


# Unsupported features
class UnsupportedRuntimeFeature(Warning):
    """Raised when a requested feature is not supported on the selected runtime."""

    pass


class ProjectInvalidError(BaseRuntimeError):
    """When there is insufficient information provided to identify a unique project."""

    pass


class WorkspacesNotSupportedError(BaseRuntimeError):
    """When a non-workspaces supporting runtime gets a workspaces-related request."""

    pass
