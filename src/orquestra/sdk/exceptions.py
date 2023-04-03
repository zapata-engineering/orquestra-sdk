################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import typing as t

from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import State


class WorkflowSyntaxError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
        self.msg = msg


class DirtyGitRepo(Warning):
    pass


class BaseRuntimeError(Exception):
    def __init__(self, message: t.Optional[str] = None):
        super().__init__(message)
        self.message = message


# Generic
class NotFoundError(BaseRuntimeError):
    pass


# Config Errors
class ConfigFileNotFoundError(BaseRuntimeError):
    pass


class ConfigNameNotFoundError(BaseRuntimeError):
    pass


class RuntimeConfigError(BaseRuntimeError):
    pass


class UnsavedConfigChangesError(BaseRuntimeError):
    pass


# Workflow Definition Errors
class WorkflowDefinitionModuleNotFound(NotFoundError):
    """
    Raised when loading workflow definitions module failed.
    """

    def __init__(self, module_name: str, sys_path: t.Sequence[str]):
        self.module_name = module_name
        # 'sys.path' is often mutated. Let's guard against this action-at-a-distance.
        self.sys_path = list(sys_path)


class NoWorkflowDefinitionsFound(NotFoundError):
    """
    Raised when there's no workflow definitions in the specified module.
    """

    def __init__(self, module_name: str):
        self.module_name = module_name


class InvalidWorkflowDefinitionError(BaseRuntimeError):
    pass


class WorkflowDefinitionSyntaxError(BaseRuntimeError):
    pass


class WorkflowTooLargeError(BaseRuntimeError):
    pass


# Task Definition Errors
class InvalidTaskDefinitionError(BaseRuntimeError):
    pass


class NodesInTaskResourcesWarning(Warning):
    """
    Raised when a "nodes" resource is passed to a Task.

    Nodes currently only apply to workflows and this option will be ignored.
    """


# Workflow Errors
class WorkflowNotFoundError(BaseRuntimeError):
    pass


class InvalidWorkflowRunLogsError(BaseRuntimeError):
    pass


class WorkflowRunNotSucceeded(BaseRuntimeError):
    def __init__(self, message: str, state: State):
        super().__init__(message)
        self.state = state


class WorkflowRunNotFinished(BaseRuntimeError):
    def __init__(self, message: str, state: State):
        super().__init__(message)
        self.state = state


class WorkflowRunCanNotBeTerminated(BaseRuntimeError):
    pass


class WorkflowRunNotFoundError(NotFoundError):
    pass


class WorkflowRunNotStarted(WorkflowRunNotFoundError):
    pass


class TaskRunNotFound(NotFoundError):
    pass


class TaskInvocationNotFoundError(NotFoundError):
    """
    Raised when we can't find a Task Invocation that matches the provided ID.
    """

    def __init__(self, invocation_id: TaskInvocationId):
        super().__init__()
        self.invocation_id = invocation_id


# Auth Errors
class UnauthorizedError(BaseRuntimeError):
    pass


# Ray Errors
class RayActorNameClashError(BaseRuntimeError):
    pass


# CLI Exceptions
class UserCancelledPrompt(BaseRuntimeError):
    pass


class LoginURLUnavailableError(BaseRuntimeError):
    def __init__(self, base_uri: str):
        self.base_uri = base_uri


class InProcessFromCLIError(NotFoundError):
    """Raised when the user requests the in-process runtime when using the CLI"""
