################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Exception types related to the Workflow Driver API.
"""
import requests


class InvalidTokenError(Exception):
    """
    Raised when the communication with the external Config Service couldn't be made
    because of an invalid token.
    """


class InvalidWorkflowDef(Exception):
    """
    Raised when an invalid Workflow Definition is sent to the Workflow Driver
    """

    def __init__(self, message: str, detail: str):
        self.message = message
        self.detail = detail
        super().__init__(message, detail)


class InvalidWorkflowRunRequest(Exception):
    """
    Raised when an invalid Workflow Run request is sent to the Workflow Driver
    """

    def __init__(self, message: str, detail: str):
        self.message = message
        self.detail = detail
        super().__init__(message, detail)


class InvalidWorkflowDefID(Exception):
    """
    Raised when an invalid Workflow Definition ID is sent to the Workflow Driver

    Workflow Definition IDs are expected to be UUIDs, if an ID is sent that cannot be
    parsed as a UUID, this exception will be raised.
    """

    def __init__(self, workflow_def_id: str):
        self.workflow_def_id = workflow_def_id
        super().__init__(workflow_def_id)


class InvalidWorkflowRunID(Exception):
    """
    Raised when an invalid Workflow Run ID is sent to the Workflow Driver

    Workflow Run IDs are expected to be UUIDs, if an ID is sent that cannot be
    parsed as a UUID, this exception will be raised.
    """

    def __init__(self, workflow_run_id: str):
        self.workflow_run_id = workflow_run_id
        super().__init__(workflow_run_id)


class UnknownHTTPError(Exception):
    """
    Raised when there's an error we don't handle otherwise.
    """

    def __init__(self, response: requests.Response):
        self.response = response
        super().__init__(response)


class WorkflowDefNotFound(Exception):
    """
    Raised when a Workflow Definition cannot be found
    """

    def __init__(self, workflow_def_id: str):
        self.workflow_def_id = workflow_def_id
        super().__init__(workflow_def_id)


class WorkflowRunNotFound(Exception):
    """
    Raised when a Workflow Run cannot be found
    """

    def __init__(self, workflow_run_id: str):
        self.workflow_run_id = workflow_run_id
        super().__init__(workflow_run_id)
