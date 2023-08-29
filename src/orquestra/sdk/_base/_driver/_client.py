################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""
Code for accessing the Workflow Driver API.

Implemented API spec:
    https://github.com/zapatacomputing/workflow-driver/tree/2b3534/openapi
"""

import io
import re
import zlib
from tarfile import TarFile
from typing import Generic, List, Mapping, Optional, Tuple, TypeVar, Union
from urllib.parse import urljoin

import pydantic
import requests
from requests import codes

from orquestra.sdk import ProjectRef
from orquestra.sdk._base._spaces._api import make_workspace_zri
from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.responses import ComputeEngineWorkflowResult, WorkflowResult
from orquestra.sdk.schema.workflow_run import (
    WorkflowRun,
    WorkflowRunMinimal,
    WorkspaceId,
)

from .._regex import VERSION_REGEX
from . import _exceptions, _models


def _match_unsupported_version(error_detail: str):
    # We try to match format of the error message to parse the supported and
    # submitted versions.
    # If we fail, we return None and carry on.
    matches = re.match(
        rf"Unsupported SDK version: (?P<requested>{VERSION_REGEX})?\. "
        rf"Supported SDK versions: \[(?P<available>({VERSION_REGEX}[,]?)+)?\]",
        error_detail,
    )
    if matches is None:
        return None, None

    group_matches = matches.groupdict()
    requested = group_matches.get("requested")
    available_match = group_matches.get("available")
    available = available_match.split(",") if available_match is not None else None

    return requested, available


class ExternalUriProvider:
    API_ACTIONS = {
        # Workflow Definitions
        "create_workflow_def": "/api/workflow-definitions",
        "list_workflow_defs": "/api/workflow-definitions",
        "get_workflow_def": "/api/workflow-definitions/{}",
        "delete_workflow_def": "/api/workflow-definitions/{}",
        # Workflow Runs
        "create_workflow_run": "/api/workflow-runs",
        "list_workflow_runs": "/api/workflow-runs",
        "get_workflow_run": "/api/workflow-runs/{}",
        "terminate_workflow_run": "/api/workflow-runs/{}/terminate",
        # Artifacts
        "get_workflow_run_artifacts": "/api/artifacts",
        "get_artifact": "/api/artifacts/{}",
        # Run results
        "get_workflow_run_results": "/api/run-results",
        "get_workflow_run_result": "/api/run-results/{}",
        # Logs
        "get_workflow_run_logs": "/api/workflow-run-logs",
        "get_task_run_logs": "/api/task-run-logs",
        "get_workflow_run_system_logs": "/api/workflow-run-logs/system",
        # Login
        "get_login_url": "/api/login",
        # Workspaces
        "list_workspaces": "/api/catalog/workspaces",
        "list_projects": "/api/catalog/workspaces/{}/projects",
    }

    def __init__(self, base_uri):
        self._base_uri = base_uri

    def uri_for(
        self, action_id: str, parameters: Optional[Tuple[str, ...]] = None
    ) -> str:
        endpoint = ExternalUriProvider.API_ACTIONS[action_id]
        if parameters:
            endpoint = endpoint.format(*parameters)
        return urljoin(self._base_uri, endpoint)


def _handle_common_errors(response: requests.Response):
    if response.status_code == codes.UNAUTHORIZED:
        raise _exceptions.InvalidTokenError()
    elif response.status_code == codes.FORBIDDEN:
        raise _exceptions.ForbiddenError()
    elif not response.ok:
        raise _exceptions.UnknownHTTPError(response)


T = TypeVar("T")


class Paginated(Generic[T]):
    """
    Represents a paginated response.

    The contents of the current page can be accessed via ``contents``.

    ``prev_page_token`` and ``next_page_token`` can be used with the original method to
    return the previous and next page, respectively.
    """

    def __init__(
        self,
        contents: List[T],
        prev_page_token: Optional[str] = None,
        next_page_token: Optional[str] = None,
    ):
        self._contents = contents
        self._prev_token = prev_page_token
        self._next_token = next_page_token

    def __repr__(self):
        return (
            f"Paginated(contents={self._contents}, "
            f"prev_token={self._prev_token}, next_token={self._next_token})"
        )

    @property
    def contents(self) -> List[T]:
        """The contents property."""
        return self._contents

    @property
    def prev_page_token(self) -> Optional[str]:
        """The prev_token property."""
        return self._prev_token

    @property
    def next_page_token(self) -> Optional[str]:
        """The next_token property."""
        return self._next_token


class DriverClient:
    """
    Client for interacting with the Workflow Driver API via HTTP.
    """

    def __init__(self, session: requests.Session, uri_provider: ExternalUriProvider):
        self._uri_provider = uri_provider
        self._session = session

    @classmethod
    def from_token(cls, token: str, uri_provider: ExternalUriProvider):
        """
        Args:
            token: Auth token taken from logging in.
            uri_provider: Class that provides URIS for http requests
        """
        session = requests.Session()
        session.headers["Content-Type"] = "application/json"
        session.headers["Authorization"] = f"Bearer {token}"
        return cls(session=session, uri_provider=uri_provider)

    # --- helpers ---

    def _get(
        self,
        uri: str,
        query_params: Optional[Mapping],
        allow_redirects: bool = True,
    ) -> requests.Response:
        """Helper method for GET requests"""
        response = self._session.get(
            uri,
            params=query_params,
            allow_redirects=allow_redirects,
        )

        return response

    def _post(
        self,
        uri: str,
        body_params: Optional[Mapping],
        query_params: Optional[Mapping] = None,
    ) -> requests.Response:
        """Helper method for POST requests"""
        response = self._session.post(
            uri,
            json=body_params,
            params=query_params,
        )
        return response

    def _delete(self, uri: str) -> requests.Response:
        """Helper method for DELETE requests"""
        response = self._session.delete(uri)

        return response

    # --- queries ---

    # ---- Worklow Defs ----

    def create_workflow_def(
        self,
        workflow_def: WorkflowDef,
        project: Optional[ProjectRef],
    ) -> _models.WorkflowDefID:
        """
        Stores a workflow definition for future submission

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowDef
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError

        Returns:
            the WorkflowDefID associated with the stored definition
        """
        query_params = (
            _models.CreateWorkflowDefsRequest(
                workspaceId=project.workspace_id,
                projectId=project.project_id,
            ).dict()
            if project
            else None
        )
        resp = self._post(
            self._uri_provider.uri_for("create_workflow_def"),
            body_params=workflow_def.dict(),
            query_params=query_params,
        )

        if resp.status_code == codes.BAD_REQUEST:
            error = _models.Error.parse_obj(resp.json())
            raise _exceptions.InvalidWorkflowDef(
                message=error.message, detail=error.detail
            )

        _handle_common_errors(resp)

        return (
            _models.Response[_models.CreateWorkflowDefResponse, _models.MetaEmpty]
            .parse_obj(resp.json())
            .data.id
        )

    def list_workflow_defs(
        self, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Paginated[WorkflowDef]:
        """
        Lists all known workflow definitions

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """
        resp = self._get(
            self._uri_provider.uri_for("list_workflow_defs"),
            query_params=_models.ListWorkflowDefsRequest(
                pageSize=page_size,
                pageToken=page_token,
            ).dict(),
        )

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.ListWorkflowDefsResponse, _models.Pagination
        ].parse_obj(resp.json())
        contents = [d.workflow for d in parsed_response.data]
        if parsed_response.meta is not None:
            next_token = parsed_response.meta.nextPageToken
        else:
            next_token = None

        return Paginated(
            contents=contents,
            next_page_token=next_token,
        )

    def get_login_url(self, redirect_port: int) -> str:
        """First step in the auth flow. Fetches the URL that the user has to visit.

        Raises:
            requests.ConnectionError: if the request fails.
            KeyError: if the URL couldn't be found in the response.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_login_url"),
            query_params={"port": f"{redirect_port}"},
            allow_redirects=False,
        )
        _handle_common_errors(resp)
        return resp.headers["Location"]

    def get_workflow_def(
        self, workflow_def_id: _models.WorkflowDefID
    ) -> _models.GetWorkflowDefResponse:
        """
        Gets a stored workflow definition

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowDefID
            orquestra.sdk._base._driver._exceptions.WorkflowDefNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError

        Returns:
            a parsed WorkflowDef
        """
        resp = self._get(
            self._uri_provider.uri_for(
                "get_workflow_def", parameters=(workflow_def_id,)
            ),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowDefID(workflow_def_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowDefNotFound(workflow_def_id)

        _handle_common_errors(resp)

        parsed_resp = _models.Response[
            _models.GetWorkflowDefResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        return parsed_resp.data

    def delete_workflow_def(self, workflow_def_id: _models.WorkflowDefID):
        """
        Gets a stored workflow definition

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowDefID
            orquestra.sdk._base._driver._exceptions.WorkflowDefNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """
        resp = self._delete(
            self._uri_provider.uri_for(
                "delete_workflow_def", parameters=(workflow_def_id,)
            ),
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowDefID(workflow_def_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowDefNotFound(workflow_def_id)

        _handle_common_errors(resp)

    # ---- Workflow Runs ----

    def create_workflow_run(
        self,
        workflow_def_id: _models.WorkflowDefID,
        resources: _models.Resources,
        dry_run: bool,
    ) -> _models.WorkflowRunID:
        """
        Submit a workflow def to run in the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunRequest
            orquestra.sdk._base._driver._exceptions.UnsupportedSDKVersion
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """
        resp = self._post(
            self._uri_provider.uri_for("create_workflow_run"),
            body_params=_models.CreateWorkflowRunRequest(
                workflowDefinitionID=workflow_def_id,
                resources=resources,
                dryRun=dry_run,
            ).dict(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            error = _models.Error.parse_obj(resp.json())
            if error.code == _models.ErrorCode.SDK_VERSION_UNSUPPORTED:
                requested, available = _match_unsupported_version(error.detail)
                raise _exceptions.UnsupportedSDKVersion(requested, available)
            raise _exceptions.InvalidWorkflowRunRequest(
                message=error.message, detail=error.detail
            )

        _handle_common_errors(resp)

        return (
            _models.Response[_models.CreateWorkflowRunResponse, _models.MetaEmpty]
            .parse_obj(resp.json())
            .data.id
        )

    def list_workflow_runs(
        self,
        workflow_def_id: Optional[_models.WorkflowDefID] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        workspace: Optional[WorkspaceId] = None,
    ) -> Paginated[WorkflowRunMinimal]:
        """
        List workflow runs with a specified workflow def ID from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """
        # Schema: https://github.com/zapatacomputing/workflow-driver/blob/fa3eb17f1132d9c7f4960331ffe7ddbd31e02f8c/openapi/src/resources/workflow-runs.yaml#L10 # noqa: E501
        resp = self._get(
            self._uri_provider.uri_for("list_workflow_runs"),
            query_params=_models.ListWorkflowRunsRequest(
                workflowDefinitionID=workflow_def_id,
                pageSize=page_size,
                pageToken=page_token,
                workspaceId=workspace,
            ).dict(),
        )

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.ListWorkflowRunsResponse, _models.Pagination
        ].parse_obj(resp.json())

        if parsed_response.meta is not None:
            next_token = parsed_response.meta.nextPageToken
        else:
            next_token = None

        workflow_runs = []
        for r in parsed_response.data:
            workflow_def = self.get_workflow_def(r.definitionId)
            workflow_runs.append(r.to_ir(workflow_def.workflow))

        return Paginated(
            contents=workflow_runs,
            next_page_token=next_token,
        )

    def get_workflow_run(self, wf_run_id: _models.WorkflowRunID) -> WorkflowRun:
        """
        Gets the status of a workflow run from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run", parameters=(wf_run_id,)),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.WorkflowRunResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        workflow_def = self.get_workflow_def(parsed_response.data.definitionId)

        return parsed_response.data.to_ir(workflow_def.workflow)

    def terminate_workflow_run(
        self, wf_run_id: _models.WorkflowRunID, force: Optional[bool] = None
    ):
        """
        Asks the workflow driver to terminate a workflow run

        Args:
            wf_run_id: the workflow to terminate
            force: if the workflow should be forcefully terminated

        Raises:
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._post(
            self._uri_provider.uri_for(
                "terminate_workflow_run", parameters=(wf_run_id,)
            ),
            body_params=None,
            query_params=_models.TerminateWorkflowRunRequest(force=force).dict(),
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

    # --- Workflow Run Artifacts ---

    def get_workflow_run_artifacts(
        self, wf_run_id: _models.WorkflowRunID
    ) -> Mapping[_models.TaskRunID, List[_models.WorkflowRunArtifactID]]:
        """
        Gets the workflow run artifact IDs of a workflow run from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for(
                "get_workflow_run_artifacts",
            ),
            query_params=_models.GetWorkflowRunArtifactsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.GetWorkflowRunArtifactsResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        return parsed_response.data

    def get_workflow_run_artifact(
        self, artifact_id: _models.WorkflowRunArtifactID
    ) -> WorkflowResult:
        """
        Gets workflow run artifacts from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunArtifactID
            orquestra.sdk._base._driver._exceptions.WorkflowRunArtifactNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for("get_artifact", parameters=(artifact_id,)),
            query_params=None,
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunArtifactNotFound(artifact_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunArtifactID(artifact_id)

        _handle_common_errors(resp)

        # Bug with mypy and Pydantic:
        #   Unions cannot be passed to parse_obj_as: pydantic/pydantic#1847
        return pydantic.parse_obj_as(
            WorkflowResult, resp.json()  # type: ignore[arg-type]
        )

    # --- Workflow Run Results ---

    def get_workflow_run_results(
        self, wf_run_id: _models.WorkflowRunID
    ) -> List[_models.WorkflowRunResultID]:
        """
        Gets the workflow run result IDs of a workflow run from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run_results"),
            query_params=_models.GetWorkflowRunResultsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.GetWorkflowRunResultsResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        return parsed_response.data

    def get_workflow_run_result(
        self, result_id: _models.WorkflowRunResultID
    ) -> Union[WorkflowResult, ComputeEngineWorkflowResult]:
        """
        Gets workflow run results from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunResultID
            orquestra.sdk._base._driver._exceptions.WorkflowRunResultNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for(
                "get_workflow_run_result", parameters=(result_id,)
            ),
            query_params=None,
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunResultNotFound(result_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunResultID(result_id)

        _handle_common_errors(resp)

        # To ensure the correct ordering of results, we serialize the results on CE as:
        # {
        #   "results": [
        #       (JSONResult | PickleResult).json(),
        #       (JSONResult | PickleResult).json(),
        #       ...
        #   ]
        # } aka a ComputeEngineWorkflowResult.json()
        # For older workflows, we respond with:
        # (JSONResult | PickleResult).json()

        json_response = resp.json()
        try:
            # Try an older response
            # Bug with mypy and Pydantic:
            #   Unions cannot be passed to parse_obj_as: pydantic/pydantic#1847
            return pydantic.parse_obj_as(
                WorkflowResult, json_response  # type: ignore[arg-type]
            )
        except pydantic.ValidationError:
            # If we fail, try parsing each part of a list separately
            return ComputeEngineWorkflowResult.parse_obj(json_response)

    # --- Workflow Logs ---
    def get_workflow_run_logs(
        self, wf_run_id: _models.WorkflowRunID
    ) -> List[_models.WorkflowLogMessage]:
        """
        Gets the logs of a workflow run from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotReadable
        """

        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run_logs"),
            query_params=_models.GetWorkflowRunLogsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        # Handle errors
        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunLogsNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        _handle_common_errors(resp)

        # Decompress data
        try:
            unzipped: bytes = zlib.decompress(resp.content, 16)
        except zlib.error as e:
            raise _exceptions.WorkflowRunLogsNotReadable(wf_run_id, None) from e

        untarred = TarFile(fileobj=io.BytesIO(unzipped)).extractfile("step-logs")
        assert untarred is not None
        decoded = untarred.read().decode()

        # Parse the decoded data as logs
        messages = []
        for section_str in decoded.split("\n"):
            if len(section_str) < 1:
                continue

            events = pydantic.parse_raw_as(_models.WorkflowLogSection, section_str)

            for event in events:
                messages.append(event.message)

        return messages

    def get_task_run_logs(
        self,
        wf_run_id: _models.WorkflowRunID,
        task_inv_id: _models.TaskInvocationID,
    ) -> List[_models.TaskLogMessage]:
        """
        Gets the logs of a task run from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.TaskRunLogsNotFound
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotReadable
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for("get_task_run_logs"),
            query_params=_models.GetTaskRunLogsRequest(
                workflowRunId=wf_run_id, taskInvocationId=task_inv_id
            ).dict(),
        )

        # Handle errors
        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.TaskRunLogsNotFound(wf_run_id, task_inv_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        _handle_common_errors(resp)

        # Decompress data
        try:
            unzipped: bytes = zlib.decompress(resp.content, 16)
        except zlib.error as e:
            raise _exceptions.WorkflowRunLogsNotReadable(wf_run_id, task_inv_id) from e

        untarred = TarFile(fileobj=io.BytesIO(unzipped)).extractfile("step-logs")
        assert untarred is not None
        decoded = untarred.read().decode()

        # Parse the decoded data as logs
        messages = []
        for section_str in decoded.split("\n"):
            if len(section_str) < 1:
                continue

            events = pydantic.parse_raw_as(_models.TaskLogSection, section_str)

            for event in events:
                messages.append(event.message)

        return messages

    def get_system_logs(self, wf_run_id: _models.WorkflowRunID) -> List[_models.SysLog]:
        """
        Get the logs of a workflow run from the workflow driver.

        Raises:
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotFound
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotReadable
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
            NotImplementedError: when a log object's source_type is not a recognised
                value, or is a value for a schema has not been defined.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run_system_logs"),
            query_params=_models.GetWorkflowRunLogsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        # Handle errors
        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunLogsNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        _handle_common_errors(resp)

        # Decompress data
        try:
            unzipped: bytes = zlib.decompress(resp.content, 16)
        except zlib.error as e:
            raise _exceptions.WorkflowRunLogsNotReadable(
                wf_run_id, task_invocation_id=None
            ) from e

        untarred = TarFile(fileobj=io.BytesIO(unzipped)).extractfile("step-logs")
        assert untarred is not None
        decoded = untarred.read().decode()

        messages = []
        for section_str in decoded.split("\n"):
            if len(section_str) < 1:
                continue
            events = pydantic.parse_raw_as(_models.SysSection, section_str)

            for event in events:
                messages.append(event.message)

        return messages

    def list_workspaces(self):
        """
        Gets the list of all workspaces
        """

        resp = self._get(
            self._uri_provider.uri_for("list_workspaces"),
            query_params=None,
        )

        _handle_common_errors(resp)

        parsed_response = pydantic.parse_obj_as(
            _models.ListWorkspacesResponse, resp.json()
        )

        return parsed_response

    def list_projects(self, workspace_id):
        """
        Gets the list of all projects in given workspace
        """

        workspace_zri = make_workspace_zri(workspace_id)

        resp = self._get(
            self._uri_provider.uri_for("list_projects", parameters=(workspace_zri,)),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkspaceZRI(workspace_zri)

        _handle_common_errors(resp)

        parsed_response = pydantic.parse_obj_as(
            _models.ListProjectResponse, resp.json()
        )

        return parsed_response

    def get_workflow_project(self, wf_run_id: _models.WorkflowRunID) -> ProjectRef:
        """
        Gets the status of a workflow run from the workflow driver

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound
            orquestra.sdk._base._driver._exceptions.InvalidTokenError
            orquestra.sdk._base._driver._exceptions.ForbiddenError
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError
        """

        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run", parameters=(wf_run_id,)),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.WorkflowRunResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        workflow_def = self.get_workflow_def(parsed_response.data.definitionId)

        return ProjectRef(
            workspace_id=workflow_def.workspaceId, project_id=workflow_def.project
        )
