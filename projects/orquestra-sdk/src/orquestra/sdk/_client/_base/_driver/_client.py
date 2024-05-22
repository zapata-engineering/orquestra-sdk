################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Code for accessing the Workflow Driver API.

Implemented API spec:
    https://github.com/zapatacomputing/workflow-driver/tree/2b3534/openapi
"""

import io
import re
import zlib
from datetime import timedelta
from tarfile import TarFile
from typing import Generic, List, Mapping, Optional, Tuple, TypeVar, Union, cast
from urllib.parse import urljoin

import pydantic
import requests
from orquestra.workflow_shared import VERSION_REGEX, ProjectRef, exceptions
from orquestra.workflow_shared.orqdantic import TypeAdapter
from orquestra.workflow_shared.schema.ir import WorkflowDef
from orquestra.workflow_shared.schema.responses import (
    ComputeEngineWorkflowResult,
    WorkflowResult,
)
from orquestra.workflow_shared.schema.workflow_run import (
    State,
    WorkflowRun,
    WorkflowRunMinimal,
    WorkflowRunSummary,
    WorkspaceId,
)
from requests import codes

from .._spaces._api import make_workspace_zri
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


def _get_state_query(state: Optional[Union[State, List[State]]]) -> Optional[str]:
    """Construct the state query from the required states.

    Uses the following heuristic:
    - state is None - no state filtering to be done, returns None
    - single state arg - return the state value.
    - multiple states - return a comma-separated list of state values.

    Args:
        state: The required state filters.

    Returns:
        str: The constructed query that can be passed as the state parameter to
            ListWorkflowRunsRequest.
    """
    if state is None:
        return None
    if isinstance(state, list):
        return ",".join([st.value for st in state])
    return state.value


T = TypeVar("T")


class Paginated(Generic[T]):
    """Represents a paginated response.

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
    """Client for interacting with the Workflow Driver API via HTTP."""

    def __init__(self, session: requests.Session, uri_provider: ExternalUriProvider):
        self._uri_provider = uri_provider
        self._session = session

    @classmethod
    def from_token(
        cls, token: str, uri_provider: ExternalUriProvider
    ) -> "DriverClient":
        """Create a driver client from a token and URI.

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
        """Helper method for GET requests."""
        try:
            response = self._session.get(
                uri,
                params=query_params,
                allow_redirects=allow_redirects,
            )
        except requests.exceptions.ConnectionError as e:
            raise exceptions.RemoteConnectionError(uri) from e

        return response

    def _post(
        self,
        uri: str,
        body_params: Optional[Mapping],
        query_params: Optional[Mapping] = None,
    ) -> requests.Response:
        """Helper method for POST requests."""
        try:
            response = self._session.post(
                uri,
                json=body_params,
                params=query_params,
            )
        except requests.exceptions.ConnectionError as e:
            raise exceptions.RemoteConnectionError(uri) from e

        return response

    def _delete(self, uri: str) -> requests.Response:
        """Helper method for DELETE requests."""
        try:
            response = self._session.delete(uri)
        except requests.exceptions.ConnectionError as e:
            raise exceptions.RemoteConnectionError(uri) from e

        return response

    # --- queries ---

    # ---- Workflow Defs ----

    def create_workflow_def(
        self,
        workflow_def: WorkflowDef,
        project: Optional[ProjectRef],
    ) -> _models.WorkflowDefID:
        """Stores a workflow definition for future submission.

        Args:
            workflow_def: The workflow definition to be stored.
            project: the projectref under which to store the workflow def.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowDef: when an invalid
                Workflow Definition is sent to the Workflow Driver.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.

        Returns:
            the WorkflowDefID associated with the stored definition
        """
        query_params = (
            _models.CreateWorkflowDefsRequest(
                workspaceId=project.workspace_id,
                projectId=project.project_id,
            ).model_dump()
            if project
            else None
        )
        resp = self._post(
            self._uri_provider.uri_for("create_workflow_def"),
            body_params=workflow_def.model_dump(),
            query_params=query_params,
        )

        if resp.status_code == codes.BAD_REQUEST:
            error = _models.Error.model_validate(resp.json())
            raise _exceptions.InvalidWorkflowDef(
                message=error.message, detail=error.detail
            )

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        return (
            _models.Response[_models.CreateWorkflowDefResponse, _models.MetaEmpty]
            .model_validate(resp.json())
            .data.id
        )

    def list_workflow_defs(
        self, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Paginated[WorkflowDef]:
        """Lists all known workflow definitions.

        Args:
            page_size: Maximum number of results returned in a single page.
            page_token: Token for the requested page of the list results.
                If omitted the first page will be returned.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                list workflow runs from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("list_workflow_defs"),
            query_params=_models.ListWorkflowDefsRequest(
                pageSize=page_size,
                pageToken=page_token,
            ).model_dump(),
        )

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.ListWorkflowDefsResponse, _models.Pagination
        ].model_validate(resp.json())
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

        Args:
            redirect_port: The localhost port (http://localhost:<port>) to which the
                browser will redirect after a successful login.

        Raises:
            requests.ConnectionError: when the request fails.
            KeyError: when the URL couldn't be found in the response.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_login_url"),
            query_params={"port": f"{redirect_port}"},
            allow_redirects=False,
        )

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        return resp.headers["Location"]

    def get_workflow_def(
        self, workflow_def_id: _models.WorkflowDefID
    ) -> _models.GetWorkflowDefResponse:
        """Gets a stored workflow definition.

        Args:
            workflow_def_id: Id of the stored workflow definition to be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowDefID: when the
                workflow def ID is incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowDefNotFound: when no
                 workflow with the specified ID is found.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.

        Returns:
            The parsed WorkflowDef.
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

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_resp = _models.Response[
            _models.GetWorkflowDefResponse, _models.MetaEmpty
        ].model_validate(resp.json())

        return parsed_resp.data

    def delete_workflow_def(self, workflow_def_id: _models.WorkflowDefID):
        """Gets a stored workflow definition.

        Args:
            workflow_def_id: the ID of the workflow definition to be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowDefID: when the
                workflow def ID is incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowDefNotFound: when no
                 workflow with the specified ID is found.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
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

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

    # ---- Workflow Runs ----

    def create_workflow_run(
        self,
        workflow_def_id: _models.WorkflowDefID,
        resources: _models.Resources,
        dry_run: bool,
        head_node_resources: Optional[_models.HeadNodeResources],
    ) -> _models.WorkflowRunID:
        """Submit a workflow def to run in the workflow driver.

        Args:
            workflow_def_id: ID of the workflow definition to be submitted.
            resources: The resources required to execute the workflow.
            dry_run: Run the workflow without actually executing any task code.
            head_node_resources: the requested resources for the head node

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunRequest: when an
                invalid Workflow Run request is sent to the Workflow Driver.
            orquestra.sdk._base._driver._exceptions.UnsupportedSDKVersion: when an
                unsupported Workflow SDK version is used to submit a workflow run.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._post(
            self._uri_provider.uri_for("create_workflow_run"),
            body_params=_models.CreateWorkflowRunRequest(
                workflowDefinitionID=workflow_def_id,
                resources=resources,
                dryRun=dry_run,
                headNodeResources=head_node_resources,
            ).model_dump(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            error = _models.Error.model_validate(resp.json())
            if error.code == _models.ErrorCode.SDK_VERSION_UNSUPPORTED:
                requested, available = _match_unsupported_version(error.detail)
                raise _exceptions.UnsupportedSDKVersion(requested, available)
            raise _exceptions.InvalidWorkflowRunRequest(
                message=error.message, detail=error.detail
            )

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        return (
            _models.Response[_models.CreateWorkflowRunResponse, _models.MetaEmpty]
            .model_validate(resp.json())
            .data.id
        )

    def list_workflow_runs(
        self,
        workflow_def_id: Optional[_models.WorkflowDefID] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        workspace: Optional[WorkspaceId] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
    ) -> Paginated[WorkflowRunMinimal]:
        """List workflow runs with a specified workflow def ID from the workflow driver.

        Args:
            workflow_def_id: Only list workflow runs matching this workflow def. If
                omitted, workflow runs with any workflow definition will be returned.
            page_size: Maximum number of results returned in a single page.
            page_token: Token for the requested page of the list results.
                If omitted the first page will be returned.
            workspace: Only list workflow runs in the specified workspace. If omitted,
                workflow runs from all workspaces will be returned.
            max_age: Only list workflows younger than the specified age.
            state: Only list workflows in the specified state(s).

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                list workflow runs from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        try:
            resp = self._list_workflow_runs(
                workflow_def_id,
                page_size,
                page_token,
                workspace,
                max_age,
                state,
            )
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.ListWorkflowRunsResponse, _models.Pagination
        ].model_validate(resp.json())

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

    def list_workflow_run_summaries(
        self,
        workflow_def_id: Optional[_models.WorkflowDefID] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        workspace: Optional[WorkspaceId] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
    ) -> Paginated[WorkflowRunSummary]:
        """List workflow runs summaries with a specified workflow def ID.

        Args:
            workflow_def_id: Only list workflow runs matching this workflow def. If
                omitted, workflow runs with any workflow definition will be returned.
            page_size: Maximum number of results returned in a single page.
            page_token: Token for the requested page of the list results.
                If omitted the first page will be returned.
            workspace: Only list workflow runs in the specified workspace. If omitted,
                workflow runs from all workspaces will be returned.
            max_age: Only list workflows younger than the specified age.
            state: Only list workflows in the specified state(s).

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                list workflow runs from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        try:
            resp = self._list_workflow_runs(
                workflow_def_id,
                page_size,
                page_token,
                workspace,
                max_age,
                state,
            )
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.ListWorkflowRunSummariesResponse, _models.Pagination
        ].model_validate(resp.json())

        if parsed_response.meta is not None:
            next_token = parsed_response.meta.nextPageToken
        else:
            next_token = None

        workflow_runs = []
        for r in parsed_response.data:
            workflow_runs.append(r.to_ir())

        return Paginated(
            contents=workflow_runs,
            next_page_token=next_token,
        )

    def _list_workflow_runs(
        self,
        workflow_def_id: Optional[_models.WorkflowDefID] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        workspace: Optional[WorkspaceId] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
    ):
        """Get a list of wf runs from the workflow driver.

        The responses will be parsed using the specified response model.

        This method consolidates the logic used by both list_workflow_runs and
        list_workflow_run_summaries.

        Args:
            workflow_def_id: Only list workflow runs matching this workflow def. If
                omitted, workflow runs with any workflow definition will be returned.
            page_size: Maximum number of results returned in a single page.
            page_token: Token for the requested page of the list results.
                If omitted the first page will be returned.
            workspace: Only list workflow runs in the specified workspace. If omitted,
                workflow runs from all workspaces will be returned.
            max_age: Only list workflows younger than the specified age.
            state: Only list workflows in the specified state(s).


        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                list workflow runs from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        # Schema: https://github.com/zapatacomputing/workflow-driver/blob/fa3eb17f1132d9c7f4960331ffe7ddbd31e02f8c/openapi/src/resources/workflow-runs.yaml#L10 # noqa: E501

        resp = self._get(
            self._uri_provider.uri_for("list_workflow_runs"),
            query_params=_models.ListWorkflowRunsRequest(
                workflowDefinitionID=workflow_def_id,
                pageSize=page_size,
                pageToken=page_token,
                workspaceId=workspace,
                projectId=None,
                maxAge=(int(max_age.total_seconds()) if max_age else None),
                state=_get_state_query(state),
            ).model_dump(),
        )

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        return resp

    def get_workflow_run(self, wf_run_id: _models.WorkflowRunID) -> WorkflowRun:
        """Gets the status of a workflow run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow run to be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound: when the
                workflow run cannot be found
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run", parameters=(wf_run_id,)),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.WorkflowRunResponse, _models.MetaEmpty
        ].model_validate(resp.json())

        workflow_def = self.get_workflow_def(parsed_response.data.definitionId)

        return parsed_response.data.to_ir(workflow_def.workflow)

    def terminate_workflow_run(
        self, wf_run_id: _models.WorkflowRunID, force: Optional[bool] = None
    ):
        """Asks the workflow driver to terminate a workflow run.

        Args:
            wf_run_id: the workflow to terminate
            force: if the workflow should be forcefully terminated

        Raises:
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound: when the
                workflow run cannot be found
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                terminate a workflow run from a workspace that does not belong to the
                user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._post(
            self._uri_provider.uri_for(
                "terminate_workflow_run", parameters=(wf_run_id,)
            ),
            body_params=None,
            query_params=_models.TerminateWorkflowRunRequest(force=force).model_dump(),
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

    # --- Workflow Run Artifacts ---

    def get_workflow_run_artifacts(
        self, wf_run_id: _models.WorkflowRunID
    ) -> Mapping[_models.TaskRunID, List[_models.WorkflowRunArtifactID]]:
        """Gets the artifact IDs of a workflow run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow run whose artifacts we want.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound: when the
                workflow run cannot be found
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for(
                "get_workflow_run_artifacts",
            ),
            query_params=_models.GetWorkflowRunArtifactsRequest(
                workflowRunId=wf_run_id
            ).model_dump(),
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.GetWorkflowRunArtifactsResponse, _models.MetaEmpty
        ].model_validate(resp.json())

        return parsed_response.data

    def get_workflow_run_artifact(
        self, artifact_id: _models.WorkflowRunArtifactID
    ) -> WorkflowResult:
        """Gets workflow run artifacts from the workflow driver.

        Args:
            artifact_id: ID of the artifact to be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunArtifactID: when
                the artifact_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunArtifactNotFound: when
                the artifact cannot be found.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_artifact", parameters=(artifact_id,)),
            query_params=None,
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunArtifactNotFound(artifact_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunArtifactID(artifact_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        return cast(
            WorkflowResult,
            TypeAdapter(WorkflowResult).validate_python(resp.json()),
        )

    # --- Workflow Run Results ---

    def get_workflow_run_results(
        self, wf_run_id: _models.WorkflowRunID
    ) -> List[_models.WorkflowRunResultID]:
        """Gets the workflow run result IDs of a workflow run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow run whose results we want.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound: when the
                workflow run cannot be found
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run_results"),
            query_params=_models.GetWorkflowRunResultsRequest(
                workflowRunId=wf_run_id
            ).model_dump(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.GetWorkflowRunResultsResponse, _models.MetaEmpty
        ].model_validate(resp.json())

        return parsed_response.data

    def get_workflow_run_result(
        self, result_id: _models.WorkflowRunResultID
    ) -> Union[WorkflowResult, ComputeEngineWorkflowResult]:
        """Gets workflow run results from the workflow driver.

        Args:
            result_id: ID of the result to be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunResultID: when the
                result_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunResultNotFound: when the
                workflow run result cannot be found
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
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

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        # To ensure the correct ordering of results, we serialize the results on CE as:
        # {
        #   "results": [
        #       (JSONResult | PickleResult).model_dump_json(),
        #       (JSONResult | PickleResult).model_dump_json(),
        #       ...
        #   ]
        # } aka a ComputeEngineWorkflowResult.model_dump_json()
        # For older workflows, we respond with:
        # (JSONResult | PickleResult).model_dump_json()

        json_response = resp.json()
        try:
            # Try an older response
            return cast(
                WorkflowResult,
                TypeAdapter(WorkflowResult).validate_python(json_response),
            )

        except pydantic.ValidationError:
            # If we fail, try parsing each part of a list separately
            return ComputeEngineWorkflowResult.model_validate(json_response)

    # --- Workflow Logs ---
    def get_workflow_run_logs(
        self, wf_run_id: _models.WorkflowRunID
    ) -> List[_models.WorkflowLogMessage]:
        """Gets the logs of a workflow run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow run whose logs we want.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound: when the
                workflow run cannot be found
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotReadable: when
                the logs exist, but cannot be decoded.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run_logs"),
            query_params=_models.GetWorkflowRunLogsRequest(
                workflowRunId=wf_run_id
            ).model_dump(),
        )

        # Handle errors
        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunLogsNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

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

            events = TypeAdapter(_models.WorkflowLogSection).validate_json(section_str)

            for event in events:
                messages.append(event.message)

        return messages

    def get_task_run_logs(
        self,
        wf_run_id: _models.WorkflowRunID,
        task_inv_id: _models.TaskInvocationID,
    ) -> List[_models.TaskLogMessage]:
        """Gets the logs of a task run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow run containing the task.
            task_inv_id: ID of the task invocation, the logs of which should be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.TaskRunLogsNotFound: when the
                task run logs cannot be found.
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotReadable: when
                the logs exist, but cannot be decoded.
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_task_run_logs"),
            query_params=_models.GetTaskRunLogsRequest(
                workflowRunId=wf_run_id, taskInvocationId=task_inv_id
            ).model_dump(),
        )

        # Handle errors
        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.TaskRunLogsNotFound(wf_run_id, task_inv_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

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

            events = TypeAdapter(_models.TaskLogSection).validate_json(section_str)

            for event in events:
                messages.append(event.message)

        return messages

    def get_system_logs(self, wf_run_id: _models.WorkflowRunID) -> List[_models.SysLog]:
        """Get the system-level logs of a workflow run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow runs the logs of which should be loaded.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotFound: when the
                workflow run logs cannot be found.
            orquestra.sdk._base._driver._exceptions.WorkflowRunLogsNotReadable: when
                the logs exist, but cannot be decoded.
            NotImplementedError: when a log object's source_type is not a recognised
                value, or is a value for a schema has not been defined.
        """
        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run_system_logs"),
            query_params=_models.GetWorkflowRunLogsRequest(
                workflowRunId=wf_run_id
            ).model_dump(),
        )

        # Handle errors
        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunLogsNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

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

            events = TypeAdapter(_models.SysSection).validate_json(section_str)

            for event in events:
                messages.append(event.message)

        return messages

    def list_workspaces(self):
        """Gets the list of all workspaces.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        resp = self._get(
            self._uri_provider.uri_for("list_workspaces"),
            query_params=None,
        )

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = TypeAdapter(_models.ListWorkspacesResponse).validate_python(
            resp.json()
        )

        return parsed_response

    def list_projects(self, workspace_id: WorkspaceId):
        """Gets the list of all projects in given workspace.

        Args:
            workspace_id: ID of the target workspace.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
        """
        workspace_zri = make_workspace_zri(workspace_id)

        resp = self._get(
            self._uri_provider.uri_for("list_projects", parameters=(workspace_zri,)),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkspaceZRI(workspace_zri)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = TypeAdapter(_models.ListProjectResponse).validate_python(
            resp.json()
        )

        return parsed_response

    def get_workflow_project(self, wf_run_id: _models.WorkflowRunID) -> ProjectRef:
        """Gets the status of a workflow run from the workflow driver.

        Args:
            wf_run_id: ID of the workflow run.

        Raises:
            orquestra.sdk._base._driver._exceptions.InvalidTokenError: when the
                authorization token is rejected by the remote cluster.
            orquestra.sdk._base._driver._exceptions.ForbiddenError: when attempting to
                access a workflow run from a workspace that does not belong to the user.
            orquestra.sdk._base._driver._exceptions.UnknownHTTPError: when any other
                error is raised by the remote cluster.
            orquestra.sdk._base._driver._exceptions.InvalidWorkflowRunID: when the
                wf_run_id is empty or incorrectly formatted.
            orquestra.sdk._base._driver._exceptions.WorkflowRunNotFound: when the
                workflow run cannot be found
        """
        resp = self._get(
            self._uri_provider.uri_for("get_workflow_run", parameters=(wf_run_id,)),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        try:
            _handle_common_errors(resp)
        except (
            _exceptions.InvalidTokenError,
            _exceptions.ForbiddenError,
            _exceptions.UnknownHTTPError,
        ):
            raise

        parsed_response = _models.Response[
            _models.WorkflowRunResponse, _models.MetaEmpty
        ].model_validate(resp.json())

        workflow_def = self.get_workflow_def(parsed_response.data.definitionId)

        return ProjectRef(
            workspace_id=workflow_def.workspaceId, project_id=workflow_def.project
        )
