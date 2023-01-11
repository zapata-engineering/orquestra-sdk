################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################

"""REST client for Quantum Engine API."""

import typing as t
from urllib.parse import urljoin

import requests

from orquestra.sdk.exceptions import NotFoundError


def _handle_http_error(response: requests.Response):
    """Raise an exception on errors"""
    response.raise_for_status()


API_ACTION = {
    "get_version": "/version",
    "submit_workflow": "/v1/workflows",
    "get_workflow": "/v1/workflow",
    "get_workflow_result": "/v2/workflows/{}/result",
    "get_log": "/v1/logs/{}",
    "list_workflow": "/v1/workflowlist",
    "stop_workflow": "/v1/workflows/{}",
    "get_login_url": "v1/login",
    "get_artifact": "/v2/workflows/{}/step/{}/artifact/{}",
    # Endpoints from OG `orq`:
    # https://github.com/zapatacomputing/orquestra-runtime/blob/7b8d1562d7b6afe17c6dd9979bc1c04f23dfc89b/src/python/orquestra/runtime/remote/_remote.py#L31-L42  # noqa: E501
}


class QEClient:
    """Thin layer on top of Quantum Engine API. Ideally, it should use the same
    abstractions as the REST API and should contain no additional logic.
    """

    def __init__(self, session: requests.Session, base_uri: str):
        self._base_uri = base_uri
        self._session = session

    def _get(
        self,
        endpoint: str,
        allowed_error_codes: t.Optional[t.List[int]] = None,
        params: t.Optional[t.Dict[str, str]] = None,
        str_payload: t.Optional[str] = None,
        json_payload: t.Optional[t.Dict[str, str]] = None,
        timeout: t.Optional[float] = None,
        allow_redirects: bool = True,
    ) -> requests.Response:
        """Helper method for GET requests"""
        response = self._session.get(
            urljoin(self._base_uri, endpoint),
            params=params,
            json=json_payload,
            data=str_payload,
            timeout=timeout,
            allow_redirects=allow_redirects,
        )

        if not response.ok and response.status_code not in (allowed_error_codes or []):
            _handle_http_error(response)
        return response

    def _post(
        self,
        endpoint: str,
        allowed_error_codes: t.Optional[t.List[int]] = None,
        params: t.Optional[t.Dict[str, str]] = None,
        str_payload: t.Optional[str] = None,
        json_payload: t.Optional[t.Dict[str, str]] = None,
    ) -> requests.Response:
        """Helper method for POST requests"""
        response = self._session.post(
            urljoin(self._base_uri, endpoint),
            params=params,
            json=json_payload,
            data=str_payload,
        )

        if not response.ok and response.status_code not in (allowed_error_codes or []):
            _handle_http_error(response)
        return response

    def _delete(
        self,
        endpoint: str,
        allowed_error_codes: t.Optional[t.List[int]] = None,
        params: t.Optional[t.Dict[str, str]] = None,
        str_payload: t.Optional[str] = None,
        json_payload: t.Optional[t.Dict[str, str]] = None,
    ) -> requests.Response:
        """Helper method for DELETE requests"""
        response = self._session.delete(
            urljoin(self._base_uri, endpoint),
            params=params,
            json=json_payload,
            data=str_payload,
        )
        if not response.ok and response.status_code not in (allowed_error_codes or []):
            _handle_http_error(response)
        return response

    def get_version(self) -> t.Dict:
        response = self._get(API_ACTION["get_version"])
        return response.json()

    def get_login_url(self) -> str:
        """First step in the auth flow. Fetches the URL that the user has to visit.

        Raises:
            requests.ConnectionError: if the request fails.
            KeyError: if the URL couldn't be found in the response.
        """
        resp = self._get(
            API_ACTION["get_login_url"],
            params={"state": "0"},
            timeout=10.0,
            allow_redirects=False,
        )
        return resp.headers["Location"]

    def submit_workflow(self, workflow: str) -> str:
        """
        Args:
            workflow: content of the QE workflow yaml
        Returns:
            workflow run ID
        """
        response = self._post(
            API_ACTION["submit_workflow"],
            str_payload=workflow,
        )
        # .json() wouldn't work because the API returns the wf ID as plain text.
        return response.text

    def get_workflow(self, wf_id: str):
        response = self._get(
            API_ACTION["get_workflow"],
            params={"workflowid": wf_id},
        )

        return response.json()

    def get_workflow_result(self, wf_id: str) -> bytes:

        response = self._get(
            API_ACTION["get_workflow_result"].format(wf_id),
            allowed_error_codes=[404],
        )
        if response.status_code == requests.codes.not_found:
            json_response = response.json()
            raise NotFoundError(json_response["message"])

        content_type = response.headers.get("Content-Type")
        if (
            content_type is None
            or "application/x-gtar-compressed" not in content_type.split(";")
        ):
            # Not a gzipped response
            raise ValueError(
                "Workflow result is the incorrect format: "
                f"{content_type or '[Missing Content-Type header]'}"
            )

        return response.content

    def get_log(self, wf_id: str, step_name: str) -> t.Dict:
        """
        Queries QE to get logs for a single QE workflow step. Note that this
        doesn't raise an error when `step_name` is invalid; rather the returned
        logs are empty.

        Returns: a dictionary with the following entries:
            - "logs": the value is a list of log lines as strings

        Raises:
            requests.exceptions.HTTPError: 400 client error when QE couldn't
                find a workflow matching the `wf_id`.
        """
        response = self._get(
            API_ACTION["get_log"].format(wf_id),
            params={"stepname": step_name},
        )
        return response.json()

    def get_workflow_list(self):
        # For future filtering of workflow runs
        # "detail": True lets get_all_workflow_runs_status use the same code as
        # get_workflow_run_status for parsing the workflow representation returned
        # from QE
        params: t.Dict[str, t.Any] = {"detail": "true"}

        response = self._get(API_ACTION["list_workflow"], params=params)
        return response.json()

    def stop_workflow(self, wf_id):
        """
        Raises:
            requests.exceptions.RequestException
        """
        _ = self._delete(API_ACTION["stop_workflow"].format(wf_id))

    def get_artifact(self, workflow_id, step_name, artifact_name):
        raw_response = self._get(
            API_ACTION["get_artifact"].format(workflow_id, step_name, artifact_name)
        )
        return raw_response.content
