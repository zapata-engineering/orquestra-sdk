################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Code for accessing the Config Service API.

API spec: https://github.com/zapatacomputing/config-service/tree/main/openapi/src
"""
import typing as t
from urllib.parse import urljoin

import requests
from requests import codes

from . import _exceptions
from ._models import (
    ListSecretsRequest,
    SecretDefinition,
    SecretName,
    SecretNameObj,
    SecretValue,
    SecretValueObj,
    WorkspaceId,
)

API_ACTIONS = {
    "list_secrets": "/api/config/secrets",
    "create_secret": "/api/config/secrets",
    "get_secret": "/api/config/secrets/{}",
    "update_secret": "/api/config/secrets/{}",
    "delete_secret": "/api/config/secrets/{}",
}


def _handle_common_errors(response: requests.Response):
    """Handle common errors that can arise from API calls.

    Args:
        response: the response from the API.

    Raises:
        orquestra.sdk.secrets._exceptions.InvalidTokenError: when the authorization
            token is rejected by the remote cluster.
        orquestra.sdk.secrets._exceptions.UnknownHTTPError: when any other error is
            raised by the remote cluster.
    """
    if response.status_code == codes.UNAUTHORIZED:
        raise _exceptions.InvalidTokenError()
    elif not response.ok:
        raise _exceptions.UnknownHTTPError(response)


class SecretsClient:
    """Client for interacting with the Config Service API via HTTP."""

    def __init__(self, base_uri: str, session: requests.Session):
        self._base_uri = base_uri
        self._session = session

    @classmethod
    def from_token(cls, base_uri: str, token: str) -> "SecretsClient":
        """Construct a secrets client from a token string.

        Args:
            base_uri: Orquestra cluster URI, like 'https://foobar.orquestra.io'.
            token: Auth token taken from logging in, or "the passport".
        """
        session = requests.Session()
        session.headers["Content-Type"] = "application/json"
        session.headers["Authorization"] = f"Bearer {token}"
        return cls(base_uri=base_uri, session=session)

    # --- helpers ---

    def _get(
        self, endpoint: str, query_params: t.Optional[t.Mapping] = None
    ) -> requests.Response:
        """Helper method for GET requests."""
        response = self._session.get(
            urljoin(self._base_uri, endpoint),
            params=query_params,
        )

        return response

    def _post(
        self, endpoint: str, body_params: t.Optional[t.Mapping]
    ) -> requests.Response:
        """Helper method for POST requests."""
        response = self._session.post(
            urljoin(self._base_uri, endpoint),
            json=body_params,
        )
        return response

    def _delete(self, endpoint: str) -> requests.Response:
        """Helper method for DELETE requests."""
        response = self._session.delete(urljoin(self._base_uri, endpoint))

        return response

    # --- queries ---

    def get_secret(self, name: SecretName) -> SecretDefinition:
        """Get a secret from the API.

        Args:
            name: the name of the secret to be fetched.

        Raises:
            orquestra.sdk.secrets._exceptions.SecretNotFoundError: when no secret with
                the specified name is found.
            orquestra.sdk.secrets._exceptions.InvalidTokenError: when the authorization
                token is rejected by the remote cluster.
            orquestra.sdk.secrets._exceptions.UnknownHTTPError: when any other error is
                raised by the remote cluster.
        """
        resp = self._get(API_ACTIONS["get_secret"].format(name))

        if resp.status_code == requests.codes.NOT_FOUND:
            raise _exceptions.SecretNotFoundError(secret_name=name)

        try:
            _handle_common_errors(resp)
        except (_exceptions.InvalidTokenError, _exceptions.UnknownHTTPError):
            raise

        return SecretDefinition.model_validate(resp.json()["data"]["details"])

    def list_secrets(
        self, workspace_id: t.Optional[WorkspaceId]
    ) -> t.Sequence[SecretNameObj]:
        """List the available secrets.

        Args:
            workspace_id: the ID of the workspace to which the secrets belong.

        Raises:
            orquestra.sdk.secrets._exceptions.InvalidTokenError: when the authorization
                token is rejected by the remote cluster.
            orquestra.sdk.secrets._exceptions.UnknownHTTPError: when any other error is
                raised by the remote cluster.
        """
        resp = self._get(
            API_ACTIONS["list_secrets"],
            query_params=(
                ListSecretsRequest(workspace=workspace_id).model_dump()
                if workspace_id
                else None
            ),
        )
        try:
            _handle_common_errors(resp)
        except (_exceptions.InvalidTokenError, _exceptions.UnknownHTTPError):
            raise

        return [SecretNameObj.model_validate(d) for d in resp.json()["data"]]

    # --- mutations ---

    def create_secret(self, new_secret: SecretDefinition):
        """Post a new secret.

        Args:
            new_secret: the definition of the secret to be created.

        Raises:
            orquestra.sdk.secrets._exceptions.SecretAlreadyExistsError: when a secret
                with the specified name already exists.
            orquestra.sdk.secrets._exceptions.InvalidTokenError: when the authorization
                token is rejected by the remote cluster.
            orquestra.sdk.secrets._exceptions.UnknownHTTPError: when any other error is
                raised by the remote cluster
        """
        resp = self._post(
            API_ACTIONS["create_secret"],
            body_params={"data": new_secret.model_dump()},
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.SecretAlreadyExistsError(secret_name=new_secret.name)

        try:
            _handle_common_errors(resp)
        except (_exceptions.InvalidTokenError, _exceptions.UnknownHTTPError):
            raise

    def update_secret(self, name: SecretName, value: SecretValue):
        """Update the value of an existing secret.

        Args:
            name: the name of the secret to be updated.
            value: the new value to be assigned to the secret.

        Raises:
            orquestra.sdk.secrets._exceptions.SecretNotFoundError: When no secret with
                the specified name is found.
            orquestra.sdk.secrets._exceptions.InvalidTokenError: when the authorization
                token is rejected by the remote cluster.
            orquestra.sdk.secrets._exceptions.UnknownHTTPError: when any other error is
                raised by the remote cluster
        """
        obj = SecretValueObj(value=value)
        resp = self._post(
            API_ACTIONS["update_secret"].format(name),
            body_params={"data": obj.model_dump()},
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.SecretNotFoundError(secret_name=name)

        try:
            _handle_common_errors(resp)
        except (_exceptions.InvalidTokenError, _exceptions.UnknownHTTPError):
            raise

    def delete_secret(self, name: SecretName):
        """Remove a secret.

        Args:
            name: the name of the secret to be deleted.

        Raises:
            orquestra.sdk.secrets._exceptions.SecretNotFoundError: When no secret with
                the specified name is found.
            orquestra.sdk.secrets._exceptions.InvalidTokenError: when the authorization
                token is rejected by the remote cluster.
            orquestra.sdk.secrets._exceptions.UnknownHTTPError: when any other error is
                raised by the remote cluster
        """
        resp = self._delete(API_ACTIONS["delete_secret"].format(name))

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.SecretNotFoundError(secret_name=name)

        try:
            _handle_common_errors(resp)
        except (_exceptions.InvalidTokenError, _exceptions.UnknownHTTPError):
            raise
