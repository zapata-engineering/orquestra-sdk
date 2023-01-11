################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for accessing the Config Service API.

API spec: https://github.com/zapatacomputing/config-service/tree/main/openapi/src
"""
import typing as t
from urllib.parse import urljoin

import requests
from requests import codes

from . import _exceptions
from ._models import (
    SecretDefinition,
    SecretName,
    SecretNameObj,
    SecretValue,
    SecretValueObj,
)

API_ACTIONS = {
    "list_secrets": "/api/config/secrets",
    "create_secret": "/api/config/secrets",
    "get_secret": "/api/config/secrets/{}",
    "update_secret": "/api/config/secrets/{}",
    "delete_secret": "/api/config/secrets/{}",
}


def _handle_common_errors(response: requests.Response):
    if response.status_code == codes.UNAUTHORIZED:
        raise _exceptions.InvalidTokenError()
    elif not response.ok:
        raise _exceptions.UnknownHTTPError(response)


class SecretsClient:
    """
    Client for interacting with the Config Service API via HTTP.
    """

    def __init__(self, base_uri: str, session: requests.Session):
        self._base_uri = base_uri
        self._session = session

    @classmethod
    def from_token(cls, base_uri: str, token: str):
        """
        Args:
            base_uri: Orquestra cluster URI, like 'https://foobar.orquestra.io'.
            token: Auth token taken from logging in, or "the passport".
        """
        session = requests.Session()
        session.headers["Content-Type"] = "application/json"
        session.headers["Authorization"] = f"Bearer {token}"
        return cls(base_uri=base_uri, session=session)

    # --- helpers ---

    def _get(self, endpoint: str) -> requests.Response:
        """Helper method for GET requests"""
        response = self._session.get(urljoin(self._base_uri, endpoint))

        return response

    def _post(
        self, endpoint: str, body_params: t.Optional[t.Mapping]
    ) -> requests.Response:
        """Helper method for POST requests"""
        response = self._session.post(
            urljoin(self._base_uri, endpoint),
            json=body_params,
        )
        return response

    def _delete(self, endpoint: str) -> requests.Response:
        """Helper method for DELETE requests"""
        response = self._session.delete(urljoin(self._base_uri, endpoint))

        return response

    # --- queries ---

    def get_secret(self, name: SecretName) -> SecretDefinition:
        """
        Raises:
            SecretNotFoundError: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._get(API_ACTIONS["get_secret"].format(name))

        if resp.status_code == requests.codes.NOT_FOUND:
            raise _exceptions.SecretNotFoundError(secret_name=name)

        _handle_common_errors(resp)

        return SecretDefinition.parse_obj(resp.json()["data"]["details"])

    def list_secrets(self) -> t.Sequence[SecretNameObj]:
        """
        Raises:
            InvalidTokenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._get(API_ACTIONS["list_secrets"])
        _handle_common_errors(resp)

        return [SecretNameObj.parse_obj(d) for d in resp.json()["data"]]

    # --- mutations ---

    def create_secret(self, new_secret: SecretDefinition):
        """
        Raises:
            SecretAlreadyExistsError: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._post(
            API_ACTIONS["create_secret"],
            body_params={"data": new_secret.dict()},
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.SecretAlreadyExistsError(secret_name=new_secret.name)

        _handle_common_errors(resp)

    def update_secret(self, name: SecretName, value: SecretValue):
        """
        Raises:
            SecretNotFoundError: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        obj = SecretValueObj(value=value)
        resp = self._post(
            API_ACTIONS["update_secret"].format(name),
            body_params={"data": obj.dict()},
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.SecretNotFoundError(secret_name=name)

        _handle_common_errors(resp)

    def delete_secret(self, name: SecretName):
        """
        Raises:
            SecretNotFoundError: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._delete(API_ACTIONS["delete_secret"].format(name))

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.SecretNotFoundError(secret_name=name)

        _handle_common_errors(resp)
