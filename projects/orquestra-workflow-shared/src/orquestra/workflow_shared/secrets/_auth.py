################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
import os
import typing as t
from pathlib import Path
from typing import Protocol

from orquestra.workflow_shared.schema.configs import ConfigName

from ..env import PASSPORT_FILE_ENV
from ._client import SecretsClient

# We assume that we can access the Config Service under a well-known URI if the passport
# auth is being used. This relies on the DNS configuration on the remote cluster.
BASE_URI = "http://config-service.config-service:8099"


class SecretAuthorization(Protocol):
    @staticmethod
    def authorize(config_name) -> t.Optional[SecretsClient]:
        ...


class PassportAuthorization:
    @staticmethod
    def authorize(config_name) -> t.Optional[SecretsClient]:
        if (passport_path := os.getenv(PASSPORT_FILE_ENV)) is None:
            return None

        passport_token = Path(passport_path).read_text()
        return SecretsClient.from_token(base_uri=BASE_URI, token=passport_token)


class AuthorizationMethodStorage:
    authorizations: t.List[SecretAuthorization] = [PassportAuthorization()]

    @staticmethod
    def register_authorization(authorization: SecretAuthorization):
        AuthorizationMethodStorage.authorizations.append(authorization)

    @staticmethod
    def authorize(config_name) -> t.Optional[SecretsClient]:
        for auth in AuthorizationMethodStorage.authorizations:
            client = auth.authorize(config_name)
            if client is not None:
                return client
        return None


def authorized_client(config_name: t.Optional[ConfigName]) -> SecretsClient:
    """Create an authorized secrets client.

    If the passport file environment variable is set, this will be preferentially used
    for authorisation.
    Otherwise, the named config will be used.

    Args:
        config_name: the config to be used for authorisation.

    Raises:
        AssertionError: if none of the methods returned proper authorized client
    """
    auth_methods = AuthorizationMethodStorage()

    if client := auth_methods.authorize(config_name):
        return client
    else:
        raise AssertionError("No authorization method returned proper client")
