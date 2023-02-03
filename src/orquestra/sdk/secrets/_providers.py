################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Deferred authorization for accessing remote secrets vault.
"""

import os
import typing as t
from pathlib import Path

from .._base import _config
from ._client import SecretsClient


class SecretsProvider(t.Protocol):
    """
    Builds a ``SecretsClient`` object with a proper authorization.
    Implementations of this protocol decide where to get the auth from.
    """

    def make_client(
        self,
        config_name: t.Optional[str],
    ) -> SecretsClient:
        ...


def _read_config_opts(config_name):
    return _config.read_config(config_name=config_name).runtime_options


class ConfigProvider:
    """
    Gets auth creds from a local config file.

    Implements SecretsProvider.
    """

    def make_client(
        self,
        config_name: t.Optional[str],
    ):
        opts = _read_config_opts(config_name)
        return SecretsClient.from_token(base_uri=opts["uri"], token=opts["token"])


class PassportProvider:
    """
    Gets auth creds based on env variables:

    * The token is taken from a "passport" file. The location of this file is governed
      by ORQUESTRA_PASSPORT_FILE env var.
    * The URI is hardcoded to a self-reference.

    Implements SecretsProvider. Ignores the ``config_name`` and ``config_save_path``
    arguments.
    """

    # env var name
    ORQUESTRA_PASSPORT_FILE = "ORQUESTRA_PASSPORT_FILE"
    BASE_URI = "http://config-service.config-service:8099"

    def make_client(
        self,
        config_name: t.Optional[str],
    ) -> SecretsClient:
        """
        Ignores the arguments and reads passport and cluster URI from env vars.
        """
        passport_path = Path(os.environ[self.ORQUESTRA_PASSPORT_FILE])
        passport_token = passport_path.read_text()

        return SecretsClient.from_token(base_uri=self.BASE_URI, token=passport_token)
