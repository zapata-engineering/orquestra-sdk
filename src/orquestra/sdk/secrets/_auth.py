################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import os
import typing as t
from pathlib import Path

from orquestra.sdk import exceptions
from orquestra.sdk.schema.configs import ConfigName

from .._base import _config
from .._base._env import PASSPORT_FILE_ENV
from ._client import SecretsClient

# We assume that we can access the Config Service under a well-known URI if the passport
# auth is being used. This relies on the DNS configuration on the remote cluster.
BASE_URI = "http://config-service.config-service:8099"


def _authorize_with_passport() -> t.Optional[SecretsClient]:
    if (passport_path := os.getenv(PASSPORT_FILE_ENV)) is None:
        return None

    passport_token = Path(passport_path).read_text()
    return SecretsClient.from_token(base_uri=BASE_URI, token=passport_token)


def _read_config_opts(config_name: ConfigName):
    try:
        cfg = _config.read_config(config_name=config_name)
    except exceptions.ConfigNameNotFoundError:
        raise

    return cfg.runtime_options


def _authorize_with_config(
    config_name: ConfigName,
) -> SecretsClient:
    try:
        opts = _read_config_opts(config_name)
    except exceptions.ConfigNameNotFoundError:
        raise

    return SecretsClient.from_token(base_uri=opts["uri"], token=opts["token"])


def authorized_client(config_name: t.Optional[ConfigName]) -> SecretsClient:
    """Create an authorized secrets client.

    If the passport file environment variable is set, this will be preferentially used
    for authorisation.
    Otherwise, the named config will be used.

    Args:
        config_name: the config to be used for authorisation.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no matching config was
            found.

    """
    # At the moment there are only two ways to authorize the secrets client: passport
    # and config file. If more schemes are developed in the future, they should be
    # added here.
    if (passport_client := _authorize_with_passport()) is not None:
        return passport_client

    if config_name is None:
        raise exceptions.ConfigNameNotFoundError(
            "Please provide config name while accessing the secrets locally"
        )

    try:
        return _authorize_with_config(config_name)
    except exceptions.ConfigNameNotFoundError:
        raise
