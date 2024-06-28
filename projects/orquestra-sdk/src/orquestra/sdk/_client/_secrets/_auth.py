################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
import typing as t

from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.secrets import SecretsClient

from .._base._config._fs import read_config


def _read_config_opts(config_name: ConfigName):
    if config_name is None:
        raise exceptions.ConfigNameNotFoundError(
            "Please provide config name while accessing the secrets locally"
        )

    try:
        cfg = read_config(config_name=config_name)
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


class ConfigAuthorization:
    @staticmethod
    def authorize(config_name) -> t.Optional[SecretsClient]:
        return _authorize_with_config(config_name)
