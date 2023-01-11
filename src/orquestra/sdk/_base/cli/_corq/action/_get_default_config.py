################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import argparse
import typing as t

import orquestra.sdk._base._config as _config
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    GetDefaultConfig,
    ResponseMetadata,
    ResponseStatusCode,
)


def orq_get_default_config(
    args: argparse.Namespace,
) -> t.Union[GetDefaultConfig, ErrorResponse]:
    config_name = _config.read_default_config_name()

    return GetDefaultConfig(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message=f"Default config is {config_name}.",
        ),
        default_config_name=config_name,
    )
