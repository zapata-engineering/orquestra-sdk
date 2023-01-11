################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import argparse

import orquestra.sdk._base._config as _config
from orquestra.sdk.schema.responses import (
    ResponseMetadata,
    ResponseStatusCode,
    SetDefaultConfig,
)


def orq_set_default_config(args: argparse.Namespace) -> SetDefaultConfig:
    config_name = args.config_name

    _config.update_default_config_name(config_name)

    return SetDefaultConfig(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message=f"Default config was set to {config_name}.",
        ),
        default_config_name=config_name,
    )
