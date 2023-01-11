################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""
Implementation of the command::

    orq set token \
        -s/--server_url <url> \
        -c/--config <config name> \
        -t/--token_file <filepath>
"""
import typing as t
from argparse import Namespace

from orquestra.sdk._base import _config
from orquestra.sdk.schema.configs import RuntimeName
from orquestra.sdk.schema.responses import (
    ResponseMetadata,
    ResponseStatusCode,
    SetTokenResponse,
)

STDIN_ALIAS = "-"
ONLY_VIABLE_RUNTIME_NAME = RuntimeName.QE_REMOTE


def _resolve_token(token_filename: str) -> str:
    if token_filename == STDIN_ALIAS:
        content = input()
        return content
    else:
        with open(token_filename) as f:
            content = f.read().strip()
            return content


def orq_set_token(args: Namespace) -> SetTokenResponse:
    """
    Expects the following CLI parameters:
        server_uri
        config
        token_file
    """
    # handle parsing CLI args and default values
    server_uri: t.Optional[str] = args.server_uri
    config: t.Optional[str] = args.config
    token_file: str = args.token_file

    token = _resolve_token(token_file)

    _config.update_config(
        config_name=config,
        runtime_name=ONLY_VIABLE_RUNTIME_NAME,
        new_runtime_options={
            **({"uri": server_uri} if server_uri is not None else {}),
            "token": token,
        },
    )
    result_config = _config.read_config(config)

    return SetTokenResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Token was stored in the config file.",
        ),
        result_config=result_config,
    )
