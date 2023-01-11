################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import argparse
import logging
import sys
from typing import Optional, Tuple, Union

import requests

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config
from orquestra.sdk._base._qe import _client
from orquestra.sdk.schema.configs import RuntimeName
from orquestra.sdk.schema.responses import ResponseStatusCode

ONLY_VIABLE_RUNTIME_NAME = RuntimeName.QE_REMOTE


def _store_token(config_name: str, token: str):
    """
    Write the new token to the existing stored config entry.

    Args:
        config_name: The name of the config to which to write this token.
        token: The token to write.
    """
    _config.update_config(
        config_name=config_name,
        # We need to pass it in case it's the first time we're writing this config.
        runtime_name=ONLY_VIABLE_RUNTIME_NAME,
        new_runtime_options={
            # We need to pass it in case it's the first time we're writing this config.
            "token": token,
        },
    )


def _get_token_from_stdin(config_name: str):
    token = input(
        "Please paste the token below. If the terminal hangs up, please press "
        "<Ctrl>+C and run: "
        f"'echo <paste token content> | orq set token -c {config_name} -t -'\n"
        "\033[92mToken: \033[0m"
    )
    return token


def _resolve_config_name_and_existance(
    passed_config_name: Optional[str], passed_use_default: Optional[bool] = False
) -> Tuple[Union[str, None], bool]:
    """
    If we have a name for the config, or are using the default, resolve the name. Also
    determine whether the config already exists, or if we need to create one.

    Args:
        passed_config_name: the config name to be resolved.
        passed_use_default: If true, tries to use the default config. Defaults to False.

    Raises:
        sysexit (INVALID_CLI_COMMAND_ERROR): if config already exists, but it relates
            to a runtime which does not require login.

    Returns:
        Tuple of the resolved name (which may be none), and a bool which is True if a
            config entry already exists.
    """

    # Case 1: We're using the default configuration
    if passed_use_default:
        resolved_config_name = _config.read_default_config_name()
        if passed_config_name is not None:
            assert passed_config_name == resolved_config_name

        config = _config.read_config(resolved_config_name)

        if config.runtime_name == ONLY_VIABLE_RUNTIME_NAME:
            logging.info(f"Using current default config '{resolved_config_name}'.")
            return resolved_config_name, True
        print(
            f"Cannot login with default config '{resolved_config_name}' "
            f"as it is not a {ONLY_VIABLE_RUNTIME_NAME} runtime config. "
            "Please provide the quantum engine server uri in order to log in with a "
            "new configuration, or specify an existing configuration.",
            file=sys.stderr,
        )
        sys.exit(ResponseStatusCode.INVALID_CLI_COMMAND_ERROR.value)

    # Case 2 or 3: We're not using the default but a name is provided. It's either
    # the name of an existing config, or it's the name of a new config. Check
    # against the list of know configs to determine, if it's there then case 2;
    # otherwise case 3.
    elif passed_config_name is not None:

        resolved_config_name = passed_config_name
        using_existing_config = passed_config_name in _config.read_config_names()
        if using_existing_config:
            logging.info(f"Loading config '{resolved_config_name}'.")
        return resolved_config_name, using_existing_config

    # Case 3: We're not using the default and no name is provided, therefore we
    # must be saving a new config under a generated name.
    return None, False


def _resolve_server_uri_from_file(
    resolved_config_name: str, passed_server_uri: Optional[str]
) -> str:
    """
    Read in the server uri from an existing config entry and compare to the passed
    server uri.

    Args:
        resolved_config_name: Name of the config entry from which to read.
        passed_server_uri: The uri argument to which to compare.

    Raises:
        sysexit (INVALID_CLI_COMMAND_ERROR): if the stored config lacks a uri value, and
            no uri value is supplied.

    Returns:
        str: _description_
    """

    # Read the uri from the file
    file_server_uri = None
    try:
        file_server_uri = _config.read_config(resolved_config_name).runtime_options[
            "uri"
        ]
    except KeyError:
        # If we can't read in the uri, the only option is to use the one that the user
        # has passed in, therefore if no uri has been passed then we'll have to error
        # out here.
        _require_server_uri(passed_server_uri)

    # Compare the file uri to the passed uri. If they're different, update to the passed
    # version. (If no uri is passed, ignore this check).
    if passed_server_uri is not None:
        resolved_server_uri = str(passed_server_uri)
        if passed_server_uri != file_server_uri:
            logging.info(
                f"The server uri for config '{resolved_config_name}' will be updated "
                f"from '{file_server_uri}' to '{passed_server_uri}'."
            )
    else:
        resolved_server_uri = str(file_server_uri)

    return resolved_server_uri


def _require_server_uri(passed_server_uri: Optional[str]):
    """
    Check that the URI is specified, raising an error if it is not.
    """
    if passed_server_uri is None:
        print(
            "Please provide the quantum engine server uri.",
            file=sys.stderr,
        )
        sys.exit(ResponseStatusCode.INVALID_CLI_COMMAND_ERROR.value)


def _resolve_runtime_name(passed_runtime_name) -> RuntimeName:
    # 1. It doesn't make sense to use any 'runtime_name' other than QE_RUNTIME,
    # so we don't need to resolve it. We're assuming that CLI arg parse allows
    # only QE_REMOTE.
    assert passed_runtime_name in {None, ONLY_VIABLE_RUNTIME_NAME.value}, (
        f"Oops, we've got a forbidden value of runtime_name: {passed_runtime_name}.,"
        " If you're seeing this, please report it as a bug."
    )
    return ONLY_VIABLE_RUNTIME_NAME


def orq_login(args: argparse.Namespace):
    # We have three potential cases to consider:
    # 1. We're using the default configuration.
    # 2. We're using an existing, named configuration.
    # 3. We're using a new configuration

    using_existing_config: bool
    _config_name: Union[str, None]
    config_name: str
    server_uri: str
    runtime_name: RuntimeName

    # Resolve provided names and determine whether we're writing a new config or not.
    _config_name, using_existing_config = _resolve_config_name_and_existance(
        args.config, args.default_config
    )

    # Resolve the runtime name
    runtime_name = _resolve_runtime_name(args.runtime)

    # Resolve the uri
    if using_existing_config:
        config_name = str(_config_name)
        server_uri = _resolve_server_uri_from_file(config_name, args.server_uri)
    else:
        _require_server_uri(args.server_uri)
        server_uri = args.server_uri

    # Construct the current runtime options
    resolved_runtime_options = {"uri": server_uri}

    # Save the config and set it as the default
    if using_existing_config:
        _config.update_config(
            config_name=config_name,
            runtime_name=runtime_name,
            new_runtime_options=resolved_runtime_options,
        )
    else:
        config_name = _config.generate_config_name(runtime_name, server_uri)
        # Generate a suitable name for the config
        stored_config, _ = _config.write_config(
            config_name,
            runtime_name,
            resolved_runtime_options,
        )

    _config.update_default_config_name(config_name)

    client = _client.QEClient(session=requests.Session(), base_uri=server_uri)
    # Ask QE for the login url to log in to the platform
    try:
        target_url = client.get_login_url()
    except requests.ConnectionError:
        print(f"Unable to communicate with server: {server_uri}", file=sys.stderr)
        sys.exit(ResponseStatusCode.CONNECTION_ERROR.value)

    except KeyError:
        print(f"Unable to get login url from server: {server_uri}", file=sys.stderr)
        sys.exit(ResponseStatusCode.CONNECTION_ERROR.value)

    print("Please follow this URL to proceed with login:")
    print(target_url)
    token = _get_token_from_stdin(config_name)
    print(token)
    if not token:
        print(
            "Error setting conf: Malformed token detected: empty token. Please log in again.",  # noqa: E501
            file=sys.stderr,
        )
        sys.exit(ResponseStatusCode.UNKNOWN_ERROR.value)

    _store_token(config_name, token)

    print(
        f"\nLogin to {server_uri} succeeded! {config_name} is now set as the default configuration."  # noqa: E501
    )
