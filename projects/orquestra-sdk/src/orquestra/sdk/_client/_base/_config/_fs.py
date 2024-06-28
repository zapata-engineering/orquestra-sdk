################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""This is the internal module for saving and loading runtime configurations.

See docs/runtime_configurations.rst for more information.
"""
import os
import pathlib
from pathlib import Path
from typing import Any, List, Mapping, Optional, Union
from urllib.parse import ParseResult, urlparse

import filelock
import orquestra.workflow_shared.exceptions as exceptions
from orquestra.workflow_shared.env import PASSPORT_FILE_ENV
from orquestra.workflow_shared.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    ConfigName,
    RuntimeConfiguration,
    RuntimeConfigurationFile,
    RuntimeName,
)
from pydantic import ValidationError

from .._env import CONFIG_PATH_ENV, CURRENT_CLUSTER_ENV, CURRENT_CONFIG_ENV
from . import _settings


def get_config_file_path() -> Path:
    """Get the absolute path to the config file.

    Returns:
        Path: Path to the configuration file. The default is `~/.orquestra/config.json`
            but can be configured using the `ORQ_CONFIG_PATH` environment variable.
    """
    config_file_path = os.getenv(CONFIG_PATH_ENV)
    if config_file_path is not None:
        _config_file_path = Path(config_file_path).resolve()
    else:
        _config_file_path = Path.home() / ".orquestra" / _settings.CONFIG_FILE_NAME
    _ensure_directory(_config_file_path.parent)
    return _config_file_path


def is_passport_file_available() -> bool:
    return PASSPORT_FILE_ENV in os.environ


def _get_config_directory() -> Path:
    """Get the path to the directory that contains the configuration file.

    Returns:
        Path: path to the parent directory.
    """
    abs_file_path = get_config_file_path()
    return abs_file_path.parent


def _ensure_directory(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def _open_config_file() -> RuntimeConfigurationFile:
    config_file = get_config_file_path()
    if not config_file.exists():
        raise exceptions.ConfigFileNotFoundError(
            f"Config file {config_file} not found."
        )
    data: str = config_file.read_text()
    return RuntimeConfigurationFile.model_validate_json(data)


def _save_config_file(
    config_file_contents: RuntimeConfigurationFile,
):
    config_file: Path = get_config_file_path()
    config_file.write_text(data=config_file_contents.model_dump_json(indent=2))


EMPTY_CONFIG_FILE = RuntimeConfigurationFile(
    version=CONFIG_FILE_CURRENT_VERSION,
    configs=dict(),
)


def _resolve_config_file() -> Optional[RuntimeConfigurationFile]:
    try:
        return _open_config_file()
    except exceptions.ConfigFileNotFoundError:
        return None


def _save_new_config_file(
    resolved_config_name,
    resolved_runtime_name,
    resolved_runtime_options,
    resolved_prev_config_file,
):
    new_config_entry = RuntimeConfiguration(
        config_name=resolved_config_name,
        runtime_name=resolved_runtime_name,
        runtime_options=resolved_runtime_options,
    )
    new_config_file: RuntimeConfigurationFile
    if resolved_prev_config_file is not None:
        new_config_file = resolved_prev_config_file.model_copy(deep=True)
    else:
        new_config_file = RuntimeConfigurationFile(
            version=CONFIG_FILE_CURRENT_VERSION,
            configs={},
        )
    new_config_file.configs[resolved_config_name] = new_config_entry

    _save_config_file(new_config_file)


def _resolve_remote_auto_config(config_name) -> RuntimeConfiguration:
    passport_file = pathlib.Path(os.environ[PASSPORT_FILE_ENV])
    try:
        passport_token = Path(passport_file).read_text()
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Environmental variable {PASSPORT_FILE_ENV} was set, but no file was found"
            "under its value"
        ) from e

    runtime_config = _settings.SPECIAL_CONFIG_NAME_DICT[config_name]

    try:
        netloc = os.environ[CURRENT_CLUSTER_ENV]
    except KeyError:
        raise EnvironmentError(
            f"{PASSPORT_FILE_ENV} env variable was set, but {CURRENT_CLUSTER_ENV} not. "
            "Unable to deduce cluster's URI"
        )
    uri = ParseResult(
        scheme="https", netloc=netloc, path="", params="", query="", fragment=""
    ).geturl()

    runtime_config.runtime_options = {
        "uri": uri,
        "token": passport_token,
    }

    return runtime_config


def _resolve_local_auto_config(config_env: str) -> RuntimeConfiguration:
    # if someone sets "auto" as CURRENT_CONFIG_ENV variable, we would get into infinite
    # recursion here.
    if config_env == _settings.AUTO_CONFIG_NAME:
        raise exceptions.RuntimeConfigError(
            f"{_settings.AUTO_CONFIG_NAME} can not be the value "
            f"of {CURRENT_CONFIG_ENV} env variable."
        )
    try:
        config = read_config(config_env)
    except (exceptions.ConfigFileNotFoundError, exceptions.ConfigNameNotFoundError):
        raise exceptions.RuntimeConfigError(
            f"Couldn't find the config {config_env} specified in "
            f"{CURRENT_CONFIG_ENV} env variable."
        )
    return config


def _resolve_auto_config(config_name) -> RuntimeConfiguration:
    # On studio we short-circuit to use internal URIs
    if is_passport_file_available():
        return _resolve_remote_auto_config(config_name)
    elif CURRENT_CONFIG_ENV in os.environ:
        return _resolve_local_auto_config(os.environ[CURRENT_CONFIG_ENV])
    else:
        # we are not in the cluster, and default config env was not set. Error out
        raise exceptions.RuntimeConfigError(
            f"Using '{config_name}' as the config name requires that "
            f"you're using Studio or that the '{CURRENT_CONFIG_ENV}' "
            "environment variable is set.\n"
            "For example, if you want to use a local Ray cluster, "
            f"set `{CURRENT_CONFIG_ENV}=local`."
        )


def _handle_config_name_special_cases(config_name: str) -> RuntimeConfiguration:
    # special cases: the built-in config ('local') and in process config have
    # hardcoded runtime options.
    if config_name in _settings.SPECIAL_CONFIG_NAME_DICT:
        if config_name == _settings.AUTO_CONFIG_NAME:
            return _resolve_auto_config(config_name)
        else:
            return _settings.SPECIAL_CONFIG_NAME_DICT[config_name]
    else:
        raise NotImplementedError(
            f"Config name '{config_name}' is reserved, but we don't have a config "
            "to return for it. Please report this as a bug."
        )


def _resolve_runtime_options_for_writing(
    new_runtime_options: Optional[Mapping[str, Any]],
    resolved_prev_config_entry: Optional[RuntimeConfiguration],
) -> dict:
    """Resolve the runtime options that need to be written for this config.

    If there are previously existing runtime options, updates the old set with the new
    values.
    """
    return {
        **(
            # There are existing runtime options.
            {**resolved_prev_config_entry.runtime_options}
            if resolved_prev_config_entry is not None
            else {}
        ),
        **(
            # User wants to add new options.
            {**new_runtime_options}
            if new_runtime_options is not None
            else {}
        ),
    }


def _validate_runtime_options(
    runtime_name: RuntimeName,
    runtime_options: Mapping[str, Any],
) -> dict:
    """Check that the combination of configuration options is valid.

    Args:
        runtime_name: the intended runtime.
        runtime_options: the options to be checked.

    Raises:
        RuntimeConfigError: when one or more runtime configuration options do not
            relate to the specified runtime.
    """
    # Get list of options for this runtime
    permitted_options: list
    if runtime_name == RuntimeName.RAY_LOCAL:
        permitted_options = _settings.RAY_RUNTIME_OPTIONS
    elif runtime_name == RuntimeName.IN_PROCESS:
        permitted_options = _settings.IN_PROCESS_RUNTIME_OPTIONS
    elif runtime_name == RuntimeName.CE_REMOTE:
        permitted_options = _settings.CE_RUNTIME_OPTIONS
    else:
        raise NotImplementedError(
            "No runtime option validation is defined for runtime {runtime_name}."
        )

    for key in runtime_options:
        if key not in permitted_options:
            raise exceptions.RuntimeConfigError(
                f"'{key}' is not a valid option for the {runtime_name} runtime."
            )

    return dict(runtime_options)


def save_or_update(
    config_name: ConfigName, runtime_name: RuntimeName, runtime_options: dict
):
    if config_name in _settings.SPECIAL_CONFIG_NAME_DICT:
        raise ValueError(f"Can't update {config_name}, it's a reserved name")

    if config_name in read_config_names():
        update_config(config_name, runtime_name, runtime_options)
    else:
        write_config(config_name, runtime_name, runtime_options)


def write_config(
    config_name: str,
    runtime_name: RuntimeName,
    runtime_options: dict,
):
    """Write a new configuration to the file.

    Args:
        config_name: The name under which to save the configuration.
        runtime_name: The runtime to which this configuration relates.
        runtime_options: The runtime options contained within this configuration.
    """
    # Check that the runtime name is valid and that the runtime options relate to it.
    resolved_runtime_options = _validate_runtime_options(runtime_name, runtime_options)

    with filelock.FileLock(_get_config_directory() / _settings.LOCK_FILE_NAME):
        resolved_prev_config_file = _resolve_config_file()

        _save_new_config_file(
            config_name,
            runtime_name,
            resolved_runtime_options,
            resolved_prev_config_file,
        )


def update_config(
    config_name: ConfigName,
    runtime_name: RuntimeName,
    new_runtime_options: Optional[Mapping[str, Any]] = None,
):
    """Update the values of a stored configuration.

    Ensures that whatever non-None argument is passed here will end up saved to
    the config file under `~/.orquestra/config.json`.

    Args:
        config_name: A config file has multiple "entry" configurations.
            This tells which entry we want to update.
        runtime_name: Name of the runtime
        new_runtime_options: if not None, any entries in this dictionary will
            be added to the config entry's runtime options.
            The remaining runtime_options key-values that are already in the file will
            be left intact.

    Raises:
        RuntimeConfigError:
            - if one or more runtime options are not valid for this runtime.
    """
    with filelock.FileLock(_get_config_directory() / _settings.LOCK_FILE_NAME):
        config_file = _resolve_config_file()

        resolved_prev_config_entry = (
            config_file.configs.get(config_name)
            if config_file
            else EMPTY_CONFIG_FILE.configs.get(config_name)
        )

        resolved_options: dict = _resolve_runtime_options_for_writing(
            new_runtime_options,
            resolved_prev_config_entry,
        )

        try:
            resolved_runtime_options = _validate_runtime_options(
                runtime_name, resolved_options
            )
        except exceptions.RuntimeConfigError:
            raise

        _save_new_config_file(
            config_name,
            runtime_name,
            resolved_runtime_options,
            config_file,
        )


def generate_config_name(
    runtime_name: Union[RuntimeName, str], uri: Optional[str]
) -> ConfigName:
    """Generate a name for the specified runtime configuration options.

    CE_REMOTE configurations are named based on their cluster uri.
    All other configurations have static names assigned.
    """
    if runtime_name == RuntimeName.CE_REMOTE:
        if not uri:
            raise AttributeError(
                "CE runtime configurations must have a 'URI' value set."
            )
        new_name = _generate_cluster_uri_name(uri)
    elif runtime_name == RuntimeName.RAY_LOCAL:
        new_name = _settings.BUILT_IN_CONFIG_NAME
    elif runtime_name == RuntimeName.IN_PROCESS:
        new_name = _settings.IN_PROCESS_CONFIG_NAME
    else:
        raise NotImplementedError(
            f"No config naming schema is defined for Runtime '{runtime_name}'."
        )

    return new_name


def _generate_cluster_uri_name(uri: str) -> str:
    return str(urlparse(uri).netloc).split(".")[0]


def read_config(
    config_name: str,
) -> RuntimeConfiguration:
    """Reads a runtime configuration from the configuration file.

    Arguments:
        config_name: the name of the configuration to read
            - if it's 'local': this function returns the hardcoded local configuration

    Returns:
        a runtime configuration.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no runtime
            config matching `config_name` exists.
        orquestra.sdk.exceptions.ConfigFileNotFoundError: when no config file exists.
    """
    if config_name in _settings.SPECIAL_CONFIG_NAME_DICT:
        return _handle_config_name_special_cases(config_name)

    with filelock.FileLock(_get_config_directory() / _settings.LOCK_FILE_NAME):
        config_file = _resolve_config_file()
    # Handle missing file or config not in file
    if config_file is None:
        raise exceptions.ConfigFileNotFoundError("Could not locate config file.")
    if config_name not in config_file.configs:
        raise exceptions.ConfigNameNotFoundError(
            f"No config '{config_name}' found in file"
        )

    return config_file.configs[config_name]


def read_config_names() -> List[str]:
    """Reads the names of all configurations stored in the configuration file.

    Returns:
        list: a list of strings, each containing the name of a saved configuration.
            If the file does not exist, returns an empty list.
    """
    try:
        with filelock.FileLock(
            _get_config_directory() / _settings.LOCK_FILE_NAME, timeout=3
        ):
            config_file = _open_config_file()
            return [name for name in config_file.configs]
    except (
        exceptions.ConfigFileNotFoundError,
        ValidationError,
    ):
        return []
