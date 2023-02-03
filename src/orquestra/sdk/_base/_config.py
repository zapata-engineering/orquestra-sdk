################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
This is the internal module for saving and loading runtime configurations.
See docs/runtime_configurations.rst for more information.
"""
import os
from pathlib import Path
from typing import Any, List, Mapping, Optional, Tuple, Union
from urllib.parse import urlparse

import filelock
from pydantic.error_wrappers import ValidationError

import orquestra.sdk.exceptions as exceptions
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    ConfigName,
    RuntimeConfiguration,
    RuntimeConfigurationFile,
    RuntimeName,
)

# Why JSON?
#  The Python TOML package is unmaintained as of 2022-02-18.
#  It is not compatible with the 1.0 version of the TOML spec:
#    https://github.com/uiri/toml/issues/267#issuecomment-886139340
#  YAML is not accepted by Pydantic's parse_file and is unlikely to ever be supported:
#    https://github.com/samuelcolvin/pydantic/issues/136
CONFIG_FILE_NAME = "config.json"
LOCK_FILE_NAME = "config.json.lock"
CONFIG_ENV_VARIABLE = "ORQ_CONFIG_PATH"
BUILT_IN_CONFIG_NAME = "local"
RAY_CONFIG_NAME_ALIAS = "ray"
IN_PROCESS_CONFIG_NAME = "in_process"

LOCAL_RUNTIME_CONFIGURATION = RuntimeConfiguration(
    config_name=BUILT_IN_CONFIG_NAME,
    runtime_name=RuntimeName.RAY_LOCAL,
    runtime_options={
        "address": "auto",
        "log_to_driver": False,
        "storage": None,
        "temp_dir": None,
        "configure_logging": False,
    },
)
IN_PROCESS_RUNTIME_CONFIGURATION = RuntimeConfiguration(
    config_name=IN_PROCESS_CONFIG_NAME,
    runtime_name=RuntimeName.IN_PROCESS,
    runtime_options={},
)

SPECIAL_CONFIG_NAME_DICT = {
    IN_PROCESS_CONFIG_NAME: IN_PROCESS_RUNTIME_CONFIGURATION,
    BUILT_IN_CONFIG_NAME: LOCAL_RUNTIME_CONFIGURATION,
    RAY_CONFIG_NAME_ALIAS: LOCAL_RUNTIME_CONFIGURATION,
}
# Unique config list to prompt to the users. Separate from SPECIAL_CONFIG_NAME_DICT
# as SPECIAL_CONFIG_NAME_DICT might have duplicate names which could be confusing for
# the user
UNIQUE_CONFIGS = {RAY_CONFIG_NAME_ALIAS, IN_PROCESS_CONFIG_NAME}


# region: runtime options
RAY_RUNTIME_OPTIONS: List[str] = [
    "address",
    "log_to_driver",
    "storage",
    "temp_dir",
    "configure_logging",
]
QE_RUNTIME_OPTIONS: List[str] = [
    "uri",
    "token",
]
CE_RUNTIME_OPTIONS: List[str] = [
    "uri",
    "token",
]
IN_PROCESS_RUNTIME_OPTIONS: List[str] = []
RUNTIME_OPTION_NAMES: List[str] = list(
    set(
        RAY_RUNTIME_OPTIONS
        + QE_RUNTIME_OPTIONS
        + IN_PROCESS_RUNTIME_OPTIONS
        + CE_RUNTIME_OPTIONS
    )
)
BOOLEAN_RUNTIME_OPTIONS: List[str] = ["log_to_driver", "configure_logging"]
# endregion


def _get_config_file_path() -> Path:
    """
    Get the absolute path to the config file.

    Returns:
        Path: Path to the configuration file. The default is `~/.orquestra/config.json`
            but can be configured using the `ORQ_CONFIG_PATH` environment variable.
    """
    config_file_path = os.getenv(CONFIG_ENV_VARIABLE)
    if config_file_path is not None:
        _config_file_path = Path(config_file_path).resolve()
    else:
        _config_file_path = Path.home() / ".orquestra" / CONFIG_FILE_NAME
    _ensure_directory(_config_file_path.parent)
    return _config_file_path


def _get_config_directory() -> Path:
    """
    Get the path to the directory that contains the configuration file.

    Returns:
        Path: path to the parent directory.
    """
    abs_file_path = _get_config_file_path()
    return abs_file_path.parent


def _ensure_directory(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def _open_config_file() -> RuntimeConfigurationFile:
    config_file = _get_config_file_path()
    if not config_file.exists():
        raise exceptions.ConfigFileNotFoundError(
            f"Config file {config_file} not found."
        )
    return RuntimeConfigurationFile.parse_file(config_file)


def _save_config_file(
    config_file_contents: RuntimeConfigurationFile,
) -> Path:
    config_file: Path = _get_config_file_path()
    config_file.write_text(data=config_file_contents.json(indent=2))
    return config_file


EMPTY_CONFIG_FILE = RuntimeConfigurationFile(
    version=CONFIG_FILE_CURRENT_VERSION,
    configs=dict(),
)


def _resolve_config_file_for_writing() -> Optional[RuntimeConfigurationFile]:
    try:
        # The full config.json. It contains multiple config entries. For
        # more info, see the models in
        # orquestra.sdk.schema.configs.
        return _open_config_file()
    except exceptions.ConfigFileNotFoundError:
        return None


def _resolve_new_config_file(
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
        new_config_file = resolved_prev_config_file.copy(deep=True)
    else:
        new_config_file = RuntimeConfigurationFile(
            version=CONFIG_FILE_CURRENT_VERSION,
            configs={},
        )
    new_config_file.configs[resolved_config_name] = new_config_entry
    return new_config_file


def _handle_config_name_special_cases(config_name: str) -> RuntimeConfiguration:
    # special cases: the built-in config ('local') and in process config have
    # hardcoded runtime options.
    if config_name in SPECIAL_CONFIG_NAME_DICT:
        return SPECIAL_CONFIG_NAME_DICT[config_name]
    else:
        raise NotImplementedError(
            f"Config name '{config_name}' is reserved, but we don't have a config "
            "to return for it. Please report this as a bug."
        )


def _resolve_runtime_options_for_writing(
    new_runtime_options: Optional[Mapping[str, Any]],
    resolved_config_name: str,
    resolved_prev_config_entry: Optional[RuntimeConfiguration],
) -> dict:
    """
    Resolve the runtime options that need to be written for this config.

    If there are previously existing runtime options, updates the old set with the new
    values.

    If the config name is one of the special cases, returns the hardcoded runtime
    options for that case.
    """
    if resolved_config_name in SPECIAL_CONFIG_NAME_DICT:
        return _handle_config_name_special_cases(resolved_config_name).runtime_options
    else:
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


def _resolve_runtime_name_for_writing(
    runtime_name, resolved_prev_config_entry, resolved_config_name
):
    resolved_runtime_name: str
    if runtime_name is not None:
        # the caller passed it in explicitly
        resolved_runtime_name = runtime_name
    elif resolved_prev_config_entry is not None:
        # it was stored already
        resolved_runtime_name = resolved_prev_config_entry.runtime_name
    else:
        # runtime_name == prev_config == None
        raise ValueError(
            f"Can't figure out an appropriate runtime_name for {resolved_config_name}. "
            "Please pass it explicitly."
        )
    return resolved_runtime_name


def _resolve_prev_config_entry_for_writing(
    prev_config_file, new_config_name
) -> Optional[RuntimeConfiguration]:
    if prev_config_file is not None:
        return prev_config_file.configs.get(new_config_name)
    else:
        return EMPTY_CONFIG_FILE.configs.get(new_config_name)


def _resolve_config_name_for_writing(
    config_name: Optional[str],
    prev_config_file: Optional[RuntimeConfigurationFile] = None,
) -> str:

    resolved_config_name = _resolve_config_name(config_name, prev_config_file)

    if resolved_config_name in SPECIAL_CONFIG_NAME_DICT:
        raise ValueError(f"Can't write {config_name}, it's a reserved name")

    return resolved_config_name


def _validate_runtime_options(
    runtime_name: RuntimeName,
    runtime_options: Optional[Mapping[str, Any]] = None,
) -> dict:
    """
    Check that the combination of configuration options is valid.

    Args:
        runtime_name: the intended runtime.
        runtime_options: the options to be checked.
        require_all: If True, options are valid only if every option required for this
            runtime is provided. If False, options are valid as long as each option
            relates to this runtime. Defaults to False.

    Raises:
        RuntimeConfigError: when one or more runtime configuration options do not
            relate to the specified runtime.
    """
    # Nothing to check, do nothing.
    if runtime_options is None or len(runtime_options) == 0:
        return {}

    # Get list of options for this runtime
    permitted_options: list
    if runtime_name == RuntimeName.QE_REMOTE:
        permitted_options = QE_RUNTIME_OPTIONS
    elif runtime_name == RuntimeName.RAY_LOCAL:
        permitted_options = RAY_RUNTIME_OPTIONS
    elif runtime_name == RuntimeName.IN_PROCESS:
        permitted_options = IN_PROCESS_RUNTIME_OPTIONS
    elif runtime_name == RuntimeName.CE_REMOTE:
        permitted_options = CE_RUNTIME_OPTIONS
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


def save_or_update(config_name, runtime_name, runtime_options):
    if config_name in read_config_names():
        update_config(config_name, runtime_name, runtime_options)
    else:
        write_config(config_name, runtime_name, runtime_options)


def write_config(
    config_name: Optional[str],
    runtime_name: Union[RuntimeName, str],
    runtime_options: dict,
) -> Tuple[RuntimeConfiguration, Path]:
    """
    Write a new configuration to the file.

    Args:
        config_name: The name under which to save the configuration. If set to None, a
            unique name will be generated for the config.
        runtime_name: The runtime to which this configuration relates.
        runtime_options: The runtime options contained within this configuration.

    Returns:
        RuntimeConfiguration: the configuration as saved.
        Path: The path of the file to which the configuration was written.
    """
    # Note: we allow config_names of None rather than simply making config_name an
    # optional parameter so that users have to _explicitely_ decline to provide a name
    # (and thereby activate the name generation) by passing None rather than
    # implicitly by omitting it.

    # Check that the runtime name is valid and that the runtime options relate to it.
    resolved_runtime_name: RuntimeName = RuntimeName(runtime_name)
    resolved_runtime_options = _validate_runtime_options(
        resolved_runtime_name, runtime_options
    )

    with filelock.FileLock(_get_config_directory() / LOCK_FILE_NAME):
        # Get the config name to save under - either the user-defined one, or an auto
        # generated one if the config_name parameter is None. Either way we need to
        # pass it through _resolve_config_name_for_writing() as this handles the
        # protected names like 'local'.
        resolved_config_name: ConfigName = _resolve_config_name_for_writing(config_name)

        resolved_prev_config_file = _resolve_config_file_for_writing()

        new_config_file = _resolve_new_config_file(
            resolved_config_name,
            resolved_runtime_name,
            resolved_runtime_options,
            resolved_prev_config_file,
        )

        saved_config = RuntimeConfiguration(
            config_name=resolved_config_name,
            runtime_name=resolved_runtime_name,
            runtime_options=resolved_runtime_options,
        )

        return (
            saved_config,
            _save_config_file(new_config_file),
        )


def update_config(
    config_name: Optional[ConfigName] = None,
    runtime_name: Optional[RuntimeName] = None,
    new_runtime_options: Optional[Mapping[str, Any]] = None,
) -> Tuple[RuntimeConfiguration, Path]:
    """
    Ensures that whatever non-None argument is passed here will end up saved to
    the config file under `~/.orquestra/config.json`.

    Note that a single config file has multiple "entry" configurations, a
    reserved "local", and a "default" one.

    Args:
        config_name: A config file has multiple "entry" configurations. This
            tells which entry we want to update. If `None`, this will be
            inferred from the "default config name" stored in the file.
        runtime_name: if not None, it will be stored under the appropriate config entry.
        new_runtime_options: if not None, any entries in this dictionary will
            be added to the config entry's runtime options. The remaining
            runtime_options key-values that are already in the file will be
            left intact.

    Returns:
        RuntimeConfiguration: the configuration as saved.
        Path: the path to the file to which the changes have been written.

    Raises:
        ValueError:
            - if `config_name` was resolved to "local".
            - if `config_name` couldn't be resolved automatically.
        KeyError:
            - if one or more runtime options are not valid for this runtime.
    """

    with filelock.FileLock(_get_config_directory() / LOCK_FILE_NAME):
        # We need to retain a lock because we save the config file at the end
        # of this function.

        resolved_prev_config_file = _resolve_config_file_for_writing()

        resolved_config_name = _resolve_config_name_for_writing(
            config_name, resolved_prev_config_file
        )

        resolved_prev_config_entry = _resolve_prev_config_entry_for_writing(
            resolved_prev_config_file, resolved_config_name
        )

        resolved_runtime_name = _resolve_runtime_name_for_writing(
            runtime_name, resolved_prev_config_entry, resolved_config_name
        )

        resolved_runtime_options = _validate_runtime_options(
            resolved_runtime_name,
            _resolve_runtime_options_for_writing(
                new_runtime_options,
                resolved_config_name,
                resolved_prev_config_entry,
            ),
        )

        new_config_file = _resolve_new_config_file(
            resolved_config_name,
            resolved_runtime_name,
            resolved_runtime_options,
            resolved_prev_config_file,
        )

        return RuntimeConfiguration(
            config_name=resolved_config_name,
            runtime_name=resolved_runtime_name,
            runtime_options=resolved_runtime_options,
        ), _save_config_file(new_config_file)


def _resolve_config_file_for_reading() -> Optional[RuntimeConfigurationFile]:
    try:
        with filelock.FileLock(_get_config_directory() / LOCK_FILE_NAME):
            return _open_config_file()
    except exceptions.ConfigFileNotFoundError:
        return None


def _resolve_config_name_for_reading(
    config_name: Optional[str], config_file: Optional[RuntimeConfigurationFile]
) -> str:
    return _resolve_config_name(config_name, config_file)


def _resolve_config_name(
    config_name: Optional[str], config_file: Optional[RuntimeConfigurationFile]
) -> str:
    """Turns out we can reuse this logic for both reading and writing"""
    if config_name in SPECIAL_CONFIG_NAME_DICT:
        # the built-in hardcoded value
        return config_name
    elif config_name is not None:
        # the caller passed it in explicitly
        return config_name
    elif config_file is not None:
        # Caller didn't pass config name. The config file exists, and we're
        # using the default value.
        return config_file.default_config_name
    else:
        # config_name == None & config_file == None
        raise ValueError(
            "Couldn't resolve an appropriate config name to read the "
            "configuration from. Please pass it explicitly."
        )


def _resolve_config_entry_for_reading(
    config_name: str, config_file: Optional[RuntimeConfigurationFile]
) -> RuntimeConfiguration:
    """
    Resolve the specified configuration.

    Args:
        config_name: Name of the config to be read
        config_file: File from which the config is to be read. If None, defaults to the
            default config file location.

    Raises:
        exceptions.ConfigFileNotFoundError: if the specified config file does not exist.
        exceptions.ConfigNameNotFoundError: if the specified config is not stored in
            the file.
        NotImplementedError: if the config name is in the list of reserved names, but
            we don't have a configuration to return for that case.

    Returns:
        RuntimeConfiguration
    """
    # Deal with special cases
    if config_name in SPECIAL_CONFIG_NAME_DICT:
        return _handle_config_name_special_cases(config_name)

    # Handle missing file or config not in file
    if config_file is None:
        raise exceptions.ConfigFileNotFoundError("Could not locate config file.")
    if config_name not in config_file.configs:
        raise exceptions.ConfigNameNotFoundError(
            f"No config '{config_name}' found in file"
        )

    return config_file.configs[config_name]


def generate_config_name(
    runtime_name: Union[RuntimeName, str], uri: Optional[str]
) -> ConfigName:
    """
    Generate a for the specified runtime configuration options.

    QE_REMOTE configurations are named based on their cluster uri. All other
    configurations have static names assigned.
    """
    if runtime_name == RuntimeName.QE_REMOTE or runtime_name == RuntimeName.CE_REMOTE:
        if not uri:
            raise AttributeError(
                "QE and CE runtime configurations must have a 'URI' value set."
            )
        new_name = _generate_cluster_uri_name(uri)
    elif runtime_name == RuntimeName.RAY_LOCAL:
        new_name = BUILT_IN_CONFIG_NAME
    elif runtime_name == RuntimeName.IN_PROCESS:
        new_name = IN_PROCESS_CONFIG_NAME
    else:
        raise NotImplementedError(
            f"No config naming schema is defined for Runtime '{runtime_name}'."
        )

    return new_name


def _generate_cluster_uri_name(uri: str) -> str:
    return str(urlparse(uri).netloc).split(".")[0]


def read_config(
    config_name: Optional[str],
) -> RuntimeConfiguration:
    """
    Reads a runtime configuration from the configuration file

    Arguments:
        config_name: the name of the configuration to read
            - if it's 'local': this function returns the hardcoded local configuration
            - if it's None: this function returns the configuration set as the
              default one. The default configuration can be user-specified
              (read from the file) or a hardcoded, "local" one.

    Returns:
        a runtime configuration

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no runtime
            config matching `config_name` exists.
    """
    resolved_config_file = _resolve_config_file_for_reading()
    resolved_config_name = _resolve_config_name_for_reading(
        config_name, resolved_config_file
    )
    resolved_config_entry = _resolve_config_entry_for_reading(
        resolved_config_name, resolved_config_file
    )

    return resolved_config_entry


def read_default_config_name() -> str:
    """
    Reads a default configuration name from the configuration file, or returns
    the built-in default ("local").
    """

    try:
        with filelock.FileLock(_get_config_directory() / LOCK_FILE_NAME):
            config_file = _open_config_file()
    except (exceptions.ConfigFileNotFoundError, FileNotFoundError):
        return BUILT_IN_CONFIG_NAME

    return config_file.default_config_name


def read_config_names() -> List[str]:
    """
    Reads the names of all configurations stored in the configuration file.

    Arguments:
        config_file_path: the path to the file where the configurations are saved. If
            omitted, the default file location is used.

    Returns:
        list: a list of strings, each containing the name of a saved configuration. If
            the file does not exist, returns an empty list.
    """
    try:
        with filelock.FileLock(_get_config_directory() / LOCK_FILE_NAME, timeout=3):
            return _read_config_names()
    except filelock.Timeout:
        raise IOError(
            "Could not acquire the lock for the config file at "
            f"'{_get_config_file_path()}' "
            "- does another function or process currently hold it? "
            "If you're calling `read_config_names` from a context that has already "
            "acquired the lock, you may want to look into using `_read_config_names` "
            "instead."
        )


def _read_config_names() -> List[str]:
    """
    Reads the names of all configurations stored in the configuration file.

    This function is intended for internal use in cases where we already have a
    filelock for the config file. If you aren't wrapping the call to this function in
    `with filelock...`, you should probably be using `read_config_names` instead.

    Arguments:
        config_file_path: the path to the file where the configurations are saved. If
            omitted, the default file location is used.

    Returns:
        list: a list of strings, each containing the name of a saved configuration. If
            the file does not exist, returns an empty list.
    """

    try:
        config_file = _open_config_file()
    except (
        exceptions.ConfigFileNotFoundError,
        FileNotFoundError,
        ValidationError,
    ):
        return []
    return [name for name in config_file.configs]


def update_default_config_name(default_config_name: str):
    """
    Sets the "default_config_name" field in the user config. Creates the config
    file if it didn't exist already. Isn't expected to raise any exceptions.

    Args:
        default_config_name: the name of the configuration to update
    """

    with filelock.FileLock(_get_config_directory() / LOCK_FILE_NAME):
        prev_config_file = _resolve_config_file_for_writing()

        if prev_config_file is not None:
            new_config_file = prev_config_file.copy(deep=True)
        else:
            new_config_file = EMPTY_CONFIG_FILE.copy(deep=True)

        new_config_file.default_config_name = default_config_name
        _save_config_file(new_config_file)
