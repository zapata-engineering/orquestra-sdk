################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import json
import logging
import typing as t
import warnings

from packaging.version import parse as parse_version

from orquestra.sdk._base._factory import build_runtime_from_config

from ...exceptions import ConfigFileNotFoundError, ConfigNameNotFoundError
from ...schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    ConfigName,
    RuntimeConfiguration,
    RuntimeName,
)
from .. import _config
from ..abc import RuntimeInterface


class RuntimeConfig:
    """Encompasses the configuration with which a workflow can be run.

    Intended to be used with the WorkflowDef class to create a run with the desired
    configuration.

    If you want to submit workflows, please do not initialise RuntimeConfig objects
    directly. Instead, factory methods are provided for the supported runtimes.

    Example usage::

        # Create a config with the desired runtime
        config_in_process = RuntimeConfig.in_process()
        config_ray = RuntimeConfig.ray()
        config_ce = RuntimeConfig.ce()

        # Create the workflow run and begin its execution
        run = wf.run(config_in_process)
    """

    def __init__(
        self,
        runtime_name: str,
        name: str,
        bypass_factory_methods=False,
    ):
        if not bypass_factory_methods:
            raise ValueError(
                "Please use the appropriate factory method for your desired runtime. "
                "Supported runtimes are:\n"
                "`RuntimeConfig.in_process()` for in-process execution,\n"
                "`RuntimeConfig.ray()` for local Ray.\n"
                "`RuntimeConfig.ce()` for Compute Engine. \n"
            )

        self._name = name
        try:
            self._runtime_name: RuntimeName = RuntimeName(runtime_name)
        except ValueError as e:
            raise ValueError(
                f'"{runtime_name}" is not a valid runtime name. Valid names are:\n'
                + "\n".join(f'"{x.value}"' for x in RuntimeName)
            ) from e
        self._config_save_file = _config.get_config_file_path()

    def __str__(self) -> str:
        outstr = (
            f"RuntimeConfiguration '{self._name}' " f"for runtime {self._runtime_name} "
        )
        params_str = " with parameters:"
        for key in _config.RUNTIME_OPTION_NAMES:
            try:
                params_str += f"\n- {key}: {getattr(self, key)}"
            except AttributeError:
                continue
        if params_str == " with parameters:":
            outstr += "."
        else:
            outstr += params_str

        return outstr

    def __eq__(self, other) -> bool:
        if not isinstance(other, RuntimeConfig):
            return False

        return (
            self._name == other._name
            and self._runtime_name == other._runtime_name
            and self._get_runtime_options() == other._get_runtime_options()
        )

    @property
    def name(self) -> str:
        return self._name

    def _get_runtime_options(self) -> dict:
        """Construct a dictionary of the current runtime options.

        This is intended to translate between the user-facing API layer where runtime
        options are attributes, to the backend where we want them as a dict we can pass
        around.
        """
        runtime_options: dict = {}
        for key in _config.RUNTIME_OPTION_NAMES:
            if hasattr(self, key):
                runtime_options[key] = getattr(self, key)
        return runtime_options

    # region factories
    @classmethod
    def in_process(
        cls,
    ):
        """Factory method to generate RuntimeConfig objects for in-process runtimes.

        Returns:
            RuntimeConfig.
        """
        return RuntimeConfig("IN_PROCESS", "in_process", True)

    @classmethod
    def ray(
        cls,
    ) -> "RuntimeConfig":
        """Config for running workflows on Ray.

        Makes the SDK connect to a Ray cluster when you .run() the workflow.
        Requires starting the Ray cluster separately in the background via
        'ray start --head --storage=...'.
        """
        config = RuntimeConfig("RAY_LOCAL", "local", True)
        setattr(config, "log_to_driver", False)
        setattr(config, "configure_logging", False)

        # The paths for 'storage' and 'temp_dir' should have been passed when starting
        # the cluster, not here. Let's keep these attributes on our config object anyway
        # to retain the consistent shape
        setattr(config, "storage", None)
        setattr(config, "temp_dir", None)
        setattr(config, "address", "auto")
        return config

    @classmethod
    def ce(
        cls,
        uri: str,
        token: str,
    ) -> "RuntimeConfig":
        """Config for running workflows on Compute Engine.

        Args:
            uri: Address of the CE cluster on which to run the workflow.
            token: Authorisation token for access to the cluster.
        """
        runtime_name = RuntimeName.CE_REMOTE
        config_name = _config.generate_config_name(runtime_name, uri)

        config = RuntimeConfig(
            runtime_name,
            name=config_name,
            bypass_factory_methods=True,
        )
        setattr(config, "uri", uri)
        setattr(config, "token", token)
        _config.save_or_update(config_name, runtime_name, config._get_runtime_options())

        return config

    # endregion factories
    def _get_runtime(
        self,
    ) -> RuntimeInterface:
        """Build the run.

        Returns:
            Runtime: The runtime specified by the configuration.
        """
        runtime_options = {}
        for key in _config.RUNTIME_OPTION_NAMES:
            try:
                runtime_options[key] = getattr(self, key)
            except AttributeError:
                continue

        runtime_configuration = RuntimeConfiguration(
            config_name=self._name,
            runtime_name=self._runtime_name,
            runtime_options=runtime_options,
        )

        return build_runtime_from_config(config=runtime_configuration)

    # region LOADING FROM FILE
    @classmethod
    def list_configs(
        cls,
    ) -> t.List[ConfigName]:
        """List config names.

        Returns:
            list: list of configurations within the save file.
        """
        configs = _config.read_config_names() + list(_config.UNIQUE_CONFIGS)
        if _config.is_passport_file_available():
            configs.append(_config.AUTO_CONFIG_NAME)
        return configs

    @classmethod
    def load(
        cls,
        config_name: ConfigName,
    ):
        """Load an existing runtime configuration.

        For more information about the supported names, visit the
        `Runtime Configuration guide
        <https://docs.orquestra.io/docs/core/sdk/guides/runtime-configuration.html>`_.

        Args:
            config_name: The runtime name to load.

        Raises:
            orquestra.sdk.exceptions.ConfigFileNotFoundError: When the config file is
                of a higher version than this version of the SDK supports.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: When the specified config
                name is not present in the config file.

        Returns:
            RuntimeConfig: The configuration as loaded from the file.
        """
        # Doing this check here covers us for cases where the config file doesn't
        # exist but the user is trying to load one of the built in configs. There's
        # not need to create the config file in this case.
        if config_name in _config.SPECIAL_CONFIG_NAME_DICT:
            return cls._config_from_runtimeconfiguration(
                _config.read_config(config_name)
            )

        # Get the data from the save file
        _config_save_file = _config.get_config_file_path()
        with open(_config_save_file, "r") as f:
            data = json.load(f)

        # Migrate the file if necessary
        file_version = parse_version(data["version"])
        current_version = parse_version(CONFIG_FILE_CURRENT_VERSION)
        if file_version < current_version:
            warnings.warn(
                f"The config file at {_config_save_file} is out of date and will be "
                "migrated to the current version "
                f"(file has version {data['version']}, "
                f"the current version is {CONFIG_FILE_CURRENT_VERSION})."
            )
            migrate_config_file()
            with open(_config_save_file, "r") as f:
                data = json.load(f)
        elif file_version > current_version:
            raise ConfigFileNotFoundError(
                f"The config file at {_config_save_file} is a higher version than this "
                "version of the SDK supports "
                f"(file has version {data['version']}, "
                f"SDK supports versions up to {CONFIG_FILE_CURRENT_VERSION}). "
                "Please check that your version of the SDK is up to date."
            )

        # Read in the config from the file.
        try:
            config_data: RuntimeConfiguration = _config.read_config(config_name)
        except ConfigNameNotFoundError as e:
            raise ConfigNameNotFoundError(
                f"No config with name '{config_name}' "
                f"found in file {_config_save_file}. "
                f"Available names are: {cls.list_configs()}"
            ) from e

        config = cls._config_from_runtimeconfiguration(config_data)
        config._config_save_file = _config_save_file

        return config

    @classmethod
    def _config_from_runtimeconfiguration(
        cls, config: RuntimeConfiguration
    ) -> "RuntimeConfig":
        """Convert a RuntimeConfiguration object to a RuntimeConfig object.

        RuntimeConfiguration is used by the under-the-hood mechanisms;
        RuntimeConfig is user-facing int he Python API.

        Args:
            config: the RuntimeConfigration object to be converted (e.g. the return from
                _config.load()).
        """
        if config.runtime_name == RuntimeName.IN_PROCESS:
            return RuntimeConfig.in_process()
        elif config.runtime_name == RuntimeName.RAY_LOCAL:
            return RuntimeConfig.ray()

        interpreted_config = RuntimeConfig(
            config.runtime_name,
            config.config_name,
            bypass_factory_methods=True,
        )
        for key in config.runtime_options:
            setattr(interpreted_config, key, config.runtime_options[key])
        return interpreted_config

    # endregion LOADING FROM FILE

    def update_saved_token(self, token: str):
        """Update the stored auth token for this configuration.

        This also updates the token in memory for this RuntimeConfig object.

        Args:
            token: the new token.

        Raises:
            SyntaxError: When this method is called for a runtime configuration that
                does not use an authorisation token.
            ConfigNameNotFoundError: When there is no stored token to update.
        """
        if self._runtime_name != RuntimeName.CE_REMOTE:
            raise SyntaxError(
                "This runtime configuration does not require an authorization token. "
                "Nothing has been saved."
            )

        old_config = self._config_from_runtimeconfiguration(
            _config.read_config(self._name)
        )

        new_runtime_options: dict = old_config._get_runtime_options()
        new_runtime_options["token"] = token

        self.token = token
        _config.update_config(
            config_name=self._name,
            runtime_name=self._runtime_name,
            new_runtime_options=new_runtime_options,
        )
        logging.info(
            f"Updated authorisation token written to '{self._name}' "
            f"in file '{self._config_save_file}'. "
            "The new token is ready to be used in this runtime configuration."
        )


def migrate_config_file():
    """Update the stored configs."""
    # resolve list of files to migrate
    _config_file_path = _config.get_config_file_path().resolve()
    # Load existing file contents
    with open(_config_file_path, "r") as f:
        data = json.load(f)

    # Check version
    file_version = parse_version(data["version"])
    current_version = parse_version(CONFIG_FILE_CURRENT_VERSION)
    version_changed: bool = False
    if file_version > current_version:
        print(
            f"The file at {_config_file_path} cannot be migrated as its version is "
            "already greater than target version "
            f"(file is version {data['version']}, "
            f"current migration target is version {CONFIG_FILE_CURRENT_VERSION})."
        )
        return
    elif file_version < current_version:
        data["version"] = CONFIG_FILE_CURRENT_VERSION
        version_changed = True

    # Update configs
    changed: list = []
    for config_name in data["configs"]:
        if (
            data["configs"][config_name]["runtime_name"] == RuntimeName.RAY_LOCAL
            and "temp_dir" not in data["configs"][config_name]["runtime_options"]
        ):
            data["configs"][config_name]["runtime_options"]["temp_dir"] = None
            changed.append(config_name)

    # Write back to file if necessary)
    if len(changed) == 0 and not version_changed:
        print(f"No changes required for file '{_config_file_path}'")
        return
    else:
        with open(_config_file_path, "w") as f:
            f.write(json.dumps(data, indent=2))

    # Report changes to user
    print(
        f"Successfully migrated file {_config_file_path} to version "
        f"{CONFIG_FILE_CURRENT_VERSION}. "
        f"Updated {len(changed)} entr{'y' if len(changed) == 1 else 'ies'}"
        f"{'.' if len(changed) == 0 else ':'}"
    )
    for config_name in changed:
        print(f" - {config_name}")


def resolve_config(
    config: t.Union[ConfigName, "RuntimeConfig"],
) -> "RuntimeConfig":
    if isinstance(config, RuntimeConfig):
        # EZ. Passed-in explicitly.
        resolved_config = config
    elif isinstance(config, str):
        # Shorthand: just the config name.
        resolved_config = RuntimeConfig.load(config)
    else:
        raise TypeError(f"'config' is of unsupported type {type(config)}.")

    return resolved_config
