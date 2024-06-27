################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################

from typing import List

from orquestra.workflow_shared.schema.configs import RuntimeConfiguration, RuntimeName

# Why JSON?
#  The Python TOML package is unmaintained as of 2022-02-18.
#  It is not compatible with the 1.0 version of the TOML spec:
#    https://github.com/uiri/toml/issues/267#issuecomment-886139340
#  YAML is not accepted by Pydantic's parse_file and is unlikely to ever be supported:
#    https://github.com/samuelcolvin/pydantic/issues/136
CONFIG_FILE_NAME = "config.json"
LOCK_FILE_NAME = "config.json.lock"

BUILT_IN_CONFIG_NAME = "local"
RAY_CONFIG_NAME_ALIAS = "ray"
IN_PROCESS_CONFIG_NAME = "in_process"
AUTO_CONFIG_NAME = "auto"

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
# this runtime config is not ready-to-be-used without runtime options
SAME_CLUSTER_RUNTIME_CONFIGURATION = RuntimeConfiguration(
    config_name=AUTO_CONFIG_NAME,
    runtime_name=RuntimeName.CE_REMOTE,
    runtime_options={},
)

SPECIAL_CONFIG_NAME_DICT = {
    IN_PROCESS_CONFIG_NAME: IN_PROCESS_RUNTIME_CONFIGURATION,
    BUILT_IN_CONFIG_NAME: LOCAL_RUNTIME_CONFIGURATION,
    RAY_CONFIG_NAME_ALIAS: LOCAL_RUNTIME_CONFIGURATION,
    AUTO_CONFIG_NAME: SAME_CLUSTER_RUNTIME_CONFIGURATION,
}
# Unique config list to prompt to the users. Separate from SPECIAL_CONFIG_NAME_DICT
# as SPECIAL_CONFIG_NAME_DICT might have duplicate names which could be confusing for
# the user
UNIQUE_CONFIGS = {RAY_CONFIG_NAME_ALIAS, IN_PROCESS_CONFIG_NAME}

CLI_IGNORED_CONFIGS = {IN_PROCESS_CONFIG_NAME}

RAY_RUNTIME_OPTIONS: List[str] = [
    "address",
    "log_to_driver",
    "storage",
    "temp_dir",
    "configure_logging",
]
CE_RUNTIME_OPTIONS: List[str] = [
    "uri",
    "token",
]
IN_PROCESS_RUNTIME_OPTIONS: List[str] = []
RUNTIME_OPTION_NAMES: List[str] = list(
    set(RAY_RUNTIME_OPTIONS + IN_PROCESS_RUNTIME_OPTIONS + CE_RUNTIME_OPTIONS)
)
BOOLEAN_RUNTIME_OPTIONS: List[str] = ["log_to_driver", "configure_logging"]
