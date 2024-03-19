################################################################################
# © Copyright 2023 - 2024 Zapata Computing Inc.
################################################################################
from enum import Enum
from typing import Any, Dict, Literal

from .._base._storage import BaseModel

CONFIG_FILE_CURRENT_VERSION = "0.0.2"

ConfigName = str


class RuntimeName(str, Enum):
    RAY_LOCAL = "RAY_LOCAL"
    CE_REMOTE = "CE_REMOTE"
    QE_REMOTE = "QE_REMOTE"
    IN_PROCESS = "IN_PROCESS"

    def __format__(self, format_spec: str) -> str:
        return format(self.value, format_spec)


RemoteRuntime = Literal[RuntimeName.CE_REMOTE]


class RuntimeConfiguration(BaseModel):
    config_name: ConfigName
    runtime_name: RuntimeName
    runtime_options: Dict[str, Any] = {}

    def __str__(self):
        outstr = (
            f"RuntimeConfiguration '{self.config_name}' with parameters:\n"
            f"- runtime name: {self.runtime_name}\n"
            "- runtime options:"
        )
        for key in self.runtime_options:
            outstr += f"\n  - {key}: {self.runtime_options[key]}"
        return outstr


class RuntimeConfigurationFile(BaseModel):
    """This schema is for the storage of "Runtime configurations".

    The major version number should be bumped when:

    * The values inside the configuration file are modified, for example if
      the ``configs`` option is renamed or the type is changed.

    * The shape of ``RuntimeConfiguration`` changes.
    """

    version: str
    configs: Dict[ConfigName, RuntimeConfiguration]
    default_config_name: ConfigName = "local"
