################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Contains utilities for setting up the testing environment to replicate a
user's project state.
"""
from pathlib import Path

from orquestra.workflow_shared.schema import configs

from orquestra.sdk._client._base import _config


def write_user_config_file(
    dirpath: Path,
    runtime_config: configs.RuntimeConfiguration,
):
    config_file = dirpath / _config.CONFIG_FILE_NAME
    config_file_contents = configs.RuntimeConfigurationFile(
        version=_config.CONFIG_FILE_CURRENT_VERSION,
        configs={runtime_config.config_name: runtime_config},
    )
    config_file.write_text(config_file_contents.model_dump_json())
