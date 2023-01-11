################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Contains utilities for setting up the testing environment to replicate a
user's project state.
"""
from pathlib import Path

from orquestra.sdk._base import _config
from orquestra.sdk.schema import configs, ir

TINY_WORKFLOW_DEF = ir.WorkflowDef(
    name="single_invocation",
    fn_ref=ir.FileFunctionRef(
        file_path="empty.py",
        function_name="empty",
        line_number=0,
        type="FILE_FUNCTION_REF",
    ),
    imports={},
    tasks={},
    artifact_nodes={},
    constant_nodes={},
    task_invocations={},
    output_ids=[],
)


def write_user_config_file(
    dirpath: Path,
    runtime_config: configs.RuntimeConfiguration,
):
    config_file = dirpath / _config.CONFIG_FILE_NAME
    config_file_contents = configs.RuntimeConfigurationFile(
        version=_config.CONFIG_FILE_CURRENT_VERSION,
        configs={runtime_config.config_name: runtime_config},
    )
    config_file.write_text(config_file_contents.json())
