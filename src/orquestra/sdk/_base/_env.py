################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import os
import typing as t

# --------------------------------- SDK --------------------------------------

CONFIG_PATH_ENV = "ORQ_CONFIG_PATH"
"""
Used to configure the location of the `config.json`
Example:
    ORQ_CONFIG_PATH=/tmp/config.json
"""

DB_PATH_ENV = "ORQ_DB_PATH"
"""
Used to configure the location of the `workflows.db`
Example:
    ORQ_DB_PATH=/tmp/workflows.db
"""

PASSPORT_FILE_ENV = "ORQUESTRA_PASSPORT_FILE"
"""
Consumed by the Workflow SDK to set auth in remote contexts
"""

CURRENT_CLUSTER_ENV = "ORQ_CURRENT_CLUSTER"
"""
Set by Studio to share cluster's URL
"""

CURRENT_WORKSPACE_ENV = "ORQ_CURRENT_WORKSPACE"
"""
Set by Studio to share current workspace ID
"""

CURRENT_PROJECT_ENV = "ORQ_CURRENT_PROJECT"
"""
Set by Studio to share current project ID
"""

ORQ_VERBOSE = "ORQ_VERBOSE"
"""
If set to a truthy value, enables printing debug information when running the ``orq``
CLI commands.
"""

# --------------------------------- Ray --------------------------------------

RAY_TEMP_PATH_ENV = "ORQ_RAY_TEMP_PATH"
"""
Used to configure the location of Ray's temp storage
Example:
    ORQ_RAY_TEMP_PATH=/tmp/ray/temp
"""

RAY_STORAGE_PATH_ENV = "ORQ_RAY_STORAGE_PATH"
"""
Used to configure the location of Ray's persistent storage
Example:
    ORQ_RAY_STORAGE_PATH=/tmp/ray/storage
"""

RAY_PLASMA_PATH_ENV = "ORQ_RAY_PLASMA_PATH"
"""
Used to configure the location of Ray's plasma storage
Example:
    ORQ_RAY_PLASMA_PATH=/tmp/ray/plasma
"""

RAY_DOWNLOAD_GIT_IMPORTS_ENV = "ORQ_RAY_DOWNLOAD_GIT_IMPORTS"
"""
Used to configure if Ray downloads Git imports
Example:
    ORQ_RAY_DOWNLOAD_GIT_IMPORTS=1
"""

RAY_GLOBAL_WF_RUN_ID_ENV = "GLOBAL_WF_RUN_ID"
"""
Used to set the workflow run ID in a Ray workflow
"""

RAY_SET_CUSTOM_IMAGE_RESOURCES_ENV = "ORQ_RAY_SET_CUSTOM_IMAGE_RESOURCES"
"""
Used to configure if Ray uses custom images
Example:
    ORQ_RAY_SET_CUSTOM_IMAGE_RESOURCES=1
"""


# ------------------------------- utilities ----------------------------------


def _is_truthy(env_var_value: t.Optional[str]):
    if env_var_value is None:
        return False

    return env_var_value.lower() in {"1", "true"}


def flag_set(env_var_name: str) -> bool:
    value = os.getenv(env_var_name)
    return _is_truthy(value)
