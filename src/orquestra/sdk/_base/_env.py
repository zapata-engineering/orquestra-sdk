################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Global constants used to access environment variables."""

import os
import typing as t

# --------------------------------- SDK --------------------------------------

CONFIG_PATH_ENV = "ORQ_CONFIG_PATH"
"""
Used to configure the location of the `config.json`
Example:
    ORQ_CONFIG_PATH=/tmp/config.json
"""

PASSPORT_FILE_ENV = "ORQUESTRA_PASSPORT_FILE"
"""
Consumed by the Workflow SDK to set auth in remote contexts
"""

CURRENT_CLUSTER_ENV = "ORQ_CURRENT_CLUSTER"
"""
Set by Studio and CE to share cluster's URL
"""

CURRENT_WORKSPACE_ENV = "ORQ_CURRENT_WORKSPACE"
"""
Set by Studio and CE to share current workspace ID
"""

CURRENT_PROJECT_ENV = "ORQ_CURRENT_PROJECT"
"""
Set by Studio and CE to share current project ID
"""

CURRENT_USER_ENV = "ORQ_CURRENT_USER"
"""
Set by Studio and CE to share current user
"""

CURRENT_CONFIG_ENV = "ORQ_CURRENT_CONFIG"
"""
Can be set by the user to set default config when using "auto"
"""

ORQ_VERBOSE = "ORQ_VERBOSE"
"""
If set to a truthy value, enables printing debug information when running the ``orq``
CLI commands.
"""

ORQ_TASK_RUN_LOGS_DIR = "ORQ_TASK_RUN_LOGS_DIR"
"""
Used to set the storage location for task logs
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


# --------------------------------- MLFlow -----------------------------------

MLFLOW_CR_NAME = "ORQ_MLFLOW_CR_NAME"
"""
Used to set the MLFlow CR name.
"""

MLFLOW_PORT = "ORQ_MLFLOW_PORT"
"""
Used to set the port for communicating with MLFlow.
"""

MLFLOW_ARTIFACTS_DIR = "ORQ_MLFLOW_ARTIFACTS_DIR"
"""
Used to set the temporary directory to which mlflow artifacts can be written before
uploading.
"""

# --------------------------------- Dremio -----------------------------------

ORQ_DREMIO_URI = "ORQ_DREMIO_URI"
"""
Used for passing the Flight endpoint URI for DremioClient.
"""

ORQ_DREMIO_USER = "ORQ_DREMIO_USER"
"""
Used for passing the basic auth user for DremioClient.
"""

ORQ_DREMIO_PASS = "ORQ_DREMIO_PASS"
"""
Used for passing the basic auth password for DremioClient.
"""

# ------------------------------- utilities ----------------------------------


def _is_truthy(env_var_value: t.Optional[str]):
    if env_var_value is None:
        return False

    return env_var_value.lower() in {"1", "true"}


def flag_set(env_var_name: str) -> bool:
    value = os.getenv(env_var_name)
    return _is_truthy(value)
