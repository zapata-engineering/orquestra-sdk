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

ORQ_TASK_RUN_LOGS_DIR = "ORQ_TASK_RUN_LOGS_DIR"
"""
Used to set the storage location for task logs
"""
