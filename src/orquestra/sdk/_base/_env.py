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

PASSPORT_FILE_ENV = "ORQUESTRA_PASSPORT_FILE"
"""
Consumed by the Workflow SDK to set auth in remote contexts
"""

RAY_GLOBAL_WF_RUN_ID_ENV = "GLOBAL_WF_RUN_ID"
"""
Used to set the workflow run ID in a Ray workflow
"""
