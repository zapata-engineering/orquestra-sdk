import os
from pathlib import Path

from ._env import (
    ORQ_TASK_RUN_LOGS_DIR,
    RAY_PLASMA_PATH_ENV,
    RAY_STORAGE_PATH_ENV,
    RAY_TEMP_PATH_ENV,
)

ORQUESTRA_BASE_PATH = Path.home() / ".orquestra"


def ray_temp_path():
    """Used by RayRuntime to know where to look for the log files to read.

    Studio/Portal may need to override this in a special case.
    We will use an environment variable to override the location.
    This is unsupported outside of Studio/Portal.
    """
    try:
        return Path(os.environ[RAY_TEMP_PATH_ENV])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray"


def ray_storage_path():
    """See ``ray_temp_path``."""
    try:
        return Path(os.environ[RAY_STORAGE_PATH_ENV])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray_storage"


def ray_plasma_path():
    """See ``ray_temp_path``."""
    try:
        return Path(os.environ[RAY_PLASMA_PATH_ENV])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray_plasma"


def redirected_logs_dir():
    """Used by log redirection to store workflow logs.

    By default, this is `~/.orquestra/logs` and each workflow will have log files like:
     * `~/.orquestra/logs/wf/<wf run ID>/task/<task invocation ID>.err`
     * `~/.orquestra/logs/wf/<wf run ID>/task/<task invocation ID>.out`
    """
    try:
        return Path(os.environ[ORQ_TASK_RUN_LOGS_DIR])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "logs"
