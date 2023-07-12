################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Utilities for communicating with mlflow."""

import os
import typing as t
from pathlib import Path

from orquestra.sdk._base._config import read_config
from orquestra.sdk._base._env import CURRENT_USER_ENV
from orquestra.sdk._base._jwt import get_email_from_jwt_token
from orquestra.sdk._base._services import ORQUESTRA_BASE_PATH
from orquestra.sdk.exceptions import ConfigNameNotFoundError, RuntimeConfigError

DEFAULT_TEMP_ARTIFACTS_DIR: Path = ORQUESTRA_BASE_PATH / "mlflow" / "artifacts"


def get_temp_artifacts_dir() -> Path:
    """
    Return a path to a temp directory that can be used to temporarily store artifacts.

    Uploading artifacts to MLflow requires them to be written locally first. Finding an
    appropriate directory vary significantly between a workflow running locally and one
    running on a remote cluster. This function handles that complexity so that workflows
    do not need adjusting between runtimes.
    """

    path: Path
    if "ORQ_MLFLOW_ARTIFACTS_DIR" in os.environ:
        # In Studio and CE there is an environment variable that points to the artifact
        # directory.
        path = Path(os.environ["ORQ_MLFLOW_ARTIFACTS_DIR"])
    else:
        # If the artifact dir envvar doesn't exist, we're probably executing locally.
        path = DEFAULT_TEMP_ARTIFACTS_DIR

    path.mkdir(parents=True, exist_ok=True)

    return path


def get_current_user(config_name: t.Optional[str]) -> str:
    if CURRENT_USER_ENV in os.environ:
        return os.environ[CURRENT_USER_ENV]

    if config_name is None:
        raise ConfigNameNotFoundError("Unable to get current user without config.")

    try:
        token = read_config(config_name).runtime_options["token"]
    except KeyError:
        raise RuntimeConfigError(
            "Selected config does not have remote token configured"
            ". Did you log in it?"
        )

    return get_email_from_jwt_token(token)
