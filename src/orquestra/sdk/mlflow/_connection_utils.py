################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Utilities for communicating with mlflow."""

import os
from pathlib import Path

from orquestra.sdk._base._services import ORQUESTRA_BASE_PATH

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
