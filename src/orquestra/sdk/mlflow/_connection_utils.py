################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Utilities for communicating with mlflow."""

import os
from pathlib import Path

from orquestra.sdk._base import _env

DEFAULT_TEMP_ARTIFACTS_DIR: Path = Path.home() / ".orquestra" / "mlflow" / "artifacts"


def get_temp_artifacts_dir() -> Path:
    """
    Return a path to a temp directory that can be used to temporarily store artifacts.

    Uploading artifacts to MLFlow requires them to be written locally first. Finding an
    appropriate directory vary significantly between a workflow running locally and one
    running on a remote cluster. This function handles that complexity so that workflows
    do not need adjusting between runtimes.

    TODO: move this to a tutorial

    Example usage:
    ```
    from orquestra import sdk
    import mlflow

    artifacts_dir: Path = sdk.mlflow.get_temp_artifacts_dir()

    artifact_path = artifacts_dir / "final_state_dict.pickle"
    with artifact_path.open("wb") as f:
        pickle.dumps(my_model.state_dict())
    mlflow.log_artifact(artifact_path)
    ```
    """

    # In Studio and CE there is an environment variable that points to the artifact dir.
    if "ORQ_MLFLOW_ARTIFACTS_DIR" in os.environ:
        return Path(os.environ["ORQ_MLFLOW_ARTIFACTS_DIR"])

    # If the artifact dir envvar doesn't exist, we're probably executing locally.
    return DEFAULT_TEMP_ARTIFACTS_DIR
