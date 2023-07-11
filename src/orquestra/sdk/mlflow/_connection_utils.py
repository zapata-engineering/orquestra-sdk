################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Utilities for communicating with mlflow."""

import os
from pathlib import Path
from typing import Optional, Tuple

from requests import Response, Session

from orquestra import sdk
from orquestra.sdk._base import _env
from orquestra.sdk._base._services import ORQUESTRA_BASE_PATH
from orquestra.sdk.schema.configs import ConfigName

DEFAULT_TEMP_ARTIFACTS_DIR: Path = ORQUESTRA_BASE_PATH / "mlflow" / "artifacts"
RESOURCE_CATALOG_URI: str = "http://orquestra-resource-catalog.resource-catalog"
DEFAULT_ORQ_MLFLOW_CR_NAME: str = "mlflow"
DEFAULT_ORQ_MLFLOW_PORT: int = 8080


# region: private
def _get_mlflow_domain_name_and_port() -> Tuple[str, int]:
    """
    Reads in the CR name and port from environment variables.

    The environment variables are:
    - ORQ_MLFLOW_CR_NAME
    - ORQ_MLFLOW_PORT

    If a variable is not set, i.e. when executing locally and the user has not
    chosen to set them manually, returns the default value instead.

    Example usage:

    ```python
    mlflow_cr_name, mlflow_port = _get_mlflow_domain_name_and_port()
    ```
    """
    # TODO: check with studio/platform to ensure that the env var names are correct.
    # TODO: at present this function is called _only_ from a path that assumes we're
    # executing on a cluster. If that assumption also means we can assume that these
    # env vars are set, we can remove the defaults entirely.
    mlflow_cr_name: str
    mlflow_port: int
    if not (mlflow_cr_name := os.getenv(_env.ORQ_MLFLOW_CR_NAME)):
        mlflow_cr_name = DEFAULT_ORQ_MLFLOW_CR_NAME
    if not (mlflow_port := os.getenv(_env.ORQ_MLFLOW_PORT)):
        mlflow_port = DEFAULT_ORQ_MLFLOW_PORT
    return mlflow_cr_name, mlflow_port


def _read_passport_token() -> Optional[str]:
    """
    Read in the passport token environment variable, if it exists
    """
    if not (passport_path := os.getenv(_env.PASSPORT_FILE_ENV)):
        return None

    return Path(passport_path).read_text()


def _make_workspace_zri(workspace_id: str) -> str:
    """
    Make the workspace ZRI for the specified workspace ID.

    we have to build project ZRI from some hardcoded values + workspaceId based on
    https://zapatacomputing.atlassian.net/wiki/spaces/Platform/pages/512787664/2022-09-26+Zapata+Resource+Identifiers+ZRIs
    """  # noqa: E501
    default_tenant_id = 0
    special_workspace = "system"
    zri_type = "resource_group"

    return f"zri:v1::{default_tenant_id}:{special_workspace}:{zri_type}:{workspace_id}"


def _make_workspace_url(resource_catalog_url: str, workspace_zri: str) -> str:
    """
    Construct the URL for a workspace based on the resource catalog and workspace ZRI.
    """
    return f"{resource_catalog_url}/api/workspaces/{workspace_zri}"


def _make_session(token) -> Session:
    """
    Create a HTTP session with the specified token.
    """
    session = Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Authorization"] = f"Bearer {token}"
    return session


# endregion

# region: public


def get_temp_artifacts_dir() -> Path:
    """
    Return a path to a temp directory that can be used to temporarily store artifacts.

    Uploading artifacts to MLflow requires them to be written locally first. Finding an
    appropriate directory vary significantly between a workflow running locally and one
    running on a remote cluster. This function handles that complexity so that workflows
    do not need adjusting between runtimes.
    """

    temp_dir_path: Path = Path(
        os.getenv(_env.MLFLOW_ARTIFACTS_DIR) or DEFAULT_TEMP_ARTIFACTS_DIR
    )
    # In Studio and CE there is an environment variable that points to the artifact
    # directory. If the artifact dir envvar doesn't exist, assume we're executing
    # locally and use the default artifacts dir.

    temp_dir_path.mkdir(parents=True, exist_ok=True)

    return temp_dir_path


def get_tracking_uri(workspace_id: str, config_name: Optional[str] = None) -> str:
    """
    Get the MLFlow tracking URI for the specified workspace.

    Args:
        workspace_id: ID of the workspace.
        config_name: The name of a stored configuration specifying the execution
            environment. If this function is being executed remotely, this parameter
            can be omitted.

    Raises:
        ValueError: When this function is called without a config name in a local
            execution context.
    """

    # TODO: do we need to handle the case where a user has set the token environment
    # variable locally?
    # TODO: how do we handle cases where we're executing remotely, but the user
    # provides a config name that doesn't match?
    # TODO: do we need to worry about configs without a uri?
    if token := _read_passport_token():
        # Assume we're on a cluster
        session: Session = _make_session(token)
        workspace_zri: str = _make_workspace_zri(workspace_id)
        workspace_url: str = _make_workspace_url(RESOURCE_CATALOG_URI, workspace_zri)

        resp: Response = session.get(workspace_url)
        resp.raise_for_status()
        namespace: str = resp.json()["namespace"]
        mlflow_cr_name, mlflow_port = _get_mlflow_domain_name_and_port()

        return f"http://{mlflow_cr_name}.{namespace}:{mlflow_port}"
    else:
        # Assume we're executing locally, use external URIs
        if not config_name:
            raise ValueError("The config_name parameter is required for local runs.")
        cluster_uri: str = sdk.RuntimeConfig.load(config_name).uri

        return f"{cluster_uri}/mlflow/{workspace_id}"


def get_tracking_token(config_name: Optional[str] = None) -> str:
    """
    Get the MLFlow tracking token.

    Args:
        config_name: The name of a stored configuration specifying the execution
            environment. If this function is being executed remotely, this parameter
            can be omitted.

    Raises:
        ValueError: When this function is called without a config name in a local
            execution context.
    """
    if token := _read_passport_token():
        # Assume we're on a cluster
        return token
    else:
        # Assume we're executing locally, use the token from the config.
        if not config_name:
            raise ValueError("The config_name parameter is required for local runs.")
        config: sdk.RuntimeConfig = sdk.RuntimeConfig.load(config_name)
        return config.token


# endregion
