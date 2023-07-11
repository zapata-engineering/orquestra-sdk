################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Utilities for communicating with mlflow."""

import os
import warnings
from pathlib import Path
from typing import Optional, Tuple

from requests import Response, Session

from orquestra import sdk
from orquestra.sdk._base import _env
from orquestra.sdk._base._services import ORQUESTRA_BASE_PATH
from orquestra.sdk.schema.configs import ConfigName

DEFAULT_TEMP_ARTIFACTS_DIR: Path = ORQUESTRA_BASE_PATH / "mlflow" / "artifacts"
RESOURCE_CATALOG_URI: str = "http://orquestra-resource-catalog.resource-catalog"


# region: private
def _is_executing_remoteley() -> bool:
    """
    Determine whether the code is being executed locally, or on a cluster/studio.
    """
    # TODO: at present this uses the platform-specific environment variables, assuming
    # that if all of them are set then we must be executing remotely. This assumption
    # is not 100% - power users may set the envvars locally for fine-grained control.
    # However, the platform will shortly be adding an envvar specifically to track
    # whether we're executing remotely. Once that is done, this function should be
    # updated to check that specifically. Tests should be added for this function at
    # that time.
    envvars = [
        _env.PASSPORT_FILE_ENV,
        _env.MLFLOW_CR_NAME,
        _env.MLFLOW_PORT,
        _env.PASSPORT_FILE_ENV,
        _env.MLFLOW_ARTIFACTS_DIR,
    ]
    if None in [os.getenv(envvar) for envvar in envvars]:
        return False
    return True


def _get_mlflow_cr_name_and_port() -> Tuple[str, int]:
    """
    Reads in the MLFlow custom resource name and port from environment variables.

    The environment variables are:
    - ORQ_MLFLOW_CR_NAME
    - ORQ_MLFLOW_PORT

    Example usage:

    ```python
    mlflow_cr_name, mlflow_port = _get_mlflow_cr_name_and_port()
    ```

    Raises:
        EnvironmentError: when either of the environment variables are not set.
    """

    assert (
        _is_executing_remoteley()
    ), "This function should not be called when running locally."

    return os.getenv(_env.MLFLOW_CR_NAME), int(os.getenv(_env.MLFLOW_PORT))


def _read_passport_token() -> str:
    """
    Reads in the token.

    The file path is specified by the PASSPOT_FILE_ENV environment variable.
    """
    return Path(os.getenv(_env.PASSPORT_FILE_ENV)).read_text()


def _make_workspace_zri(workspace_id: str) -> str:
    """
    Make the workspace ZRI for the specified workspace ID.

    Builds project ZRI from some hardcoded values and the workspaceId based on
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
        ValueError: When this function is called in a local execution context without
            specifying a valid non-local config name.
    """

    if _is_executing_remoteley():
        if config_name is not None:
            warnings.warn(
                "The 'config_name' parameter is used only when executing locally, "
                "and will be ignored."
            )
        token: str = _read_passport_token()
        session: Session = _make_session(token)
        workspace_zri: str = _make_workspace_zri(workspace_id)
        workspace_url: str = _make_workspace_url(RESOURCE_CATALOG_URI, workspace_zri)

        resp: Response = session.get(workspace_url)
        resp.raise_for_status()
        namespace: str = resp.json()["namespace"]
        mlflow_cr_name, mlflow_port = _get_mlflow_cr_name_and_port()

        return f"http://{mlflow_cr_name}.{namespace}:{mlflow_port}"
    else:
        # Assume we're executing locally, use external URIs
        if not config_name:
            raise ValueError("The config_name parameter is required for local runs.")

        # TODO: try-except block to raise a more informative error message.
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
    if _is_executing_remoteley():
        # Assume we're on a cluster
        if config_name is not None:
            warnings.warn(
                "The 'config_name' parameter is used only when executing locally, "
                "and will be ignored."
            )
        token = _read_passport_token()
    else:
        # Assume we're executing locally, use the token from the config.
        if not config_name:
            raise ValueError("The config_name parameter is required for local runs.")
        config: sdk.RuntimeConfig = sdk.RuntimeConfig.load(config_name)
        token = config.token
    return token


# endregion
