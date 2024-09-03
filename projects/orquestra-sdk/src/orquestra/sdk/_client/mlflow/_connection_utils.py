################################################################################
# © Copyright 2023 - 2024 Zapata Computing Inc.
################################################################################

"""Utilities for communicating with mlflow."""

import os
import typing as t
import warnings
from pathlib import Path
from typing import Optional, Tuple

from orquestra.workflow_shared.env import PASSPORT_FILE_ENV
from orquestra.workflow_shared.exceptions import (
    ConfigNameNotFoundError,
    RuntimeConfigError,
)
from requests import Response, Session

from orquestra import sdk

from .._base import _env
from .._base._config import get_config_option
from .._base._env import CURRENT_USER_ENV
from .._base._jwt import get_email_from_jwt_token
from .._base._services import ORQUESTRA_BASE_PATH
from .._base._spaces._api import make_workspace_url, make_workspace_zri

DEFAULT_TEMP_ARTIFACTS_DIR: Path = ORQUESTRA_BASE_PATH / "mlflow" / "artifacts"
RESOURCE_CATALOG_URI: str = "http://orquestra-resource-catalog.resource-catalog"


# region: private
def _is_executing_remoteley() -> bool:
    """Determine whether the code is being executed locally, or on a cluster/studio."""
    if os.getenv(_env.CURRENT_CLUSTER_ENV):
        return True
    return False


def _get_mlflow_cr_name_and_port() -> Tuple[str, str]:
    """Reads in the MLFlow custom resource name and port from environment variables.

    The environment variables are:
    - ORQ_MLFLOW_CR_NAME
    - ORQ_MLFLOW_PORT

    Example usage::

        mlflow_cr_name, mlflow_port = _get_mlflow_cr_name_and_port()

    Raises:
        EnvironmentError: when either of the environment variables are not set.
    """
    assert (
        _is_executing_remoteley()
    ), "This function should not be called when running locally."

    if not (cr_name := os.getenv(_env.MLFLOW_CR_NAME)):
        raise EnvironmentError(
            f"The '{_env.MLFLOW_CR_NAME}' environment variable is not set."
        )
    if not (port := os.getenv(_env.MLFLOW_PORT)):
        raise EnvironmentError(
            f"The '{_env.MLFLOW_PORT}' environment variable is not set."
        )

    return cr_name, port


def _read_passport_token() -> str:
    """Reads in the token.

    The file path is specified by the PASSPOT_FILE_ENV environment variable.

    Raises:
        EnvironmentError: when the PASSPORT_FILE_ENV environment variable is not set.
    """
    if not (passport_file_path := os.getenv(PASSPORT_FILE_ENV)):
        raise EnvironmentError(
            f"The '{PASSPORT_FILE_ENV}' environment variable is not set."
        )
    return Path(passport_file_path).read_text()


def _make_session(token) -> Session:
    """Create a HTTP session with the specified token."""
    session = Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Authorization"] = f"Bearer {token}"
    return session


# endregion

# region: public


def get_temp_artifacts_dir() -> Path:
    """Return a path to a directory that can be used to temporarily store artifacts.

    Uploading artifacts to MLflow requires them to be written locally first.
    Finding an appropriate directory vary significantly between a workflow running
    locally and one running on a remote cluster.
    This function handles that complexity so that workflows do not need adjusting
    between runtimes.
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
    """Infer a URI for accessing MLflow tracking server deployed within this workspace.

    When run within an Orquestra cluster, this function returns an "internal URI" that
    helps save cluster bandwidth. This works even without specifying ``config_name``.

    When run outside a cluster, this function returns an "external URI". Requires
    passing ``config_name``.

    Args:
        workspace_id: ID of the workspace. You can get it from Orquestra Portal UI.
        config_name: The name of a stored configuration specifying the execution
            environment. If this function is being executed remotely, this parameter
            can be omitted.

    Raises:
        EnvironmentError: When one or more environment variables required for
            constructing the URI are not set.
        ValueError: When this function is called in a local execution context without
            specifying a valid non-local config name.
    """
    if _is_executing_remoteley():
        if config_name is not None:
            warnings.warn(
                "The 'config_name' parameter is used only when executing locally, "
                "and will be ignored."
            )

        try:
            token: str = _read_passport_token()
            session: Session = _make_session(token)
            workspace_zri: str = make_workspace_zri(workspace_id)
            workspace_url: str = make_workspace_url(RESOURCE_CATALOG_URI, workspace_zri)

            resp: Response = session.get(workspace_url)
            resp.raise_for_status()
            namespace: str = resp.json()["namespace"]
            mlflow_cr_name, mlflow_port = _get_mlflow_cr_name_and_port()
        except EnvironmentError:
            raise

        return f"http://{mlflow_cr_name}.{namespace}:{mlflow_port}"
    else:
        # Assume we're executing locally, use external URIs
        if not config_name:
            raise ValueError("The config_name parameter is required for local runs.")

        config = sdk.RuntimeConfig.load(config_name)
        try:
            uri = getattr(config, "uri")
            return f"{uri}/mlflow/{workspace_id}"
        except AttributeError as e:
            raise ValueError(
                f"The config '{config_name}' has no URI associated with it. "
                "In order to determine the tracking URI, the config must specify a "
                "remote execution environment where the MLFlow instance is hosted. "
                "Please double-check the config name and try again."
            ) from e


def get_tracking_token(config_name: Optional[str] = None) -> str:
    """
    Get a token suitable for authorizing the MLflow client for accessing Orquestra's
    MLflow tracking server.

    Args:
        config_name: The name of a stored configuration specifying the execution
            environment. If this function is being executed remotely, this parameter
            can be omitted.

    Raises:
        ValueError: When this function is called without a config name in a local
            execution context.
    """  # noqa: D205, D212
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


def get_current_user(config_name: t.Optional[str]) -> str:
    """Return current user that can be used to power MLFlow UI label.

    When used inside a studio or CE task, returns actively logged-in user.
    When used locally, uses token stored inside config to figure out what username
    was used during login.

    Args:
        config_name: config entry to use to deduce username. Ignored when used
            in studio or inside task executed on CE runtime.

    Raises:
        ConfigNameNotFoundError: when no matching config was
            found or when called locally without config parameter
        RuntimeConfigError: when chosen config does not
            contain token runtime option
    """
    if CURRENT_USER_ENV in os.environ:
        return os.environ[CURRENT_USER_ENV]

    if config_name is None:
        raise ConfigNameNotFoundError("Unable to get current user without config.")

    try:
        token = get_config_option(config_name, "token")
    except RuntimeConfigError:
        raise RuntimeConfigError(
            "Selected config does not have remote token configured. "
            "Did you log in with it?"
        )

    return get_email_from_jwt_token(token)


# endregion
