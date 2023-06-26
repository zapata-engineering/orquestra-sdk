################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for user-facing utilities related to secrets.
"""
import typing as t
import warnings

from .. import exceptions as sdk_exc
from .._base import _dsl, _exec_ctx
from ..schema.workflow_run import WorkspaceId
from . import _auth, _exceptions, _models


def _translate_to_zri(workspace_id: WorkspaceId, secret_name: str) -> str:
    """
    Create ZRI from workspace_id and secret_name
    """
    return f"zri:v1::0:{workspace_id}:secret:{secret_name}"


def _raise_warning_for_none_workspace(workspace: t.Optional[WorkspaceId]):
    if workspace is None:
        warnings.warn(
            "Please specify workspace ID directly for accessing secrets."
            " Support for default workspaces will be sunset in the future.",
            FutureWarning,
        )


def get(
    name: str,
    *,
    workspace_id: t.Optional[str] = None,
    config_name: t.Optional[str] = None,
) -> str:
    """
    Retrieves secret value from the remote vault.

    Args:
        name: secret identifier.
        workspace_id: ID of the workspace. Using platform-defined default if omitted -
            - currently it is personal workspace
        config_name: config entry to use to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when
            ORQUESTRA_PASSPORT_FILE env variable is set.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no matching config was
            found.
        orquestra.sdk.exceptions.NotFoundError: when no secret with the given name
            was found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.

    Returns:
        Either:
        - the value of the secret
        - if used inside a workflow function (a function decorated with @sdk.workflow),
            this function will return a "future" which will be used to retrieve the
            secret at execution time.
    """
    _raise_warning_for_none_workspace(workspace_id)

    if _exec_ctx.global_context == _exec_ctx.ExecContext.WORKFLOW_BUILD:
        return t.cast(
            str,
            _dsl.Secret(name=name, config_name=config_name, workspace_id=workspace_id),
        )

    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    if workspace_id:
        name = _translate_to_zri(workspace_id, name)

    try:
        return client.get_secret(name).value
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e
    except _exceptions.SecretNotFoundError as e:
        raise sdk_exc.NotFoundError(f"Couldn't find secret named {name}") from e


def list(
    *,
    workspace_id: t.Optional[str] = None,
    config_name: t.Optional[str] = None,
) -> t.Sequence[str]:
    """
    Lists all secret names.

    Args:
        workspace_id: ID of the workspace. Using platform-defined default if omitted -
            - currently it is personal workspace
        config_name: config entry to use to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when
            ORQUESTRA_PASSPORT_FILE env variable is set.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no matching config was
            found.
        orquestra.sdk.exceptions.NotFoundError: when no secret with the given name
            was found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    _raise_warning_for_none_workspace(workspace_id)

    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    try:
        return [obj.name for obj in client.list_secrets(workspace_id)]
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e


def set(
    name: str,
    value: str,
    *,
    workspace_id: t.Optional[str] = None,
    config_name: t.Optional[str] = None,
):
    """
    Sets secret value at the remote vault. Overwrites already existing secrets.

    Args:
        name: secret identifier.
        value: new secret name.
        workspace_id: workspace in which secret will be created. Using platform-defined
            default if omitted - currently it is personal workspace
        config_name: config entry to use to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when
            ORQUESTRA_PASSPORT_FILE env variable is set.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no matching config was
            found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    _raise_warning_for_none_workspace(workspace_id)

    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    try:
        try:
            client.create_secret(
                _models.SecretDefinition(
                    name=name, value=value, resourceGroup=workspace_id
                )
            )
        except _exceptions.SecretAlreadyExistsError:
            if workspace_id:
                name = _translate_to_zri(workspace_id, name)
            client.update_secret(name, value)
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e


def delete(
    name: str,
    *,
    workspace_id: t.Optional[str] = None,
    config_name: t.Optional[str] = None,
):
    """
    Deletes secret from the remote vault.

    Args:
        name: secret identifier.
        workspace_id: ID of the workspace. Using platform-defined default if omitted -
            - currently it is personal workspace
        config_name: config entry to use to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when
            ORQUESTRA_PASSPORT_FILE env variable is set.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no matching config was
            found.
        orquestra.sdk.exceptions.NotFoundError: when the secret ``name`` couldn't be
            found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    _raise_warning_for_none_workspace(workspace_id)

    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise
    if workspace_id:
        name = _translate_to_zri(workspace_id, name)
    try:
        client.delete_secret(name)
    # explicit rethrows of known errors
    except _exceptions.SecretNotFoundError as e:
        raise sdk_exc.NotFoundError(f"Secret {name} not found") from e
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e
