################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for user-facing utilities related to secrets.
"""
import typing as t

from .. import exceptions as sdk_exc
from .._base import _dsl, _exec_ctx
from . import _auth, _exceptions, _models


def get(
    name: str,
    *,
    config_name: t.Optional[str] = None,
) -> str:
    """
    Retrieves secret value from the remote vault.

    Args:
        name: secret identifier.
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
    if _exec_ctx.global_context == _exec_ctx.ExecContext.WORKFLOW_BUILD:
        return t.cast(str, _dsl.Secret(name=name, config_name=config_name))

    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    try:
        return client.get_secret(name).value
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e
    except _exceptions.SecretNotFoundError as e:
        raise sdk_exc.NotFoundError(f"Couldn't find secret named {name}") from e


def list(
    *,
    config_name: t.Optional[str] = None,
) -> t.Sequence[str]:
    """
    Lists all secret names.

    Args:
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
    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    try:
        return [obj.name for obj in client.list_secrets()]
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e


def set(
    name: str,
    value: str,
    *,
    config_name: t.Optional[str] = None,
):
    """
    Sets secret value at the remote vault. Overwrites already existing secrets.

    Args:
        name: secret identifier.
        value: new secret name.
        config_name: config entry to use to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when
            ORQUESTRA_PASSPORT_FILE env variable is set.

    Raises:
        orquestra.sdk.exceptions.ConfigNameNotFoundError: when no matching config was
            found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    try:
        try:
            client.create_secret(_models.SecretDefinition(name=name, value=value))
        except _exceptions.SecretAlreadyExistsError:
            client.update_secret(name, value)
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e


def delete(
    name: str,
    *,
    config_name: t.Optional[str] = None,
):
    """
    Deletes secret from the remote vault.

    Args:
        name: secret identifier.
        value: new secret name.
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
    try:
        client = _auth.authorized_client(config_name)
    except sdk_exc.ConfigNameNotFoundError:
        raise

    try:
        client.delete_secret(name)
    # explicit rethrows of known errors
    except _exceptions.SecretNotFoundError as e:
        raise sdk_exc.NotFoundError(f"Secret {name} not found") from e
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e
