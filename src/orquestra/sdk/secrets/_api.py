################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for user-facing utilities related to secrets.
"""
import typing as t
from pathlib import Path

from .. import exceptions as sdk_exc
from .._base import _exec_ctx
from . import _exceptions, _models, _providers


def _infer_secrets_provider() -> _providers.SecretsProvider:
    ctx = _exec_ctx.global_context
    if ctx == _exec_ctx.ExecContext.LOCAL_DIRECT:
        return _providers.ConfigProvider()
    elif ctx == _exec_ctx.ExecContext.LOCAL_RAY:
        return _providers.ConfigProvider()
    elif ctx == _exec_ctx.ExecContext.PLATFORM_QE:
        return _providers.PassportProvider()
    else:
        raise RuntimeError(f"Can't infer auth mode for exec context {ctx}")


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
            Required when used from a local machine. Ignored when running on Orquestra
            Platform.
        config_save_path: custom path of the config file. If omitted, uses the
            standard config location. Ignored when running on Orquestra Platform.

    Raises:
        orquestra.sdk.exceptions.NotFoundError: when no secret with the given name
            was found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    provider = _infer_secrets_provider()
    client = provider.make_client(
        config_name=config_name,
    )
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
            Required when used from a local machine. Ignored when running on Orquestra
            Platform.
        config_save_path: custom path of the config file. If omitted, uses the
            standard config location. Ignored when running on Orquestra Platform.

    Raises:
        orquestra.sdk.exceptions.NotFoundError: when no secret with the given name
            was found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    provider = _infer_secrets_provider()
    client = provider.make_client(
        config_name=config_name,
    )
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
            Required when used from a local machine. Ignored when running on Orquestra
            Platform.
        config_save_path: custom path of the config file. If omitted, uses the
            standard config location. Ignored when running on Orquestra Platform.

    Raises:
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    provider = _infer_secrets_provider()
    client = provider.make_client(
        config_name=config_name,
    )
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
            Required when used from a local machine. Ignored when running on Orquestra
            Platform.
        config_save_path: custom path of the config file. If omitted, uses the
            standard config location. Ignored when running on Orquestra Platform.

    Raises:
        orquestra.sdk.exceptions.NotFoundError: when the secret ``name`` couldn't be
            found.
        orquestra.sdk.exceptions.UnauthorizedError: when the authorization with the
            remote vault failed.
    """
    provider = _infer_secrets_provider()
    client = provider.make_client(
        config_name=config_name,
    )
    try:
        client.delete_secret(name)
    # explicit rethrows of known errors
    except _exceptions.SecretNotFoundError as e:
        raise sdk_exc.NotFoundError(f"Secret {name} not found") from e
    except _exceptions.InvalidTokenError as e:
        raise sdk_exc.UnauthorizedError() from e
