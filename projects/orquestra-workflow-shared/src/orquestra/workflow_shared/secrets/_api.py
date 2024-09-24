################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""Code for user-facing utilities related to secrets."""
import typing as t
from typing import NamedTuple

from orquestra.workflow_shared.exceptions import (
    ConfigNameNotFoundError,
    NotFoundError,
    UnauthorizedError,
)
from orquestra.workflow_shared.exec_ctx import ExecContext, get_current_exec_context
from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.schema.workflow_run import WorkspaceId

from . import _auth, _exceptions, _models


def _translate_to_zri(workspace_id: WorkspaceId, secret_name: str) -> str:
    """Create ZRI from workspace_id and secret_name."""
    return f"zri:v1::0:{workspace_id}:secret:{secret_name}"


_secret_as_string_error = (
    "Invalid usage of a Secret object. Secrets are not "
    "available when building the workflow graph and cannot"
    " be used as strings. If you need to use a Secret's"
    " value, this must be done inside of a task."
)


class Secret(NamedTuple):
    name: str
    # Config name is only used for the local runtimes where we can't infer the location
    # where we get a secret's value from.
    # This matches the behaviour of `sdk.secrets.get` where the config name is used to
    # get a secret when running locally.
    config_name: t.Optional[str] = None
    # Workspace ID is used by local and remote runtimes to fetch a secret from a
    # specific workspace.
    workspace_id: t.Optional[str] = None

    def __reduce__(self) -> t.Union[str, tuple[t.Any, ...]]:
        # We need to override the pickling behaviour for Secret
        # This is because we override other dunder methods which cause the normal
        # picling behaviour to fail.
        return (self.__class__, (self.name, self.config_name, self.workspace_id))

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError as e:
            raise AttributeError(_secret_as_string_error) from e

    def __getitem__(self, item):
        raise AttributeError(_secret_as_string_error)

    def __str__(self):
        raise AttributeError(_secret_as_string_error)

    def __iter__(self):
        raise AttributeError(_secret_as_string_error)


def get(
    name: str,
    *,
    workspace_id: t.Optional[WorkspaceId] = None,
    config_name: t.Optional[ConfigName] = None,
) -> str:
    """Retrieves secret value from the remote vault.

    For more information about the supported config/runtime names, visit the
    `Runtime Configuration guide
    <https://docs.orquestra.io/docs/core/sdk/guides/runtime-configuration.html>`_.

    Args:
        name: secret identifier.
        workspace_id: ID of the workspace. Using platform-defined default if omitted -
            - currently it is personal workspace
        config_name: name of the runtime used to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when already running on a
            remote Orquestra cluster.

    Raises:
        ConfigNameNotFoundError: when no matching config was found.
        NotFoundError: when no secret with the given name was found.
        UnauthorizedError: when the authorization with the remote vault failed.

    Returns:
        Either:
        - the value of the secret
        - if used inside a workflow function (a function decorated with @sdk.workflow),
            this function will return a "future" which will be used to retrieve the
            secret at execution time.
    """
    if get_current_exec_context() == ExecContext.WORKFLOW_BUILD:
        return t.cast(
            str,
            Secret(name=name, config_name=config_name, workspace_id=workspace_id),
        )

    try:
        client = _auth.authorized_client(config_name)
    except ConfigNameNotFoundError:
        raise

    if workspace_id:
        name = _translate_to_zri(workspace_id, name)

    try:
        return client.get_secret(name).value
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise UnauthorizedError() from e
    except _exceptions.SecretNotFoundError as e:
        raise NotFoundError(f"Couldn't find secret named {name}") from e


def list(
    *,
    workspace_id: t.Optional[WorkspaceId] = None,
    config_name: t.Optional[ConfigName] = None,
) -> t.Sequence[str]:
    """Lists all secret names.

    For more information about the supported config/runtime names, visit the
    `Runtime Configuration guide
    <https://docs.orquestra.io/docs/core/sdk/guides/runtime-configuration.html>`_.

    Args:
        workspace_id: ID of the workspace. Using platform-defined default if omitted -
            - currently it is personal workspace.
        config_name: name of the runtime used to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when already running on a
            remote Orquestra cluster.

    Raises:
        ConfigNameNotFoundError: when no matching config was found.
        UnauthorizedError: when the authorization with the remote vault failed.
    """
    try:
        client = _auth.authorized_client(config_name)
    except ConfigNameNotFoundError:
        raise

    try:
        return [obj.name for obj in client.list_secrets(workspace_id)]
    # explicit rethrows of known errors
    except _exceptions.InvalidTokenError as e:
        raise UnauthorizedError() from e


def set(
    name: str,
    value: str,
    *,
    workspace_id: t.Optional[WorkspaceId] = None,
    config_name: t.Optional[ConfigName] = None,
):
    """Sets secret value at the remote vault. Overwrites already existing secrets.

    For more information about the supported config/runtime names, visit the
    `Runtime Configuration guide
    <https://docs.orquestra.io/docs/core/sdk/guides/runtime-configuration.html>`_.

    Args:
        name: secret identifier.
        value: new secret name.
        workspace_id: workspace in which secret will be created. Using platform-defined
            default if omitted - currently it is personal workspace.
        config_name: name of the runtime used to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when already running on a
            remote Orquestra cluster.

    Raises:
        ConfigNameNotFoundError: when no matching config was found.
        UnauthorizedError: when the authorization with the remote vault failed.
    """
    try:
        client = _auth.authorized_client(config_name)
    except ConfigNameNotFoundError:
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
        raise UnauthorizedError() from e


def delete(
    name: str,
    *,
    workspace_id: t.Optional[WorkspaceId] = None,
    config_name: t.Optional[ConfigName] = None,
):
    """Deletes secret from the remote vault.

    For more information about the supported config/runtime names, visit the
    `Runtime Configuration guide
    <https://docs.orquestra.io/docs/core/sdk/guides/runtime-configuration.html>`_.

    Args:
        name: secret identifier.
        workspace_id: ID of the workspace. Using platform-defined default if omitted -
            - currently it is personal workspace.
        config_name: name of the runtime used to communicate with Orquestra Platform.
            Required when used from a local machine. Ignored when already running on a
            remote Orquestra cluster.

    Raises:
        ConfigNameNotFoundError: when no matching config was found.
        NotFoundError: when the secret ``name`` couldn't be found.
        UnauthorizedError: when the authorization with the remote vault failed.
    """
    try:
        client = _auth.authorized_client(config_name)
    except ConfigNameNotFoundError:
        raise
    if workspace_id:
        name = _translate_to_zri(workspace_id, name)
    try:
        client.delete_secret(name)
    # explicit rethrows of known errors
    except _exceptions.SecretNotFoundError as e:
        raise NotFoundError(f"Secret {name} not found") from e
    except _exceptions.InvalidTokenError as e:
        raise UnauthorizedError() from e
