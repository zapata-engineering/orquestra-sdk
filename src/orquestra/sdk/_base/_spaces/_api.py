################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t

from ...schema.configs import ConfigName
from ...schema.workflow_run import WorkspaceId
from .._api._config import RuntimeConfig, _resolve_config
from ._structs import Project, Workspace


def list_workspaces(
    config: t.Union[ConfigName, "RuntimeConfig"],
) -> t.Sequence[Workspace]:
    """Get the list of all workspaces available to a user.
    Warning: works only on CE runtimes

    Args:
        config: The name of the configuration to use.

    Raises:
        ConfigNameNotFoundError: when the named config is not found in the file.
    """
    # Resolve config
    resolved_config = _resolve_config(config)

    runtime = resolved_config._get_runtime()

    return runtime.list_workspaces()


def list_projects(
    config: t.Union[ConfigName, "RuntimeConfig"],
    workspace_id: t.Union[WorkspaceId, Workspace],
) -> t.Sequence[Project]:
    """Get the list of all workspaces available to a user.
    Warning: works only on CE runtimes

    Args:
        config: The name of the configuration to use.
        workspace_id: ID of the workspace to use
    Raises:
        ConfigNameNotFoundError: when the named config is not found in the file.
    """
    # Resolve config
    resolved_config = _resolve_config(config)
    resolved_workspace_id: str

    resolved_workspace_id = (
        workspace_id.workspace_id
        if isinstance(workspace_id, Workspace)
        else workspace_id
    )

    runtime = resolved_config._get_runtime()

    return runtime.list_projects(resolved_workspace_id)
