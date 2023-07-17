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


def make_workspace_zri(workspace_id: str) -> str:
    """
    Make the workspace ZRI for the specified workspace ID.

    Builds project ZRI from some hardcoded values and the workspaceId based on
    https://zapatacomputing.atlassian.net/wiki/spaces/Platform/pages/512787664/2022-09-26+Zapata+Resource+Identifiers+ZRIs
    """  # noqa: E501
    default_tenant_id = 0
    special_workspace = "system"
    zri_type = "resource_group"

    return f"zri:v1::{default_tenant_id}:{special_workspace}:{zri_type}:{workspace_id}"


def make_workspace_url(resource_catalog_url: str, workspace_zri: str) -> str:
    """
    Construct the URL for a workspace based on the resource catalog and workspace ZRI.
    """
    return f"{resource_catalog_url}/api/workspaces/{workspace_zri}"
