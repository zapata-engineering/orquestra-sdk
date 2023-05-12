################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Tools for handling combinations of workspace and project.
"""

import typing as t

from orquestra.sdk.schema.workflow_run import ProjectId, WorkspaceId

from .._spaces._structs import Project, ProjectRef, Workspace


def get_space_ids(
    workspace: t.Optional[t.Union[Workspace, WorkspaceId]],
    project: t.Optional[t.Union[Project, ProjectRef, ProjectId]],
) -> t.Tuple[t.Optional[WorkspaceId], t.Optional[ProjectId]]:
    """
    Take any combination of project and workspace identifiers and extract the IDs.

    Args:
        workspace: _description_
        project: _description_

    Returns:
        _description_

    Raises:
        ValueError: When a project object that includes a workspace ID is specified
            alongside a workspace object, but the workspace IDs do not match.
        ValueError: when a project object that does not include a workspace ID is
            specified without an acompanying workspace.
    """
    _workspace_id = None
    _project_id = None
    _project_ref_workspace_id = None

    if workspace:
        if isinstance(workspace, WorkspaceId):
            _workspace_id = workspace
        elif isinstance(workspace, Workspace):
            _workspace_id = workspace.workspace_id
    if project:
        if isinstance(project, Project) or isinstance(project, ProjectRef):
            _project_id = project.project_id
            _project_ref_workspace_id = project.workspace_id
        elif isinstance(project, ProjectId):
            _project_id = project

    # Check for mismatch between a specified workspace ID, and one contained in the
    # Project argument.
    if (
        _workspace_id
        and _project_ref_workspace_id
        and _workspace_id != _project_ref_workspace_id
    ):
        raise ValueError(
            "Mismatch in workspace ID. "
            f"The workspace argument has an ID of `{_workspace_id}`, but the "
            "project argument contains the workspace ID "
            f"`{_project_ref_workspace_id}`."
        )
    # Check for a project ID without an acompanying workspace ID
    if _project_id and not _workspace_id:
        raise ValueError(
            "There is insufficient information to uniquely identify a workspace and "
            "project."
        )
    return _workspace_id, _project_id
