################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import os
from typing import Optional

from orquestra.workflow_shared import ProjectRef
from orquestra.workflow_shared.exceptions import ProjectInvalidError
from orquestra.workflow_shared.schema.workflow_run import ProjectId, WorkspaceId

from .._env import CURRENT_PROJECT_ENV, CURRENT_WORKSPACE_ENV


def resolve_studio_ref(
    workspace_id: Optional[WorkspaceId],
    project_id: Optional[ProjectId],
) -> Optional[ProjectRef]:
    """Resolve the workspace and project IDs from the passed args or environment vars.

    Args:
        workspace_id: ID of the workspace for workflow - supported only on CE.
        project_id: ID of the project for workflow - supported only on CE.

    Raises:
        orquestra.sdk.exceptions.ProjectInvalidError: when one but not both of the
            workspace and project id arguments are specified - this is insufficient
            information to uniquely identify a project.
    """
    current_workspace = resolve_studio_workspace_ref(workspace_id)

    current_project = resolve_studio_project_ref(project_id)

    if current_project is None and current_workspace is None:
        return None
    elif current_project is None or current_workspace is None:
        raise ProjectInvalidError(
            "Invalid project ID. Either explicitly pass workspace_id "
            "and project_id, or omit both"
        )
    else:
        return ProjectRef(workspace_id=current_workspace, project_id=current_project)


def resolve_studio_workspace_ref(
    workspace_id: Optional[WorkspaceId],
) -> Optional[WorkspaceId]:
    current_workspace: Optional[WorkspaceId]
    if workspace_id:
        current_workspace = workspace_id
    else:
        try:
            current_workspace = os.environ[CURRENT_WORKSPACE_ENV]
        except KeyError:
            current_workspace = None
    return current_workspace


def resolve_studio_project_ref(
    project_id: Optional[ProjectId],
):
    current_project: Optional[ProjectId]
    if project_id:
        current_project = project_id
    else:
        try:
            current_project = os.environ[CURRENT_PROJECT_ENV]
        except KeyError:
            current_project = None
    return current_project
