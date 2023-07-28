################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import os
from typing import Optional

from orquestra.sdk.exceptions import ProjectInvalidError
from orquestra.sdk.schema.workflow_run import ProjectId, WorkspaceId

from .._config import AUTO_CONFIG_NAME
from .._env import CURRENT_PROJECT_ENV, CURRENT_WORKSPACE_ENV
from ._structs import ProjectRef


def resolve_studio_project_ref(
    workspace_id: Optional[WorkspaceId],
    project_id: Optional[ProjectId],
) -> Optional[ProjectRef]:
    """
    Resolve the workspace and project IDs from the passed args or environment vars.
    """
    current_workspace: Optional[WorkspaceId]
    if workspace_id:
        current_workspace = workspace_id
    else:
        try:
            current_workspace = os.environ[CURRENT_WORKSPACE_ENV]
        except KeyError:
            current_workspace = None

    current_project: Optional[ProjectId]
    if project_id:
        current_project = project_id
    else:
        try:
            current_project = os.environ[CURRENT_PROJECT_ENV]
        except KeyError:
            current_project = None

    if current_project is None and current_workspace is None:
        return None
    else:
        return ProjectRef(workspace_id=current_workspace, project_id=current_project)
