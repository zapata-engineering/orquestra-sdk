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
    # Passed explicitly
    if workspace_id and project_id:
        return ProjectRef(workspace_id=workspace_id, project_id=project_id)
    # passed explicitly only 1 value. Invalid entry
    elif workspace_id or project_id:
        raise ProjectInvalidError(
            "Invalid project ID. Either explicitly pass workspace_id "
            "and project_id, or omit both"
        )

    # Currently no way to figure out workspace and projects without env vars
    try:
        current_workspace = os.environ[CURRENT_WORKSPACE_ENV]
        current_project = os.environ[CURRENT_PROJECT_ENV]
    except KeyError:
        return None

    return ProjectRef(workspace_id=current_workspace, project_id=current_project)
