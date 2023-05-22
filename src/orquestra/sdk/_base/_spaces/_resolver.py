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
    config_name: str,
) -> Optional[ProjectRef]:
    # Passed explicitly
    if workspace_id or project_id:
        if workspace_id is None or project_id is None:
            raise ProjectInvalidError(
                "Invalid project ID. Either explicitly pass workspace_id "
                "and project_id, or omit both"
            )
        return ProjectRef(workspace_id=workspace_id, project_id=project_id)

    # Infer workspace and project from studio ONLY when using "auto" config-name
    if config_name != AUTO_CONFIG_NAME:
        return None

    # Currently no way to figure out workspace and projects without env vars
    if CURRENT_PROJECT_ENV not in os.environ or CURRENT_WORKSPACE_ENV not in os.environ:
        return None

    current_workspace = os.environ[CURRENT_WORKSPACE_ENV]
    current_project = os.environ[CURRENT_PROJECT_ENV]
    return ProjectRef(workspace_id=current_workspace, project_id=current_project)
