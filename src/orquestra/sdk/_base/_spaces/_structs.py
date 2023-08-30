################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from dataclasses import dataclass

from orquestra.sdk.schema.workflow_run import ProjectId, WorkspaceId


@dataclass(frozen=True)
class ProjectRef:
    workspace_id: WorkspaceId
    project_id: ProjectId


@dataclass(frozen=True)
class Workspace:
    workspace_id: WorkspaceId
    name: str
    "Display name — doesn't have to be unique"


@dataclass(frozen=True)
class Project:
    project_id: ProjectId
    workspace_id: WorkspaceId
    name: str
    "Display name — doesn't have to be unique"
