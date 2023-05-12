################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pytest

import orquestra.sdk._base._spaces._resolvers as resolvers
from orquestra.sdk._base._spaces._structs import Project, ProjectRef, Workspace
from orquestra.sdk.schema.workflow_run import ProjectId, WorkspaceId


class TestGetSpaceIDs:
    @staticmethod
    @pytest.mark.parametrize(
        "project",
        [
            Project(
                "<project id sentinel>",
                "<workspace id sentinel>",
                "<project name sentinel>",
            ),
            ProjectRef("<workspace id sentinel>", "<project id sentinel>"),
        ],
    )
    def test_project_only(project):
        assert resolvers.get_space_ids(project=project, workspace=None) == (
            "<workspace id sentinel>",
            "<project id sentinel>",
        )

    @staticmethod
    @pytest.mark.parametrize(
        "workspace",
        [
            WorkspaceId("<workspace id sentinel>"),
            Workspace("<workspace id sentinel>", "<workspace name sentinel>"),
        ],
    )
    def test_workspace_only(workspace):
        assert resolvers.get_space_ids(workspace=workspace, project=None) == (
            "<workspace id sentinel>",
            None,
        )

    @staticmethod
    @pytest.mark.parametrize(
        "project",
        [
            ProjectId("<project id sentinel>"),
            Project(
                "<project id sentinel>",
                "<workspace id sentinel>",
                "<project name sentinel>",
            ),
            ProjectRef("<workspace id sentinel>", "<project id sentinel>"),
        ],
    )
    @pytest.mark.parametrize(
        "workspace",
        [
            WorkspaceId("<workspace id sentinel>"),
            Workspace("<workspace id sentinel>", "<workspace name sentinel>"),
        ],
    )
    def test_workspace_and_project(workspace, project):
        assert resolvers.get_space_ids(workspace=workspace, project=project) == (
            "<workspace id sentinel>",
            "<project id sentinel>",
        )

    @staticmethod
    @pytest.mark.parametrize(
        "project",
        [
            Project(
                "<project id sentinel>",
                "<workspace id sentinel 1>",
                "<project name sentinel>",
            ),
            ProjectRef("<workspace id sentinel 1>", "<project id sentinel>"),
        ],
    )
    @pytest.mark.parametrize(
        "workspace",
        [
            WorkspaceId("<workspace id sentinel 2>"),
            Workspace("<workspace id sentinel 2>", "<workspace name sentinel>"),
        ],
    )
    def test_workspace_id_clash(workspace, project):
        with pytest.raises(ValueError) as e:
            resolvers.get_space_ids(workspace=workspace, project=project)
        assert e.exconly() == (
            "ValueError: Mismatch in workspace ID. "
            "The workspace argument has an ID of `<workspace id sentinel 2>`, but the "
            "project argument contains the workspace ID `<workspace id sentinel 1>`."
        )

    @staticmethod
    def test_missing_workspace_id():
        with pytest.raises(ValueError) as e:
            resolvers.get_space_ids(
                project=ProjectId("<project id sentinel>"), workspace=None
            )
        assert e.exconly() == (
            "ValueError: There is insufficient information to uniquely identify a "
            "workspace and project."
        )
