################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import typing as t

import pytest

from orquestra.sdk._client._base._env import CURRENT_PROJECT_ENV, CURRENT_WORKSPACE_ENV
from orquestra.sdk._client._base._spaces._api import (
    make_workspace_url,
    make_workspace_zri,
)
from orquestra.sdk._client._base._spaces._resolver import resolve_studio_ref
from orquestra.sdk._shared._spaces._structs import ProjectRef


@pytest.mark.parametrize(
    "ws, proj, env_ws, env_proj, expected",
    [
        ("ws", "proj", None, None, ProjectRef("ws", "proj")),
        ("ws", "proj", "w/e", "w/e", ProjectRef("ws", "proj")),
        (None, None, None, None, None),
        (None, None, "my_ws", "my_proj", ProjectRef("my_ws", "my_proj")),
    ],
)
def test_studio_resolver(
    monkeypatch,
    ws,
    proj,
    env_ws: t.Optional[str],
    env_proj: t.Optional[str],
    expected,
):
    if env_ws:
        monkeypatch.setenv(CURRENT_WORKSPACE_ENV, env_ws)
    if env_proj:
        monkeypatch.setenv(CURRENT_PROJECT_ENV, env_proj)

    assert expected == resolve_studio_ref(
        workspace_id=ws,
        project_id=proj,
    )


class TestMakeWorkspaceZRI:
    @staticmethod
    def test_string_construction():
        # When
        zri = make_workspace_zri("<workspace id sentinel>")

        # Then
        assert zri == "zri:v1::0:system:resource_group:<workspace id sentinel>"


class TestMakeWorkspaceURL:
    @staticmethod
    def test_string_construction():
        # When
        url = make_workspace_url(
            "<resource catalog url sentinel>", "<workspace zri sentinel>"
        )

        # Then
        assert (
            url
            == "<resource catalog url sentinel>/api/workspaces/<workspace zri sentinel>"
        )
