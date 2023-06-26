import typing as t

import pytest

from orquestra.sdk._base._env import CURRENT_PROJECT_ENV, CURRENT_WORKSPACE_ENV
from orquestra.sdk._base._spaces._resolver import resolve_studio_project_ref
from orquestra.sdk._base._spaces._structs import ProjectRef


@pytest.mark.parametrize(
    "ws, proj, env_ws, env_proj, expected",
    [
        ("ws", "proj", None, None, ProjectRef("ws", "proj")),
        ("ws", "proj", "w/e", "w/e", ProjectRef("ws", "proj")),
        (None, None, None, None, None),
        (None, None, "my_ws", "my_proj", ProjectRef("my_ws", "my_proj")),
        (None, None, "my_ws", None, None),
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

    assert expected == resolve_studio_project_ref(
        workspace_id=ws,
        project_id=proj,
    )
