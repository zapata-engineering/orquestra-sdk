################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import os

import pytest

from orquestra.sdk._client._base._testing import _connections


@pytest.fixture(scope="module")
def change_test_dir(tmp_path_factory, request):
    project_dir = tmp_path_factory.mktemp("project")
    os.chdir(project_dir)
    yield project_dir
    os.chdir(request.config.invocation_dir)


@pytest.fixture(scope="module")
def shared_ray_conn():
    with _connections.make_ray_conn() as ray_params:
        yield ray_params
