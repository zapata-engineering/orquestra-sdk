################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from orquestra.sdk._base._driver import _client
from orquestra.sdk._base._factory import build_runtime_from_config
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName


@pytest.fixture
def runtime():
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )

    return build_runtime_from_config(project_dir=Path.cwd(), config=config)


@pytest.fixture
def runtime_verbose(tmp_path):
    (tmp_path / ".orquestra").mkdir(exist_ok=True)
    # Fake CE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return build_runtime_from_config(
        project_dir=Path.cwd(), config=config, verbose=True
    )


@pytest.fixture
def mocked_client(monkeypatch: pytest.MonkeyPatch):
    mocked_client = MagicMock(spec=_client.DriverClient)
    mocked_client.from_token.return_value = mocked_client
    monkeypatch.setattr(
        "orquestra.sdk._base._driver._client.DriverClient", mocked_client
    )
    return mocked_client


@pytest.fixture
def workflow_def_id():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def workflow_run_id():
    return "00000000-0000-0000-0000-000000000000"
