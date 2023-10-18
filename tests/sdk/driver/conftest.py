################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from unittest.mock import MagicMock

import pytest

from orquestra.sdk._base._api import RuntimeConfig
from orquestra.sdk._base._api.repos import RuntimeRepo
from orquestra.sdk._base._driver import _client
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName


@pytest.fixture
def runtime(mock_workflow_db_location):
    config_model = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    config = RuntimeConfig._config_from_runtimeconfiguration(config_model)

    return RuntimeRepo().get_runtime(config)


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
