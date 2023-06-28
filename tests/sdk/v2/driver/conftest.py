from unittest.mock import MagicMock

import pytest

from orquestra.sdk._base._driver import _ce_runtime, _client, _exceptions, _models
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName


@pytest.fixture
def mocked_client(monkeypatch: pytest.MonkeyPatch):
    mocked_client = MagicMock(spec=_client.DriverClient)
    mocked_client.from_token.return_value = mocked_client
    monkeypatch.setattr(
        "orquestra.sdk._base._driver._client.DriverClient", mocked_client
    )
    return mocked_client


@pytest.fixture
def runtime(mock_workflow_db_location, mocked_client):
    # Fake CE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return _ce_runtime.CERuntime(config, client=mocked_client)


@pytest.fixture
def runtime_verbose(tmp_path, mocked_client):
    (tmp_path / ".orquestra").mkdir(exist_ok=True)
    # Fake QE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return _ce_runtime.CERuntime(
        config,
        client=mocked_client,
        verbose=True,
    )


@pytest.fixture
def workflow_def_id():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def workflow_run_id():
    return "00000000-0000-0000-0000-000000000000"
