################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from unittest.mock import MagicMock

import pytest
from orquestra.workflow_shared.schema.configs import RuntimeConfiguration, RuntimeName

from orquestra.sdk._client._base._driver import _client
from orquestra.sdk._client._base._factory import build_runtime_from_config

VALID_JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"  # noqa


@pytest.fixture
def runtime():
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": VALID_JWT_TOKEN},
    )

    return build_runtime_from_config(config=config)


@pytest.fixture
def runtime_verbose(tmp_path):
    (tmp_path / ".orquestra").mkdir(exist_ok=True)
    # Fake CE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.CE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": VALID_JWT_TOKEN},
    )
    # Return a runtime object
    return build_runtime_from_config(config=config, verbose=True)


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
