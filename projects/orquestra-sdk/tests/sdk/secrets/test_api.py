################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""
Tests for the user-facing secrets API.
"""

from typing import Callable
from unittest.mock import Mock

import pytest
from orquestra.workflow_shared import exceptions as sdk_exc
from orquestra.workflow_shared.secrets import _auth, _exceptions, _models

from orquestra import sdk


class TestIntegrationWithClient:
    """
    Assumes behavior of SecretsClient and tests how Secrets class reacts.

    Test boundary:
    [sdk.secrets.{get,set}] -> [_auth.authorized_client] -> [SecretsClient]
    """

    @staticmethod
    @pytest.fixture
    def secrets_client_mock(monkeypatch: pytest.MonkeyPatch):
        client = Mock()
        client.list_secrets.return_value = []

        monkeypatch.setattr(_auth, "authorized_client", Mock(return_value=client))

        return client

    class TestErrors:
        class TestForwardsClientExceptions:
            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    sdk_exc.UnauthorizedError(),
                ],
            )
            @pytest.mark.parametrize(
                "secrets_action",
                [
                    lambda: sdk.secrets.get(name="some-secret", workspace_id="ws"),
                    lambda: sdk.secrets.delete(name="some-secret", workspace_id="ws"),
                    lambda: sdk.secrets.list(workspace_id="ws"),
                    lambda: sdk.secrets.set(
                        name="some-secret",
                        value="You're doing great! :)",
                        workspace_id="ws",
                    ),
                ],
            )
            def test_common_errors(
                secrets_client_mock: Mock, secrets_action: Callable, exc: Exception
            ):
                secrets_client_mock.get_secret.side_effect = exc
                secrets_client_mock.delete_secret.side_effect = exc
                secrets_client_mock.list_secrets.side_effect = exc
                secrets_client_mock.create_secret.side_effect = exc

                with pytest.raises(type(exc)):
                    secrets_action()

        class TestForwardsConfigErrors:
            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    sdk_exc.ConfigNameNotFoundError(),
                ],
            )
            @pytest.mark.parametrize(
                "secrets_action",
                [
                    lambda: sdk.secrets.get(name="some-secret", workspace_id="ws"),
                    lambda: sdk.secrets.delete(name="some-secret", workspace_id="ws"),
                    lambda: sdk.secrets.list(workspace_id="ws"),
                    lambda: sdk.secrets.set(
                        name="some-secret",
                        value="You're doing great! :)",
                        workspace_id="ws",
                    ),
                ],
            )
            def test_common_errors(
                monkeypatch: pytest.MonkeyPatch,
                secrets_action: Callable,
                exc: Exception,
            ):
                monkeypatch.setattr(_auth, "authorized_client", Mock(side_effect=exc))

                with pytest.raises(type(exc)):
                    secrets_action()

    @pytest.mark.parametrize("workspace_id", ["coolest_workspace_ever"])
    class TestPassingData:
        @staticmethod
        def test_creating(secrets_client_mock: Mock, workspace_id: str):
            secret_name = "my secret?"
            secret_value = "I don't like being mean to folks"

            sdk.secrets.set(secret_name, secret_value, workspace_id=workspace_id)

            secrets_client_mock.create_secret.assert_called_with(
                _models.SecretDefinition(
                    name=secret_name, value=secret_value, resourceGroup=workspace_id
                ),
            )

        @staticmethod
        def test_overwriting_secret(secrets_client_mock: Mock, workspace_id: str):
            secret_name = "my secret?"
            secret_value = "I don't like being mean to folks"

            secrets_client_mock.create_secret.side_effect = (
                _exceptions.SecretAlreadyExistsError(secret_name)
            )

            sdk.secrets.set(secret_name, secret_value, workspace_id=workspace_id)

            expected_secret_name = (
                secret_name
                if workspace_id is None
                else f"zri:v1::0:{workspace_id}:secret:{secret_name}"
            )
            secrets_client_mock.update_secret.assert_called_with(
                expected_secret_name, secret_value
            )

        @staticmethod
        def test_listing(secrets_client_mock: Mock, workspace_id: str):
            sdk.secrets.list(workspace_id=workspace_id)

            secrets_client_mock.list_secrets.assert_called_with(workspace_id)

        @staticmethod
        def test_deleting(secrets_client_mock: Mock, workspace_id: str):
            secret_name = "my secret"

            sdk.secrets.delete(secret_name, workspace_id=workspace_id)
            expected_secret_name = (
                secret_name
                if workspace_id is None
                else f"zri:v1::0:{workspace_id}:secret:{secret_name}"
            )

            secrets_client_mock.delete_secret.assert_called_with(expected_secret_name)
