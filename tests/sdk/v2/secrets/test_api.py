################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Tests for the user-facing secrets API.
"""

from unittest.mock import Mock

import pytest

from orquestra import sdk
from orquestra.sdk import exceptions as sdk_exc
from orquestra.sdk.secrets import _auth, _exceptions, _models


class TestIntegrationWithClient:
    """
    Assumes behavior of SecretsClient and tests how Secrets class reacts.

    Test boundary:
    [sdk.secrets.{get,set}] -> [_auth.authorized_client] -> [SecretsClient]
    """

    @staticmethod
    @pytest.fixture
    def secrets_client_mock(monkeypatch):
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
                    lambda: sdk.secrets.get(name="some-secret"),
                    lambda: sdk.secrets.delete(name="some-secret"),
                    lambda: sdk.secrets.list(),
                    lambda: sdk.secrets.set(
                        name="some-secret",
                        value="You're doing great! :)",
                    ),
                ],
            )
            def test_common_errors(secrets_client_mock, secrets_action, exc):
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
                    lambda: sdk.secrets.get(name="some-secret"),
                    lambda: sdk.secrets.delete(name="some-secret"),
                    lambda: sdk.secrets.list(),
                    lambda: sdk.secrets.set(
                        name="some-secret",
                        value="You're doing great! :)",
                    ),
                ],
            )
            def test_common_errors(monkeypatch, secrets_action, exc):
                monkeypatch.setattr(_auth, "authorized_client", Mock(side_effect=exc))

                with pytest.raises(type(exc)):
                    secrets_action()

    class TestPassingData:
        @staticmethod
        def test_creating(secrets_client_mock):
            secret_name = "my secret?"
            secret_value = "I don't like being mean to folks"

            sdk.secrets.set(secret_name, secret_value)

            secrets_client_mock.create_secret.assert_called_with(
                _models.SecretDefinition(
                    name=secret_name,
                    value=secret_value,
                ),
            )

        @staticmethod
        def test_overwriting_secret(secrets_client_mock):
            secret_name = "my secret?"
            secret_value = "I don't like being mean to folks"

            secrets_client_mock.create_secret.side_effect = (
                _exceptions.SecretAlreadyExistsError(secret_name)
            )

            sdk.secrets.set(secret_name, secret_value)

            secrets_client_mock.update_secret.assert_called_with(
                secret_name, secret_value
            )

        @staticmethod
        def test_listing(secrets_client_mock):
            sdk.secrets.list()

            secrets_client_mock.list_secrets.assert_called()

        @staticmethod
        def test_deleting(secrets_client_mock):
            secret_name = "my secret"

            sdk.secrets.delete(secret_name)

            secrets_client_mock.delete_secret.assert_called_with(secret_name)
