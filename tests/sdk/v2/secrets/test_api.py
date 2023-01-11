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
from orquestra.sdk._base import _exec_ctx
from orquestra.sdk.secrets import _exceptions, _models, _providers


class TestSelectsAppropriateProvider:
    """
    Verifies that the secrets.{get,set} use the proper SecretProvider implementation
    based on the 'auth_mode' and execution context.

    Test boundary:
    [sdk.secrets.{get,set}] -> [ConfigProvider]
                            -> [PassportProvider]
    """

    @staticmethod
    @pytest.fixture
    def ConfigProvider(monkeypatch):
        provider_cls = Mock()

        # Workaround for 'Mock() not iterable' inside list_secrets tests.
        provider_cls().make_client().list_secrets.return_value = []
        # We just called the mock to set it up. We need to reset the counter to enable
        # asserts on call count.
        provider_cls.reset_mock()

        monkeypatch.setattr(_providers, "ConfigProvider", provider_cls)
        return provider_cls

    @staticmethod
    @pytest.fixture
    def PassportProvider(monkeypatch):
        provider_cls = Mock()

        # Workaround for 'Mock() not iterable' inside list_secrets tests.
        provider_cls().make_client().list_secrets.return_value = []
        # We just called the mock to set it up. We need to reset the counter to enable
        # asserts on call count.
        provider_cls.reset_mock()

        monkeypatch.setattr(_providers, "PassportProvider", provider_cls)
        return provider_cls

    @staticmethod
    @pytest.fixture
    def dummy_config_name():
        return "shouldnt-matter"

    @staticmethod
    @pytest.fixture
    def secret_name():
        return "a_secret_name"

    @staticmethod
    @pytest.fixture
    def secret_value():
        return "a_secret_value"

    @staticmethod
    @pytest.fixture
    def fake_config_path(tmp_path):
        return tmp_path / "config.json"

    class TestGet:
        @staticmethod
        def test_default_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates usage from a local
            script outside of a runtime.
            """
            # When
            sdk.secrets.get(
                secret_name,
                config_name=dummy_config_name,
                config_file_path=fake_config_path,
            )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_ray_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            local runtime in Ray.
            """
            # When
            with _exec_ctx.local_ray():
                sdk.secrets.get(
                    secret_name,
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_qe_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            remote runtime with QE.
            """
            # When
            with _exec_ctx.platform_qe():
                sdk.secrets.get(
                    secret_name,
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_not_called()
            PassportProvider.assert_called()

    class TestDelete:
        @staticmethod
        def test_default_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates usage from a local
            script outside of a runtime.
            """
            # When
            sdk.secrets.delete(
                secret_name,
                config_name=dummy_config_name,
                config_file_path=fake_config_path,
            )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_ray_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            local runtime in Ray.
            """
            # When
            with _exec_ctx.local_ray():
                sdk.secrets.delete(
                    secret_name,
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_qe_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            remote runtime with QE.
            """
            # When
            with _exec_ctx.platform_qe():
                sdk.secrets.delete(
                    secret_name,
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_not_called()
            PassportProvider.assert_called()

    class TestList:
        @staticmethod
        def test_default_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates usage from a local
            script outside of a runtime.
            """
            # When
            sdk.secrets.list(
                config_name=dummy_config_name,
                config_file_path=fake_config_path,
            )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_ray_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            local runtime in Ray.
            """
            # When
            with _exec_ctx.local_ray():
                sdk.secrets.list(
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_qe_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            remote runtime with QE.
            """
            # When
            with _exec_ctx.platform_qe():
                sdk.secrets.list(
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_not_called()
            PassportProvider.assert_called()

    class TestSet:
        @staticmethod
        def test_default_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
            secret_value,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates usage from a local
            script outside of a runtime.
            """
            # When
            sdk.secrets.set(
                secret_name,
                secret_value,
                config_name=dummy_config_name,
                config_file_path=fake_config_path,
            )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_ray_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
            secret_value,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            local runtime in Ray.
            """
            # When
            with _exec_ctx.local_ray():
                sdk.secrets.set(
                    secret_name,
                    secret_value,
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_called()
            PassportProvider.assert_not_called()

        @staticmethod
        def test_qe_ctx(
            ConfigProvider,
            PassportProvider,
            dummy_config_name,
            fake_config_path,
            secret_name,
            secret_value,
        ):
            """
            Default exec context (LOCAL_DIRECT). Simulates a task running on the
            remote runtime with QE.
            """
            # When
            with _exec_ctx.platform_qe():
                sdk.secrets.set(
                    secret_name,
                    secret_value,
                    config_name=dummy_config_name,
                    config_file_path=fake_config_path,
                )

            # Then
            ConfigProvider.assert_not_called()
            PassportProvider.assert_called()


class TestIntegrationWithClient:
    """
    Assumes behavior of SecretsClient and tests how Secrets class reacts.

    Test boundary:
    [sdk.secrets.{get,set}] -> [SecretsProvider] -> [SecretsClient]
    """

    @staticmethod
    @pytest.fixture
    def secrets_client_mock(monkeypatch):
        client = Mock()
        client.list_secrets.return_value = []

        ConfigProvider = Mock()
        ConfigProvider().make_client.return_value = client
        monkeypatch.setattr(_providers, "ConfigProvider", ConfigProvider)

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
