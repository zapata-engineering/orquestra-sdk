################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for secrets access authorization.
"""

import json
from pathlib import Path

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk.schema import configs
from orquestra.sdk.secrets import _auth


class TestAuthorizedClient:
    class TestPassportAuth:
        """
        When there's passport env variable.
        """

        @staticmethod
        @pytest.fixture
        def passport_token():
            return "passport-token"

        @staticmethod
        @pytest.fixture
        def passport_file(tmp_path, passport_token: str):
            passport_path = tmp_path / "passport.txt"
            passport_path.write_text(passport_token)

            try:
                yield passport_path
            finally:
                passport_path.unlink()

        @staticmethod
        @pytest.mark.parametrize("config_name", [None, "some-config"])
        def test_uses_passport_token(
            monkeypatch, config_name, passport_token: str, passport_file: Path
        ):
            """
            It should use the passport token regardless of passed ``config_name``.
            """
            # Given
            monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(passport_file))

            # When
            client = _auth.authorized_client(config_name=config_name)

            # Then
            assert client._base_uri == "http://config-service.config-service:8099"
            assert (
                client._session.headers["Authorization"] == f"Bearer {passport_token}"
            )

    class TestConfigAuth:
        """
        When there's no passport env variable.
        """

        @staticmethod
        @pytest.fixture
        def config_name():
            return "cfg1"

        @staticmethod
        @pytest.fixture
        def uri():
            return "testing_uri"

        @staticmethod
        @pytest.fixture
        def token():
            return "testing_token"

        @staticmethod
        @pytest.fixture
        def config_file(monkeypatch, tmp_path, config_name, uri: str, token: str):
            config_path = tmp_path / "config.json"

            with config_path.open("w") as f:
                json.dump(
                    {
                        "version": configs.CONFIG_FILE_CURRENT_VERSION,
                        "configs": {
                            config_name: {
                                "config_name": config_name,
                                "runtime_name": "QE_REMOTE",
                                "runtime_options": {
                                    "uri": uri,
                                    "token": token,
                                },
                            },
                        },
                        "default_config_name": config_name,
                    },
                    f,
                )

            try:
                yield config_path
            finally:
                config_path.unlink()

        @staticmethod
        @pytest.fixture
        def patch_config_location(monkeypatch, config_file):
            monkeypatch.setenv("ORQ_CONFIG_PATH", str(config_file))

        @staticmethod
        def test_with_config_name(
            patch_config_location,
            config_name: str,
            uri: str,
            token: str,
        ):
            # When
            client = _auth.authorized_client(config_name=config_name)

            # Then
            assert client._base_uri == uri
            assert client._session.headers["Authorization"] == f"Bearer {token}"

        @staticmethod
        def test_uses_default_config(patch_config_location, uri: str, token: str):
            """
            Likely to happen when the user doesn't pass config name to Secrets()
            there's no passport env variable.
            """
            # When
            client = _auth.authorized_client(config_name=None)

            # Then
            assert client._base_uri == uri
            assert client._session.headers["Authorization"] == f"Bearer {token}"

        @staticmethod
        def test_missing_config_name(patch_config_location):
            # Then
            with pytest.raises(exceptions.ConfigNameNotFoundError):
                # When
                _ = _auth.authorized_client(config_name="other_config_name")

        @staticmethod
        def test_missing_config_file(monkeypatch, tmp_path: Path):
            # Given
            monkeypatch.setenv(
                "ORQ_CONFIG_PATH", str(tmp_path / "non_existing_config.json")
            )

            # Then
            with pytest.raises(exceptions.ConfigFileNotFoundError):
                # When
                _ = _auth.authorized_client(config_name="config_name")
