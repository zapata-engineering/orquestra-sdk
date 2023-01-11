################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Tests for providers of the auth for secrets access.
"""

import json
from pathlib import Path

import pytest

from orquestra.sdk.schema import configs
from orquestra.sdk.secrets import _providers


class TestConfigProvider:
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
    def config_file(tmp_path, config_name, uri: str, token: str):
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
    def test_reads_creds(config_name: str, config_file: Path, uri: str, token: str):
        provider = _providers.ConfigProvider()

        # When
        client = provider.make_client(
            config_name=config_name,
            config_save_path=config_file,
        )

        # Then
        assert client._base_uri == uri
        assert client._session.headers["Authorization"] == f"Bearer {token}"

    @staticmethod
    def test_uses_default_config(config_file: Path, uri: str, token: str):
        """
        Likely to happen when the user doesn't pass config name to Secrets(), and it's
        inside a local script, or a task on a local Ray.
        """
        provider = _providers.ConfigProvider()

        # When
        client = provider.make_client(
            config_name=None,
            config_save_path=config_file,
        )

        # Then
        assert client._base_uri == uri
        assert client._session.headers["Authorization"] == f"Bearer {token}"


class TestPassportProvider:
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
    def test_reads_creds(monkeypatch, passport_token: str, passport_file: Path):
        monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(passport_file))
        provider = _providers.PassportProvider()

        # When
        client = provider.make_client(
            config_name=None,
            config_save_path=None,
        )

        # Then
        assert client._base_uri == "http://config-service.config-service:8099"
        assert client._session.headers["Authorization"] == f"Bearer {passport_token}"
