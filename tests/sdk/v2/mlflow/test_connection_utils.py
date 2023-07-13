################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pathlib
from contextlib import suppress as do_not_raise

import pytest

import orquestra.sdk.exceptions as exceptions
from orquestra import sdk
from orquestra.sdk._base._env import CURRENT_USER_ENV


class TestGetTempArtifactsDir:
    class TestWithRemote:
        """
        'Remote' here covers studio and CE as these are treated identically in the code.
        """

        @staticmethod
        def test_happy_path(tmp_path: pathlib.Path, monkeypatch):
            # Given
            monkeypatch.setenv("ORQ_MLFLOW_ARTIFACTS_DIR", str(tmp_path))

            # When
            path = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert path == tmp_path

        @staticmethod
        def test_creates_dir(tmp_path: pathlib.Path, monkeypatch):
            # Given
            dir = tmp_path / "dir_that_does_not_exist"
            assert not dir.exists()
            monkeypatch.setenv("ORQ_MLFLOW_ARTIFACTS_DIR", str(dir))

            # When
            _ = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert dir.exists()

    class TestWithLocal:
        @staticmethod
        def test_happy_path(tmp_path: pathlib.Path, monkeypatch):
            # Given
            monkeypatch.setattr(
                sdk.mlflow._connection_utils, "DEFAULT_TEMP_ARTIFACTS_DIR", tmp_path
            )

            # When
            path = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert path == tmp_path

        @staticmethod
        def test_creates_dir(tmp_path: pathlib.Path, monkeypatch):
            # Given
            dir = tmp_path / "dir_that_does_not_exist"
            assert not dir.exists()
            monkeypatch.setattr(
                sdk.mlflow._connection_utils, "DEFAULT_TEMP_ARTIFACTS_DIR", dir
            )

            # When
            _ = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert dir.exists()


class TestGetCurrentUser:
    @pytest.mark.parametrize(
        "env_var, config_name, raises, expected_user",
        [
            ("my_username", None, do_not_raise(), "my_username"),
            ("my_username", "config", do_not_raise(), "my_username"),
            # this email is encoded inside jwt token
            (None, "proper_token", do_not_raise(), "my_hidden_email@lovely-email.com"),
            (None, "local", pytest.raises(exceptions.RuntimeConfigError), None),
            (None, "improper_token", pytest.raises(exceptions.InvalidTokenError), None),
            (None, None, pytest.raises(exceptions.ConfigNameNotFoundError), None),
            (
                None,
                "non_existing_config",
                pytest.raises(exceptions.ConfigNameNotFoundError),
                None,
            ),
        ],
    )
    def test_get_current_user(
        self,
        env_var,
        config_name,
        raises,
        expected_user,
        monkeypatch,
        tmp_default_config_json,
    ):
        if env_var:
            monkeypatch.setenv(CURRENT_USER_ENV, env_var)

        with raises:
            assert expected_user == sdk.mlflow.get_current_user(config_name)
