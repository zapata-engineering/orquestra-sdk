################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pathlib
from contextlib import suppress as do_not_raise
from typing import List
from unittest.mock import Mock, create_autospec

import orquestra.workflow_shared.exceptions as exceptions
import pytest
from pytest import MonkeyPatch
from requests import Response, Session

from orquestra import sdk
from orquestra.sdk._client._base._env import (
    CURRENT_CLUSTER_ENV,
    CURRENT_USER_ENV,
    MLFLOW_ARTIFACTS_DIR,
    MLFLOW_CR_NAME,
    MLFLOW_PORT,
    PASSPORT_FILE_ENV,
)


class TestGetTempArtifactsDir:
    class TestWithRemote:
        """
        'Remote' here covers studio and CE as these are treated identically in the code.
        """

        @staticmethod
        def test_happy_path(tmp_path: pathlib.Path, monkeypatch):
            # Given
            monkeypatch.setenv(MLFLOW_ARTIFACTS_DIR, str(tmp_path))

            # When
            path = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert path == tmp_path

        @staticmethod
        def test_creates_dir(tmp_path: pathlib.Path, monkeypatch):
            # Given
            dir = tmp_path / "dir_that_does_not_exist"
            assert not dir.exists()
            monkeypatch.setenv(MLFLOW_ARTIFACTS_DIR, str(dir))

            # When
            _ = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert dir.exists()

    class TestWithLocal:
        @staticmethod
        def test_happy_path(tmp_path: pathlib.Path, monkeypatch):
            # Given
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "DEFAULT_TEMP_ARTIFACTS_DIR",
                tmp_path,
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
                sdk.mlflow._connection_utils,
                "DEFAULT_TEMP_ARTIFACTS_DIR",
                dir,
            )

            # When
            _ = sdk.mlflow.get_temp_artifacts_dir()

            # Then
            assert dir.exists()


@pytest.fixture
def mock_is_executing_remotely(monkeypatch):
    monkeypatch.setenv(CURRENT_CLUSTER_ENV, "<current cluster sentinel>")


@pytest.fixture
def mock_is_executing_locally(monkeypatch: MonkeyPatch):
    monkeypatch.delenv(CURRENT_CLUSTER_ENV, raising=False)

    # TODO: after https://zapatacomputing.atlassian.net/browse/ORQSDK-914 this should
    # only have to concern itself with CURRENT_CLUSTER_ENV, so the following lines can
    # be excised.
    for env_var in [
        MLFLOW_ARTIFACTS_DIR,
        MLFLOW_CR_NAME,
        MLFLOW_PORT,
        PASSPORT_FILE_ENV,
    ]:
        monkeypatch.delenv(env_var, raising=False)


class TestGetTrackingURI:
    @pytest.mark.usefixtures("mock_is_executing_remotely")
    class TestRemote:
        @staticmethod
        def test_happy_path(tmp_path: pathlib.Path, monkeypatch: MonkeyPatch):
            # Given
            # Mock the passport token envvar and its file location
            token_file = tmp_path / "tmp_token_file.txt"
            token_file.write_text("<token sentinel>")
            monkeypatch.setenv(PASSPORT_FILE_ENV, str(token_file))
            # Mock the http session and response
            mock_requests_session = create_autospec(Session)
            mock_response = create_autospec(Response)
            mock_response.json.return_value = {"namespace": "<namespace sentinel>"}
            mock_requests_session.headers = {}
            mock_requests_session.get.return_value = mock_response
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "Session",
                Mock(return_value=mock_requests_session),
            )
            # Mock the custom resource name and port envvars
            monkeypatch.setenv(MLFLOW_CR_NAME, "<mlflow cr name sentinel>")
            monkeypatch.setenv(MLFLOW_PORT, "<mlflow port sentinel>")

            # When
            tracking_url = sdk.mlflow.get_tracking_uri("<workspace id sentinel>")

            # Then
            assert (
                tracking_url
                == "http://<mlflow cr name sentinel>.<namespace sentinel>:<mlflow port sentinel>"  # noqa: E501
            )

        @staticmethod
        def test_test_warns_user_that_config_parameter_will_be_ignored(
            tmp_path: pathlib.Path, monkeypatch: MonkeyPatch
        ):
            # Given
            # Mock the passport token envvar and its file location
            token_file = tmp_path / "tmp_token_file.txt"
            token_file.write_text("<token sentinel>")
            monkeypatch.setenv(PASSPORT_FILE_ENV, str(token_file))
            # Mock the http session and response
            mock_requests_session = create_autospec(Session)
            mock_response = create_autospec(Response)
            mock_response.json.return_value = {"namespace": "<namespace sentinel>"}
            mock_requests_session.headers = {}
            mock_requests_session.get.return_value = mock_response
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "Session",
                Mock(return_value=mock_requests_session),
            )
            # Mock the custom resource name and port envvars
            monkeypatch.setenv(MLFLOW_CR_NAME, "<mlflow cr name sentinel>")
            monkeypatch.setenv(MLFLOW_PORT, "<mlflow port sentinel>")

            # When
            with pytest.warns(UserWarning) as e:
                _ = sdk.mlflow.get_tracking_uri(
                    "<workspace id sentinel>", config_name="<config sentinel>"
                )

            # Then
            assert len(e.list) == 1
            assert (
                "The 'config_name' parameter is used only when executing locally, and will be ignored."  # noqa: E501
                in str(e.list[0].message)
            )

        @staticmethod
        @pytest.mark.parametrize(
            "unset_envvars",
            [
                [MLFLOW_CR_NAME],
                [MLFLOW_PORT],
                [PASSPORT_FILE_ENV],
                [MLFLOW_CR_NAME, MLFLOW_PORT],
                [MLFLOW_CR_NAME, PASSPORT_FILE_ENV],
                [MLFLOW_PORT, PASSPORT_FILE_ENV],
                [MLFLOW_CR_NAME, MLFLOW_PORT, PASSPORT_FILE_ENV],
            ],
        )
        def test_raises_EnvironemntError_when_required_env_vars_are_not_set(
            tmp_path: pathlib.Path, monkeypatch: MonkeyPatch, unset_envvars: List[str]
        ):
            # Given
            # Mock the http session and response
            mock_requests_session = create_autospec(Session)
            mock_response = create_autospec(Response)
            mock_response.json.return_value = {"namespace": "<namespace sentinel>"}
            mock_requests_session.headers = {}
            mock_requests_session.get.return_value = mock_response
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "Session",
                Mock(return_value=mock_requests_session),
            )
            # Mock env vars except the ones we're testing by leaving unset
            token_file = tmp_path / "tmp_token_file.txt"
            token_file.write_text("<token sentinel>")
            for env_var, value in [
                (MLFLOW_CR_NAME, "<cr name sentinel>"),
                (MLFLOW_PORT, "<port entinel>"),
                (PASSPORT_FILE_ENV, str(token_file)),
            ]:
                if env_var not in unset_envvars:
                    monkeypatch.setenv(env_var, value)

            # When
            with pytest.raises(EnvironmentError) as e:
                _ = sdk.mlflow.get_tracking_uri("<workspace id sentinel>")

            assert "environment variable is not set." in e.exconly()

    @pytest.mark.usefixtures("mock_is_executing_locally")
    class TestLocal:
        @staticmethod
        def test_happy_path(monkeypatch: MonkeyPatch):
            # Given
            mock_config = create_autospec(sdk.RuntimeConfig)
            mock_config.uri = "<uri sentinel>"
            monkeypatch.setattr(
                sdk.RuntimeConfig, "load", Mock(return_value=mock_config)
            )

            # When
            tracking_url = sdk.mlflow.get_tracking_uri(
                "<workspace id sentinel>", config_name="<config_name_sentinel>"
            )

            # Then
            assert tracking_url == "<uri sentinel>/mlflow/<workspace id sentinel>"

        @staticmethod
        def test_raises_ValueError_if_no_config_name():
            # When
            with pytest.raises(ValueError) as e:
                _ = sdk.mlflow.get_tracking_uri("<workspace id sentinel>")

            # Then
            assert (
                "The config_name parameter is required for local runs." in e.exconly()
            )

        @staticmethod
        def test_raises_ValueError_if_config_has_no_uri(monkeypatch: MonkeyPatch):
            # Given
            mock_config = create_autospec(sdk.RuntimeConfig)
            monkeypatch.setattr(
                sdk.RuntimeConfig, "load", Mock(return_value=mock_config)
            )

            # When
            with pytest.raises(ValueError) as e:
                _ = sdk.mlflow.get_tracking_uri(
                    "<workspace id sentinel>", config_name="<config_name_sentinel>"
                )

            # Then
            assert (
                "The config '<config_name_sentinel>' has no URI associated with it."
                in e.exconly()
            )


class TestGetTrackingToken:
    @pytest.mark.usefixtures("mock_is_executing_remotely")
    class TestWithRemote:
        @staticmethod
        def test_uses_passport_token(monkeypatch: MonkeyPatch):
            # Given
            mock_read_passport_token = Mock(return_value="<passport token sentinel>")
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_read_passport_token",
                mock_read_passport_token,
            )

            # When
            tracking_token = sdk.mlflow.get_tracking_token()

            # Then
            assert tracking_token == "<passport token sentinel>"

        @staticmethod
        def test_warns_user_that_config_parameter_will_be_ignored(monkeypatch):
            # Given
            mock_read_passport_token = Mock(return_value="<passport token sentinel>")
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_read_passport_token",
                mock_read_passport_token,
            )

            # When
            with pytest.warns(UserWarning) as e:
                _ = sdk.mlflow.get_tracking_token(config_name="<config name sentinel>")

            # Then
            assert len(e.list) == 1
            assert (
                "The 'config_name' parameter is used only when executing locally, and will be ignored."  # noqa: E501
                in str(e.list[0].message)
            )

    @pytest.mark.usefixtures("mock_is_executing_locally")
    class TestWithLocal:
        @staticmethod
        def test_uses_config_token(monkeypatch):
            # Given
            mock_config = create_autospec(sdk.RuntimeConfig)
            mock_config.token = "<token sentinel>"
            monkeypatch.setattr(
                sdk.RuntimeConfig, "load", Mock(return_value=mock_config)
            )

            # When
            tracking_token = sdk.mlflow.get_tracking_token(
                config_name="<config sentinel>"
            )

            # Then
            assert tracking_token == "<token sentinel>"

        @staticmethod
        def test_raises_ValueError_if_no_config_name():
            # When
            with pytest.raises(ValueError) as e:
                _ = sdk.mlflow.get_tracking_token()

            # Then
            assert (
                "The config_name parameter is required for local runs." in e.exconly()
            )


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
