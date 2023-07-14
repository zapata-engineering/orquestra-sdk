################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pathlib
from unittest.mock import Mock, create_autospec

import pytest
from pytest import MonkeyPatch
from requests import Response, Session

from orquestra import sdk
from orquestra.sdk._base._env import (
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


@pytest.fixture
def mock_is_executing_remotely(monkeypatch):
    monkeypatch.setattr(
        sdk.mlflow._connection_utils, "_is_executing_remoteley", Mock(return_value=True)
    )


@pytest.fixture
def mock_is_executing_locally(monkeypatch):
    monkeypatch.setattr(
        sdk.mlflow._connection_utils,
        "_is_executing_remoteley",
        Mock(return_value=False),
    )


class TestGetMLFlowCRNameAndPort:
    @staticmethod
    def test_happy_path(monkeypatch: MonkeyPatch, mock_is_executing_remotely):
        # Given
        monkeypatch.setenv(MLFLOW_CR_NAME, "<mlflow cr name sentinel>")
        monkeypatch.setenv(MLFLOW_PORT, "<mlflow port sentinel>")

        # When
        name, port = sdk.mlflow._connection_utils._get_mlflow_cr_name_and_port()

        # Then
        assert name == "<mlflow cr name sentinel>"
        assert port == "<mlflow port sentinel>"

    @staticmethod
    def test_defensive_assert_for_remote_only(
        monkeypatch: MonkeyPatch, mock_is_executing_locally
    ):
        # Given
        monkeypatch.setattr(
            sdk.mlflow._connection_utils,
            "_is_executing_remoteley",
            Mock(return_value=False),
        )

        # Then
        with pytest.raises(AssertionError):
            _ = sdk.mlflow._connection_utils._get_mlflow_cr_name_and_port()


class TestReadPassportToken:
    @staticmethod
    def test_happy_path(monkeypatch: MonkeyPatch, tmp_path: pathlib.Path):
        # Given
        token_file = tmp_path / "tmp_token_file.txt"
        token_file.write_text("<token sentinel>")
        monkeypatch.setenv(PASSPORT_FILE_ENV, str(token_file))

        # When
        token = sdk.mlflow._connection_utils._read_passport_token()

        # Then
        assert token == "<token sentinel>"


class TestMakeSession:
    @staticmethod
    def test_happy_path(monkeypatch: MonkeyPatch):
        # Given
        mock_requests_session = create_autospec(Session)
        mock_requests_session.headers = {}
        monkeypatch.setattr(
            sdk.mlflow._connection_utils,
            "Session",
            Mock(return_value=mock_requests_session),
        )

        # When
        session = sdk.mlflow._connection_utils._make_session("<token sentinel>")

        # Then
        assert session.headers["Content-Type"] == "application/json"
        assert session.headers["Authorization"] == "Bearer <token sentinel>"


class TestGetTrackingURI:
    @pytest.mark.usefixtures("mock_is_executing_remotely")
    class TestRemote:
        @staticmethod
        def test_unit_happy_path(monkeypatch: MonkeyPatch):
            # Given
            mock_session = create_autospec(Session)
            mock_response = create_autospec(Response)
            mock_response.json.return_value = {"namespace": "<namespace sentinel>"}
            mock_session.get.return_value = mock_response
            mock_read_passport_token = Mock(return_value="<passport token sentinel>")
            mock_make_session = Mock(return_value=mock_session)
            mock_make_workspace_zri = Mock(return_value="<workspace ZRI sentinel>")
            mock_make_workspace_url = Mock(return_value="<workspace URL sentinel>")
            mock_get_mlflow_cr_name_and_port = Mock(
                return_value=("<CR Name Sentinel>", "<port sentinel>")
            )

            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_read_passport_token",
                mock_read_passport_token,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils, "_make_session", mock_make_session
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_make_workspace_zri",
                mock_make_workspace_zri,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_make_workspace_url",
                mock_make_workspace_url,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_get_mlflow_cr_name_and_port",
                mock_get_mlflow_cr_name_and_port,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "RESOURCE_CATALOG_URI",
                "<resource catalog uri sentinel>",
            )

            # When
            tracking_url = sdk.mlflow.get_tracking_uri("<workspace id sentinel>")

            # Then
            assert (
                tracking_url
                == "http://<CR Name Sentinel>.<namespace sentinel>:<port sentinel>"
            )
            mock_read_passport_token.assert_called_once_with()
            mock_make_session.assert_called_once_with("<passport token sentinel>")
            mock_make_workspace_zri.assert_called_once_with("<workspace id sentinel>")
            mock_make_workspace_url.assert_called_once_with(
                "<resource catalog uri sentinel>", "<workspace ZRI sentinel>"
            )
            mock_get_mlflow_cr_name_and_port.assert_called_once_with()

        @staticmethod
        def test_warns_user_that_config_parameter_will_be_ignored(monkeypatch):
            # Given
            mock_session = create_autospec(Session)
            mock_response = create_autospec(Response)
            mock_response.json.return_value = {"namespace": "<namespace sentinel>"}
            mock_session.get.return_value = mock_response
            mock_read_passport_token = Mock(return_value="<passport token sentinel>")
            mock_make_session = Mock(return_value=mock_session)
            mock_make_workspace_zri = Mock(return_value="<workspace ZRI sentinel>")
            mock_make_workspace_url = Mock(return_value="<workspace URL sentinel>")
            mock_get_mlflow_cr_name_and_port = Mock(
                return_value=("<CR Name Sentinel>", "<port sentinel>")
            )

            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_read_passport_token",
                mock_read_passport_token,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils, "_make_session", mock_make_session
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_make_workspace_zri",
                mock_make_workspace_zri,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_make_workspace_url",
                mock_make_workspace_url,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "_get_mlflow_cr_name_and_port",
                mock_get_mlflow_cr_name_and_port,
            )
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "RESOURCE_CATALOG_URI",
                "<resource catalog uri sentinel>",
            )

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
        def test_integration_happy_path(
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
            tracking_url = sdk.mlflow.get_tracking_uri("<workspace id sentinel>")

            # Then
            assert (
                tracking_url
                == "http://<mlflow cr name sentinel>.<namespace sentinel>:<mlflow port sentinel>"  # noqa: E501
            )

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
