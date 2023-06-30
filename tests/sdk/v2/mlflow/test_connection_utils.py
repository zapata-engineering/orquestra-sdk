################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pathlib

from orquestra import sdk


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
