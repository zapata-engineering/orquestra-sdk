################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import os
from pathlib import Path
from unittest import mock

from orquestra import sdk


class TestGetTempArtifactsDir:
    @staticmethod
    def test_with_studio_or_ce(tmp_path):
        with mock.patch.dict(os.environ, {"ORQ_MLFLOW_ARTIFACTS_DIR": str(tmp_path)}):
            path = sdk.mlflow.get_temp_artifacts_dir()
        assert path == tmp_path

    @staticmethod
    def test_with_local():
        path = sdk.mlflow.get_temp_artifacts_dir()

        assert path == Path.home() / ".orquestra" / "mlflow" / "artifacts"
