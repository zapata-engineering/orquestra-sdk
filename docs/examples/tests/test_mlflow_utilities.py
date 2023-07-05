################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Snippets and tests used in the "MLFlow Utilities" tutorial.
"""

import pickle
from pathlib import Path
from unittest.mock import Mock

from pytest import MonkeyPatch

from orquestra import sdk

mlflow = Mock()
my_model = Mock()
my_model.state_dict.return_value = {"state": "awesome"}


class Snippets:
    @staticmethod
    def artifacts_dir_snippet():
        artifacts_dir: Path = sdk.mlflow.get_temp_artifacts_dir()

        artifact_path = artifacts_dir / "final_state_dict.pickle"
        with artifact_path.open("wb"):
            pickle.dumps(my_model.state_dict())
        mlflow.log_artifact(artifact_path)

        # </snippet>
        return artifacts_dir


class TestSnippets:
    @staticmethod
    def test_artifact_dir_local(monkeypatch: MonkeyPatch, tmp_path):
        monkeypatch.delenv("ORQ_MLFLOW_ARTIFACTS_DIR", raising=False)
        monkeypatch.setattr(
            sdk.mlflow._connection_utils, "DEFAULT_TEMP_ARTIFACTS_DIR", tmp_path
        )

        assert Snippets.artifacts_dir_snippet() == tmp_path

    @staticmethod
    def test_artifact_dir_env_var_override(monkeypatch: MonkeyPatch, tmp_path):
        monkeypatch.setenv("ORQ_MLFLOW_ARTIFACTS_DIR", str(tmp_path))
        monkeypatch.setattr(
            sdk.mlflow._connection_utils, "DEFAULT_TEMP_ARTIFACTS_DIR", "NOT/THIS/PATH"
        )

        assert Snippets.artifacts_dir_snippet() == tmp_path
