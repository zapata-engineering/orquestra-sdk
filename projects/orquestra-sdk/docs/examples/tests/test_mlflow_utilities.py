################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Snippets and tests used in the "MLFlow Utilities" tutorial."""

import os
import pickle
import re
from pathlib import Path
from unittest.mock import Mock

import mlflow
import pytest
import responses
import responses.matchers
from pytest import MonkeyPatch

from orquestra import sdk
from orquestra.sdk._client._base import _config
from orquestra.workflow_shared.schema import configs


class Snippets:
    @staticmethod
    def tracking_uri_token():
        import os

        import mlflow

        from orquestra import sdk

        # MLflow client looks at these environment variables. We need to set them!
        os.environ["MLFLOW_TRACKING_URI"] = sdk.mlflow.get_tracking_uri(
            config_name="prod-d",
            workspace_id="my-ws-2-caabc1",
        )
        os.environ["MLFLOW_TRACKING_TOKEN"] = sdk.mlflow.get_tracking_token(
            config_name="prod-d"
        )

        # Now we're authorized. We can use the standard MLflow client features.
        mlflow.log_metric("loss", 0.2037, step=42)

        # </snippet>
        return "my-ws-2-caabc1"

    @staticmethod
    def tracking_task():
        import os

        from orquestra import sdk

        @sdk.task(dependency_imports=sdk.PythonImports("mlflow-skinny"))
        def my_task1():
            import mlflow

            os.environ["MLFLOW_TRACKING_URI"] = sdk.mlflow.get_tracking_uri(
                config_name="prod-d",
                workspace_id="my-ws-2-caabc1",
            )
            os.environ["MLFLOW_TRACKING_TOKEN"] = sdk.mlflow.get_tracking_token(
                config_name="prod-d"
            )

            mlflow.log_metric("loss1", 0.2037, step=42)

        @sdk.task(dependency_imports=sdk.PythonImports("mlflow-skinny"))
        def my_task2():
            import mlflow

            os.environ["MLFLOW_TRACKING_URI"] = sdk.mlflow.get_tracking_uri(
                config_name="prod-d",
                workspace_id="my-ws-2-caabc1",
            )
            os.environ["MLFLOW_TRACKING_TOKEN"] = sdk.mlflow.get_tracking_token(
                config_name="prod-d"
            )

            mlflow.log_metric("loss2", 0.1234, step=44)

        # </snippet>
        return my_task1, my_task2

    @staticmethod
    def artifacts_dir_snippet():
        my_model = Mock()
        my_model.state_dict.return_value = {"state": "awesome"}

        # <artifacts_dir_snippet>
        import mlflow

        from orquestra import sdk

        artifacts_dir: Path = sdk.mlflow.get_temp_artifacts_dir()

        artifact_path = artifacts_dir / "final_state_dict.pickle"
        with artifact_path.open("wb"):
            pickle.dumps(my_model.state_dict())
        mlflow.log_artifact(str(artifact_path))

        # </snippet>
        return artifacts_dir


@pytest.fixture
def mocked_environ(monkeypatch):
    mock_environ = {}
    monkeypatch.setattr(os, "environ", mock_environ)

    yield mock_environ


def _mock_config(monkeypatch, tmp_path: Path, base_uri, token):
    monkeypatch.setattr(
        _config,
        "get_config_file_path",
        Mock(return_value=tmp_path / "shouldnt_exist.json"),
    )

    def _mock_load(name):
        if name == "in_process":
            return sdk.RuntimeConfig.in_process()
        else:
            return sdk.RuntimeConfig._config_from_runtimeconfiguration(
                configs.RuntimeConfiguration(
                    config_name="mocked",
                    runtime_name=configs.RuntimeName.CE_REMOTE,
                    runtime_options={"uri": base_uri, "token": token},
                )
            )

    monkeypatch.setattr(sdk.RuntimeConfig, "load", _mock_load)


@pytest.fixture
def mocked_responses():
    # This is the place to tap into response mocking machinery, if needed.
    with responses.RequestsMock() as mocked_responses:
        yield mocked_responses


def _wf_run_with_tasks(task_defs) -> sdk.WorkflowRun:
    @sdk.workflow
    def test_wf():
        return [task() for task in task_defs]

    return test_wf().run("in_process")


class TestSnippets:
    class TestTrackingURIToken:
        @staticmethod
        def test_local(mocked_environ, monkeypatch, tmp_path):
            """Simulates using snippets from standalone scripts or local runtimes."""
            # Given
            monkeypatch.setattr(mlflow, "log_metric", Mock())
            base_uri = "https://foo.example.com"
            token = "ABC123 mocked token"
            _mock_config(
                monkeypatch=monkeypatch,
                tmp_path=tmp_path,
                base_uri=base_uri,
                token=token,
            )

            # When
            ws_id = Snippets.tracking_uri_token()

            # Then
            assert mocked_environ["MLFLOW_TRACKING_URI"] == f"{base_uri}/mlflow/{ws_id}"
            assert mocked_environ["MLFLOW_TRACKING_TOKEN"] == token

        @staticmethod
        def test_var_env_override(
            mocked_environ, monkeypatch, tmp_path, mocked_responses
        ):
            """Simulates using the snippets from Studio or CE."""
            # Given
            monkeypatch.setattr(mlflow, "log_metric", Mock())
            monkeypatch.setenv("ORQ_CURRENT_CLUSTER", "mock orq current cluster")

            token = "ABC123 mocked token"
            passport_path = tmp_path / "passport.txt"
            passport_path.write_text(token)
            monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(passport_path))

            cr_name = "mock_cr_name"
            monkeypatch.setenv("ORQ_MLFLOW_CR_NAME", cr_name)

            mlflow_port = "2037"
            monkeypatch.setenv("ORQ_MLFLOW_PORT", mlflow_port)

            artifacts_dir = str(tmp_path / "artifacts")
            monkeypatch.setenv("ORQ_MLFLOW_ARTIFACTS_DIR", artifacts_dir)

            ws_namespace = "mocked-ns"
            mocked_responses.add(
                responses.GET,
                # We're using a regex-based match because the normal URI we send
                # requests to includes a ZRI and ZRIs are notoriously long.
                re.compile(
                    re.escape(
                        "http://orquestra-resource-catalog.resource-catalog/api/"
                        "workspaces/"
                    )
                ),
                json={
                    "namespace": ws_namespace,
                },
            )

            # When
            with pytest.warns(UserWarning):
                _ = Snippets.tracking_uri_token()

            # Then
            assert mocked_environ["MLFLOW_TRACKING_TOKEN"] == token
            assert (
                mocked_environ["MLFLOW_TRACKING_URI"]
                == f"http://{cr_name}.{ws_namespace}:{mlflow_port}"
            )

    class TestTrackingTask:
        @staticmethod
        def test_local(mocked_environ, monkeypatch, tmp_path):
            """Simulates using snippets from standalone scripts or local runtimes."""
            # Given
            monkeypatch.setattr(mlflow, "log_metric", Mock())
            base_uri = "https://foo.example.com"
            token = "ABC123 mocked token"
            _mock_config(
                monkeypatch=monkeypatch,
                tmp_path=tmp_path,
                base_uri=base_uri,
                token=token,
            )

            # When
            task1, task2 = Snippets.tracking_task()
            _ = _wf_run_with_tasks([task1, task2])

            # Then
            assert (
                mocked_environ["MLFLOW_TRACKING_URI"]
                == f"{base_uri}/mlflow/my-ws-2-caabc1"
            )
            assert mocked_environ["MLFLOW_TRACKING_TOKEN"] == token

    class TestArtifactsDirSnippet:
        @staticmethod
        def test_local(monkeypatch: MonkeyPatch, tmp_path):
            monkeypatch.setattr(mlflow, "log_artifact", Mock())
            monkeypatch.delenv("ORQ_MLFLOW_ARTIFACTS_DIR", raising=False)
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "DEFAULT_TEMP_ARTIFACTS_DIR",
                tmp_path,
            )

            assert Snippets.artifacts_dir_snippet() == tmp_path

        @staticmethod
        def test_env_var_override(monkeypatch: MonkeyPatch, tmp_path):
            monkeypatch.setattr(mlflow, "log_artifact", Mock())
            monkeypatch.setenv("ORQ_MLFLOW_ARTIFACTS_DIR", str(tmp_path))
            monkeypatch.setattr(
                sdk.mlflow._connection_utils,
                "DEFAULT_TEMP_ARTIFACTS_DIR",
                "NOT/THIS/PATH",
            )

            assert Snippets.artifacts_dir_snippet() == tmp_path
