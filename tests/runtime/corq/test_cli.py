################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Test suite for CLI v2 tests that aren't specific to RayRuntime or QERuntime."""

import argparse
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

import orquestra.sdk._base._config as v2_config
import orquestra.sdk.examples.workflow_defs
from orquestra.sdk import exceptions
from orquestra.sdk._base import _factory
from orquestra.sdk._base._config import BUILT_IN_CONFIG_NAME
from orquestra.sdk._base.cli._corq import action
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    RuntimeConfigurationFile,
)
from orquestra.sdk.schema.responses import (
    GetDefaultConfig,
    ResponseStatusCode,
    SubmitWorkflowDefResponse,
)

WORKFLOW_DEFS = """import orquestra.sdk as sdk
@sdk.task
def hello():
    pass
@sdk.workflow
def wf():
    return [hello()]
"""

WORKFLOW_DEFS_NO_WORKFLOW = """import orquestra.sdk as sdk
@sdk.task
def hello():
    pass
"""

GIT_INFER_WORKFLOW_DEFS = """import orquestra.sdk as sdk
@sdk.task(source_import=sdk.GitImport.infer())
def hello():
    pass
@sdk.workflow
def wf():
    return [hello()]
"""


@pytest.mark.parametrize(
    "test_action",
    [
        action.orq_get_workflow_def,
        action.orq_get_task_def,
    ],
)
def test_orq_get_X_def(tmp_path, monkeypatch, test_action):
    args = argparse.Namespace(directory=str(tmp_path.absolute()))
    with open(tmp_path / "workflow_defs.py", "w") as f:
        f.write(WORKFLOW_DEFS)
    response = test_action(args)
    assert response.meta.success
    assert response.meta.code == ResponseStatusCode.OK


@pytest.mark.parametrize("config_name", ["local", "in_process"])
class TestSetDefaultConfig:
    @staticmethod
    def test_orq_set_default_config_no_config(
        config_name, patch_config_location, tmp_path, monkeypatch
    ):
        def _mock_get_config_dir():
            return tmp_path

        monkeypatch.setattr(v2_config, "_get_config_directory", _mock_get_config_dir)

        args = argparse.Namespace(config_name=config_name)

        action.orq_set_default_config(args)
        assert (patch_config_location / "config.json").exists()

    @staticmethod
    def test_orq_set_default_builtin(config_name, monkeypatch):

        monkeypatch.setattr(v2_config, "update_default_config_name", value=callable)

        args = argparse.Namespace(config_name=config_name)
        response = action.orq_set_default_config(args)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK


def test_orq_set_default_remote(monkeypatch):

    monkeypatch.setattr(v2_config, "update_default_config_name", value=callable)

    args = argparse.Namespace(config_name="remote")
    response = action.orq_set_default_config(args)
    assert response.meta.success
    assert response.meta.code == ResponseStatusCode.OK
    assert response.default_config_name == "remote"


def test_orq_get_default_config_no_config(tmp_path, monkeypatch):
    def _mock_get_config_dir():
        return tmp_path / "doesnt-exist"

    monkeypatch.setattr(v2_config, "_get_config_directory", _mock_get_config_dir)

    args = argparse.Namespace()
    response = action.orq_get_default_config(args)
    assert response.meta.success
    assert response.meta.code == ResponseStatusCode.OK
    assert isinstance(response, GetDefaultConfig)
    assert response.default_config_name == "local"


def test_orq_get_default(monkeypatch):
    def _mock_read_default_config_name():
        return "local"

    monkeypatch.setattr(
        v2_config, "read_default_config_name", _mock_read_default_config_name
    )

    args = argparse.Namespace()
    response = action.orq_get_default_config(args)
    assert response.meta.success
    assert response.meta.code == ResponseStatusCode.OK


@pytest.mark.parametrize(
    "test_action",
    [
        action.orq_get_logs,
        action.orq_get_workflow_run,
        action.orq_get_workflow_run_results,
        action.orq_stop_workflow_run,
    ],
)
class TestMissingConfigErrors:
    def test_invalid_config_entry(self, patch_config_location, test_action):

        cfg_file = RuntimeConfigurationFile(
            version=CONFIG_FILE_CURRENT_VERSION,
            configs={},
        )
        (patch_config_location / v2_config.CONFIG_FILE_NAME).write_text(cfg_file.json())

        with pytest.raises(exceptions.ConfigNameNotFoundError):
            test_action(
                argparse.Namespace(config="some-cfg-name", workflow_run_id="hello")
            )  # noqa: E501


class TestListWorkflowRuns:
    @staticmethod
    def test_empty_db(tmp_path, mock_workflow_db_location):
        args = argparse.Namespace(
            directory=str(tmp_path.absolute()),
            limit=None,
            prefix=None,
            max_age=None,
            status=None,
            config=None,
            additional_project_dirs=[],
            all=True,
        )

        response = action.orq_list_workflow_runs(args)

        assert response.meta.success
        assert "Found 0 matching workflow runs." in response.meta.message
        assert response.workflow_runs == []


@pytest.mark.slow
class TestDirtyGitRepo:
    @pytest.fixture
    def git_infer_fixture(self, tmp_path, monkeypatch):
        monkeypatch.setattr("git.Repo.is_dirty", Mock(return_value=True))

        mocked = Mock(name="mocked runtime")
        mocked.return_value.create_workflow_run.return_value = "mocked ID"
        monkeypatch.setattr(_factory, "build_runtime_from_config", mocked)

        with open(tmp_path / "workflow_defs.py", "w") as f:
            f.write(GIT_INFER_WORKFLOW_DEFS)

        yield tmp_path

    def test_submit_without_force(self, git_infer_fixture):
        args = argparse.Namespace(
            config=BUILT_IN_CONFIG_NAME,
            directory=str(git_infer_fixture.absolute()),
            workflow_def_name=None,
            verbose=logging.INFO,
            force=False,
        )
        with pytest.raises(exceptions.DirtyGitRepoError):
            action.orq_submit_workflow_def(args)

    def test_submit_with_force(self, git_infer_fixture):
        args = argparse.Namespace(
            config=BUILT_IN_CONFIG_NAME,
            directory=str(git_infer_fixture.absolute()),
            workflow_def_name=None,
            verbose=logging.INFO,
            force=True,
        )

        with pytest.warns(exceptions.DirtyGitRepo):
            result = action.orq_submit_workflow_def(args)

        assert isinstance(result, SubmitWorkflowDefResponse)
        assert "mocked ID" in [run.id for run in result.workflow_runs]


EXAMPLES_PATH = Path(orquestra.sdk.examples.workflow_defs.__file__).parent


class TestSubmitWorkflowErrors:
    def test_submit_multiple_workflows_no_wf_name(self):
        args = argparse.Namespace(
            workflow_def_name="",
            directory=str(EXAMPLES_PATH),
            config=BUILT_IN_CONFIG_NAME,
            verbose=False,
            force=False,
        )
        with pytest.raises(exceptions.InvalidWorkflowDefinitionError) as exc_info:
            action.orq_submit_workflow_def(args)
        assert "Multiple workflow definitions found in project" in str(exc_info)

    def test_submit_not_workflow_function(self):
        args = argparse.Namespace(
            workflow_def_name="hello",  # hello is task function, not workflow function
            directory=str(EXAMPLES_PATH),
            config=BUILT_IN_CONFIG_NAME,
            verbose=False,
            force=False,
        )
        with pytest.raises(exceptions.InvalidWorkflowDefinitionError) as exc_info:
            action.orq_submit_workflow_def(args)
        assert "not a workflow function" in str(exc_info)

    def test_submit_no_workflows(self, tmp_path, monkeypatch):
        with open(tmp_path / "workflow_defs.py", "w") as f:
            f.write(WORKFLOW_DEFS_NO_WORKFLOW)
        args = argparse.Namespace(
            workflow_def_name="",
            directory=str(tmp_path.absolute()),
            config=BUILT_IN_CONFIG_NAME,
            verbose=False,
            force=False,
        )
        with pytest.raises(exceptions.InvalidWorkflowDefinitionError) as exc_info:
            action.orq_submit_workflow_def(args)

        assert "No workflow definitions found in project" in str(exc_info)
