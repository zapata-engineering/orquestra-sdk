################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Test suite for QE-specific CLI v2 tests."""

import typing as t
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _db, _factory
from orquestra.sdk._base._qe import _qe_runtime
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk._base.cli._corq import action
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    RuntimeConfiguration,
    RuntimeConfigurationFile,
    RuntimeName,
)
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.responses import GetLogsResponse
from orquestra.sdk.schema.workflow_run import RunStatus, State, WorkflowRun

from ..project_state import TINY_WORKFLOW_DEF

TEST_CONFIG_NAME = "test"


def _config_with_entry(config_name: str, server_uri="http://localhost"):
    return RuntimeConfigurationFile(
        version=CONFIG_FILE_CURRENT_VERSION,
        configs={
            config_name: RuntimeConfiguration(
                config_name=config_name,
                runtime_name=RuntimeName.QE_REMOTE,
                runtime_options={"uri": server_uri, "token": "foobar"},
            )
        },
    )


CONFIG_WITH_CUSTOM = _config_with_entry("test_cfg")


def _set_up_runtime_mock(monkeypatch, runtime_mock: RuntimeInterface):
    mock_factory_fn = MagicMock()
    mock_factory_fn.return_value = runtime_mock
    monkeypatch.setattr(_factory, "build_runtime_from_config", mock_factory_fn)


def _write_new_config(
    dirpath: Path,
    config_file: RuntimeConfigurationFile,
):
    """
    Overwrites the user config at 'dirpath' with a file with a single configuration:
    'runtime_config'.
    """
    config_file_path = dirpath / _config.CONFIG_FILE_NAME
    config_file_path.write_text(config_file.json())


def _read_config_file(dirpath: Path) -> RuntimeConfigurationFile:
    """
    Reads the contents of the user config file at 'dirpath'.
    """
    config_file_path = dirpath / _config.CONFIG_FILE_NAME
    return RuntimeConfigurationFile.parse_file(config_file_path)


class TestCLIWithQEMock:
    """Tests CLI actions while using a mock instead the real QERuntime."""

    def test_get_logs(self, monkeypatch, tmp_path, patch_config_location):
        _write_new_config(patch_config_location, CONFIG_WITH_CUSTOM)

        # Set up runtime object mock
        inv_id_1 = "inv-id-1"
        log_lines1 = []
        inv_id_2 = "inv-id-2"
        log_lines2 = ["foo 2", "foo bar 2"]
        mock_runtime = MagicMock(RuntimeInterface)
        mock_runtime.get_full_logs = lambda _: {
            inv_id_1: log_lines1,
            inv_id_2: log_lines2,
        }
        _set_up_runtime_mock(monkeypatch, mock_runtime)

        # Call action
        response = action.orq_get_logs(
            Namespace(
                config="test_cfg",
                directory=".",
                follow=False,
                workflow_or_task_run_id=None,
            )
        )

        # Assert
        assert isinstance(response, GetLogsResponse)
        assert response.meta.success
        assert response.logs == [
            f"task-invocation-id: {inv_id_1}",
            *log_lines1,
            f"task-invocation-id: {inv_id_2}",
            *log_lines2,
        ]

    def test_get_logs_v2_invalid_orq_project(
        self, monkeypatch, tmp_path, patch_config_location
    ):
        _write_new_config(patch_config_location, CONFIG_WITH_CUSTOM)

        mock_runtime = Mock(RuntimeInterface)
        mock_runtime.get_full_logs.side_effect = exceptions.InvalidProjectError(
            "Not an Orquestra project directory. Navigate to the repo root."
        )
        _set_up_runtime_mock(monkeypatch, mock_runtime)

        with pytest.raises(exceptions.InvalidProjectError) as exc_info:
            action.orq_get_logs(
                Namespace(
                    config="test_cfg",
                    directory="WRONG PATH",
                    follow=False,
                    workflow_or_task_run_id=None,
                )
            )
        assert "Not an Orquestra project directory" in str(exc_info)

    class TestListWorkflowRuns:
        def _populate_db(self, ids: list, config_names: t.Optional[list] = None):
            if not config_names:
                config_names = [TEST_CONFIG_NAME for _ in ids]
            assert len(ids) == len(config_names)

            with _db.WorkflowDB.open_db() as db:
                for id, config_name in zip(ids, config_names):
                    db.save_workflow_run(
                        StoredWorkflowRun(
                            workflow_run_id=id,
                            config_name=config_name,
                            workflow_def=TINY_WORKFLOW_DEF,
                        ),
                    )

        @pytest.fixture
        def patch_config_for_qe(self, monkeypatch):
            monkeypatch.setattr(
                _config,
                "read_config",
                lambda _: RuntimeConfiguration(
                    config_name=TEST_CONFIG_NAME,
                    runtime_name=RuntimeName.QE_REMOTE,
                    runtime_options={"uri": "http://localhost", "token": "blah"},
                ),
            )

        def test_list_workflows_with_no_date(
            self, tmp_path, monkeypatch, mock_workflow_db_location, patch_config_for_qe
        ):
            max_age = timedelta(days=1)
            amount_workflows = 10
            wf_with_date_run_ids = [f"date_{n+1}" for n in range(amount_workflows)]
            wf_no_date_run_ids = [f"no_date_{n+1}" for n in range(amount_workflows)]
            self._populate_db(wf_with_date_run_ids + wf_no_date_run_ids)

            def _run_with_age_based_on_id(self, run_id: str, *args, **kwargs):
                if run_id.startswith("date"):
                    # Bigger id number maps to newer workflow
                    now = datetime.now(timezone.utc)
                    start_time = (
                        now
                        - max_age
                        * (amount_workflows - int(run_id.replace("date_", "")))
                        / amount_workflows
                    )
                else:
                    now = None
                    start_time = None
                return WorkflowRun(
                    id=run_id,
                    workflow_def=TINY_WORKFLOW_DEF,
                    task_runs=[],
                    status=RunStatus(
                        state=State.RUNNING,
                        start_time=start_time,
                        end_time=now,
                    ),
                )

            monkeypatch.setattr(
                _qe_runtime.QERuntime,
                "get_workflow_run_status",
                _run_with_age_based_on_id,
            )

            args = Namespace(
                directory=str(tmp_path.absolute()),
                limit=amount_workflows * 2,
                prefix=None,
                max_age=None,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=False,
            )

            response = action.orq_list_workflow_runs(args)

            # Check that the workflows with no start time are listed last
            assert response.meta.success
            assert (
                f"Found {len(wf_with_date_run_ids + wf_no_date_run_ids)} matching"
                " workflow runs." in response.meta.message
            )
            assert sorted(wf_with_date_run_ids) == sorted(
                [run.id for run in response.workflow_runs][:amount_workflows]
            )
            # Check that the workflows are ordered according to the start time
            assert [run.id for run in response.workflow_runs][
                :amount_workflows
            ] == wf_with_date_run_ids[::-1]

            args = Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix=None,
                max_age=max_age,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=False,
            )

            response = action.orq_list_workflow_runs(args)
            # Check that the workflows with no start time are left out
            assert response.meta.success
            assert (
                f"Found {len(wf_with_date_run_ids)} matching"
                " workflow runs." in response.meta.message
            )
            assert sorted(wf_with_date_run_ids) == sorted(
                [run.id for run in response.workflow_runs]
            )
