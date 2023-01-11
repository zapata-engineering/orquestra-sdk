################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock

from pytest import MonkeyPatch

import orquestra.sdk as sdk
from orquestra.sdk._base._db import WorkflowDB
from orquestra.sdk._base._db._migration import migrate_project_db_to_shared_db
from orquestra.sdk._base.cli._corq import action
from orquestra.sdk.schema.local_database import StoredWorkflowRun


class TestProjectToSharedDBMigration:
    def test_test_migration_with_cli(
        self,
        tmp_path: Path,
        monkeypatch: MonkeyPatch,
        mock_project_workflow_db: WorkflowDB,
        mock_workflow_db_location: Path,
    ):
        # Given
        @sdk.workflow
        def test():
            return 0

        workflow_def = test.model

        project_db_location = tmp_path / ".orquestra" / "workflows.db"
        with mock_project_workflow_db as project_db:
            project_db.save_workflow_run(
                StoredWorkflowRun(
                    workflow_run_id="test",
                    config_name="test",
                    workflow_def=workflow_def,
                )
            )
        # We just want to run a command that uses the database, but we don't need to
        # interact with a runtime.
        # In this case, we're going to use `orq list workflow-runs` with these
        # arguments (important args commented):
        args = Namespace(
            # directory pointing to the test location
            directory=str(tmp_path.absolute()),
            # config set to `does_not_matter` which doesn't exist in the database
            config="does_not_matter",
            # The following args are set to stop the command from failing
            additional_project_dirs=[],
            limit=None,
            prefix=None,
            max_age=None,
            status=None,
            all=False,
        )
        mock_config = MagicMock()
        monkeypatch.setattr("orquestra.sdk._base._config.read_config", mock_config)
        # When
        resp = action.orq_list_workflow_runs(args)
        # Then
        # We don't care about this response, just that the command ran successfully
        assert resp.meta.success
        # Check the new database
        assert mock_workflow_db_location.exists()
        with WorkflowDB.open_db() as db:
            stored_wf_run = db.get_workflow_run("test")
        assert stored_wf_run.workflow_run_id == "test"
        assert stored_wf_run.config_name == "test"
        assert stored_wf_run.workflow_def == workflow_def
        # Check project DB
        assert not project_db_location.exists()
        assert project_db_location.with_suffix(".old").exists()

    def test_non_existent_project_db(
        self, tmp_path: Path, mock_workflow_db_location: Path
    ):
        # Given
        assert not mock_workflow_db_location.exists()
        # When
        migrate_project_db_to_shared_db(tmp_path)
        # Then
        assert not mock_workflow_db_location.exists()

    def test_empty_project_db(
        self,
        tmp_path: Path,
        mock_project_workflow_db: WorkflowDB,
        mock_workflow_db_location: Path,
    ):
        # Given
        project_db_location = tmp_path / ".orquestra" / "workflows.db"
        # We need to close the DB, otherwise Windows complains
        # In usual usage, the database will always be closed via the context manager
        mock_project_workflow_db._db.close()
        # When
        migrate_project_db_to_shared_db(tmp_path)
        # Then
        assert mock_workflow_db_location.exists()
        assert not project_db_location.exists()
        assert project_db_location.with_suffix(".old").exists()

    def test_with_workflows(
        self,
        tmp_path: Path,
        mock_project_workflow_db: WorkflowDB,
        mock_workflow_db_location: Path,
    ):
        # Given
        @sdk.workflow
        def test():
            return 0

        workflow_def = test.model

        project_db_location = tmp_path / ".orquestra" / "workflows.db"
        with mock_project_workflow_db as project_db:
            project_db.save_workflow_run(
                StoredWorkflowRun(
                    workflow_run_id="test",
                    config_name="test",
                    workflow_def=workflow_def,
                )
            )
        # When
        migrate_project_db_to_shared_db(tmp_path)
        # Then
        # Check new database
        assert mock_workflow_db_location.exists()
        with WorkflowDB.open_db() as db:
            stored_wf_run = db.get_workflow_run("test")
        assert stored_wf_run.workflow_run_id == "test"
        assert stored_wf_run.config_name == "test"
        assert stored_wf_run.workflow_def == workflow_def
        # Check project DB
        assert not project_db_location.exists()
        assert project_db_location.with_suffix(".old").exists()
