################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import orquestra.sdk._base._db._db as _db


class TestDBLocation:
    def test_default(self):
        # When
        location = _db._get_default_db_location()
        # Then
        assert location == Path.home() / ".orquestra" / "workflows.db"

    def test_with_environment_variable(self, tmp_path, monkeypatch):
        # Given
        env = {"ORQ_DB_LOCATION": str(tmp_path / "workflows.db")}
        monkeypatch.setattr("os.environ", env)
        # When
        location = _db._get_default_db_location()
        # Then
        assert location == tmp_path / "workflows.db"

    def test_open_project_db_in_default_location(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        mock_project_workflow_db: _db.WorkflowDB,
    ):
        # This tests this scenario:
        # ~ < cwd
        # |-- .orquestra
        #     |-- workflows.db
        # $ orq list workflow-runs

        # Given
        monkeypatch.setattr(
            _db,
            "_get_default_db_location",
            lambda: tmp_path / ".orquestra" / "workflows.db",
        )
        migrate_fn = MagicMock()
        monkeypatch.setattr(_db, "migrate_project_db_to_shared_db", migrate_fn)

        # When
        _db.WorkflowDB.open_project_db(tmp_path)

        # Then
        migrate_fn.assert_not_called()
