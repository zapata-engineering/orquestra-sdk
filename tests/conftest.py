################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Pytest's requirement to share fixtures across test files.
"""
import sqlite3
from pathlib import Path
from unittest.mock import Mock

import pytest

import orquestra.sdk._base._config
from orquestra.sdk._base import _db


@pytest.fixture
def patch_config_location(tmp_path, monkeypatch):
    """
    Makes the functions in orquestra.sdk._base._config read/write file from a
    temporary directory.
    """
    config_location = Mock(return_value=tmp_path / "config.json")
    monkeypatch.setattr(
        orquestra.sdk._base._config, "_get_config_file_path", config_location
    )
    config_file_location = Mock(return_value=tmp_path / "config.json")
    monkeypatch.setattr(
        orquestra.sdk._base._config, "_get_config_file_path", config_file_location
    )
    return tmp_path


@pytest.fixture
def patch_config_name_generation(monkeypatch):
    patched_name = "patched_config_name"
    monkeypatch.setattr(
        orquestra.sdk._base._config,
        "generate_config_name",
        Mock(return_value=patched_name),
    )
    return patched_name


@pytest.fixture
def mock_workflow_db_location(tmp_path, monkeypatch):
    """Set up a realistic Orquestra workflow database under a temporary path, and mocks
    `WorkflowDB.open_db()` to return the temporary database rather than the real one.
    """
    mock_db_location = tmp_path / "workflows.db"
    monkeypatch.setattr(
        _db._db,
        "_get_default_db_location",
        lambda: mock_db_location,
    )
    return mock_db_location


@pytest.fixture
def mock_project_workflow_db(tmp_path: Path):
    """Set up a realistic Orquestra workflow database under a temporary path, and mocks
    `WorkflowDB.open_db()` to return the temporary database rather than the real one.
    """

    db_path = tmp_path / ".orquestra" / "workflows.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path, isolation_level="EXCLUSIVE")
    with db:
        _db._db._create_workflow_table(db)
    return _db.WorkflowDB(db)


@pytest.fixture
def patch_runtime_option_validation(monkeypatch):
    def assume_valid(_, input, **kwargs):
        if input is None:
            return {}
        else:
            return input

    monkeypatch.setattr(
        orquestra.sdk._base._config, "_validate_runtime_options", assume_valid
    )
