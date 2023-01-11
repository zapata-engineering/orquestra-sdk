################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import os
import sqlite3
from contextlib import AbstractContextManager
from pathlib import Path
from typing import List, Optional, Union

from orquestra.sdk._base._db._migration import migrate_project_db_to_shared_db
from orquestra.sdk._base.abc import WorkflowRepo
from orquestra.sdk.exceptions import WorkflowNotFoundError
from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import WorkflowRunId


def _create_workflow_table(db: sqlite3.Connection):
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS workflow_runs (workflow_run_id, config_name, workflow_def)"  # noqa: E501
        )
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS workflow_runs_id ON workflow_runs(workflow_run_id)"  # noqa: E501
        )


def _get_default_db_location() -> Path:
    try:
        return Path(os.environ["ORQ_DB_LOCATION"])
    except KeyError:
        return Path.home() / ".orquestra" / "workflows.db"


class WorkflowDB(WorkflowRepo, AbstractContextManager):
    """
    SQLite storage for workflow runs
    """

    @classmethod
    def open_project_db(cls, project_dir: Union[Path, str]):
        old_db_location = Path(project_dir) / ".orquestra" / "workflows.db"
        if old_db_location != _get_default_db_location():
            migrate_project_db_to_shared_db(Path(project_dir))
        return cls.open_db()

    @classmethod
    def open_db(cls):
        db_location = _get_default_db_location()
        db_location.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(db_location, isolation_level="EXCLUSIVE")
        _create_workflow_table(db)
        return WorkflowDB(db)

    def __init__(self, db: sqlite3.Connection):
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, __exc_type, __exc_value, __traceback):
        self._db.commit()
        self._db.close()

    def save_workflow_run(
        self,
        workflow_run: StoredWorkflowRun,
    ):
        with self._db:
            self._db.execute(
                "INSERT INTO workflow_runs VALUES (?, ?, ?)",
                (
                    workflow_run.workflow_run_id,
                    workflow_run.config_name,
                    workflow_run.workflow_def.json(),
                ),
            )

    def get_workflow_run(self, workflow_run_id: WorkflowRunId) -> StoredWorkflowRun:
        """Return the StoredWorkflowRun of a previous workflow with the specified ID.

        Args:
            workflow_run_id: the ID of the workflow to be returned.

        Raises:
            WorkflowNotFoundError: raised when no matching workflow exists in the
            database.

        Returns:
            StoredWorkflowRun: the details of the stored workflow.
        """
        with self._db:
            cur = self._db.cursor()
            cur.execute(
                "SELECT * FROM workflow_runs WHERE workflow_run_id=?",
                (workflow_run_id,),
            )
            result = cur.fetchone()
            if result is None or len(result) == 0:
                raise WorkflowNotFoundError(
                    f"Workflow run with ID {workflow_run_id} not found"
                )
            return StoredWorkflowRun(
                workflow_run_id=result[0],
                config_name=result[1],
                workflow_def=WorkflowDef.parse_raw(result[2]),
            )

    def get_workflow_runs_list(
        self, prefix: Optional[str] = None, config_name: Optional[str] = None
    ) -> List[StoredWorkflowRun]:
        """
        Retrieve all workflow runs matching one or more conditions. If no conditions
        are set, returns all workflow runs.

        Arguments:
            prefix (Optional): Only return workflow runs whose IDs start with the
            prefix.
            config_name (Optional): Only return workflow runs that use the
            specified configuration name.

        Returns:
            A list of workflow runs for a given config. Includes: run ID, stored
            config, and WorkflowDef
        """
        query = "SELECT * FROM workflow_runs"

        if prefix is not None:
            query += f' WHERE workflow_run_id LIKE "{prefix}%"'

        if config_name is not None:
            if prefix is not None:
                query += " AND"
            else:
                query += " WHERE"
            query += f' config_name="{config_name}"'

        with self._db:
            cur = self._db.cursor()
            cur.execute(query)
            result = cur.fetchall()
        return [
            StoredWorkflowRun(
                workflow_run_id=row[0],
                config_name=row[1],
                workflow_def=WorkflowDef.parse_raw(row[2]),
            )
            for row in result
        ]
