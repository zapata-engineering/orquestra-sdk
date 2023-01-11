################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from pathlib import Path


def migrate_project_db_to_shared_db(project_dir: Path):
    # Avoid circular import:
    from orquestra.sdk._base._db import WorkflowDB

    old_location = project_dir / ".orquestra" / "workflows.db"
    if not old_location.exists():
        return
    with WorkflowDB.open_db() as new_db:
        c = new_db._db.cursor()
        c.execute("BEGIN TRANSACTION")
        c.execute("ATTACH DATABASE ? AS old_db", (str(old_location),))
        c.execute("INSERT INTO workflow_runs SELECT * FROM old_db.workflow_runs")
        c.execute("COMMIT")
        c.execute("DETACH DATABASE old_db")
        c.close()

    old_location.rename(old_location.with_suffix(".old"))
