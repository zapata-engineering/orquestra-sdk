################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import argparse
import datetime as dt
import typing as t
from pathlib import Path

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _factory
from orquestra.sdk._base._db import WorkflowDB
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.responses import (
    ListWorkflowRunsResponse,
    ResponseMetadata,
    ResponseStatusCode,
)
from orquestra.sdk.schema.workflow_run import State, WorkflowRun


def _orq_list_workflow_runs(
    limit: t.Optional[int],
    prefix: t.Optional[str],
    max_age: t.Optional[dt.timedelta],
    status: t.Optional[State],
    config: t.Optional[RuntimeConfiguration],
    project_dir: t.Union[str, Path],
) -> t.List[WorkflowRun]:
    """Get all workflow runs matching a set of user-defined parameters. Any parameters
    that are not defined are assumed to match all workflow runs. If no parameters are
    set, returns all workflow runs in the database.

    Prefix and config name can both be handled by sqlite3 queries, so these are passed
    through to db.list_workflow_runs. Max_age and status both require that we build a
    runtime to get the run information, so we handle those here.

    Args:
        -limit (optional): Restrict the number of runs to return, prioritizing the most
        recent. Workflows with no start time are listed last.
        - prefix (Optional): Only return runs that start with the specified string.
        - max_age (Optional): Only return runs younger than the specified maximum age.
        Workflows with no start time are omitted.
        - status (Optional): Only return runs of runs with the specified status.
        - config (Optional): Only return runs of runs with the specified config.
        - project_dir: the current project directory.

    Returns:
        GetWorkflowRunResponse

    Raises:
        exceptions.NotFoundError: Raised from _factory.build_runtime_from_config when
        the runtime name specified in the config is not recognised.
        FileNotFoundError: Raised from read_config when the config file and/or lock
        file do not exist
        NotFoundError: Raised from read_config when a specific runtime config does not
        exist.
    """
    _project_dir = Path(project_dir)

    # Get the list of workflow runs from the database, filtering by prefix and config
    # name.
    with WorkflowDB.open_project_db(_project_dir) as db:
        stored_runs = db.get_workflow_runs_list(
            prefix=prefix, config_name=config.config_name if config else None
        )
    if len(stored_runs) == 0:
        return []

    # For each unique config name in the list of runs, build the corresponding
    # runtime.
    runtimes_by_config: dict = {}
    for config_name in set([run.config_name for run in stored_runs]):
        runtimes_by_config[config_name] = _factory.build_runtime_from_config(
            _project_dir, _config.read_config(config_name)
        )

    # Get the status of each run in the list using the corresponding runtime.
    # Ignore any workflows that are no longer available in the runtime
    runs = []
    for run in stored_runs:
        try:
            runs.append(
                runtimes_by_config[run.config_name].get_workflow_run_status(
                    run.workflow_run_id
                )
            )
        except exceptions.NotFoundError:
            pass

    # Filter by status.
    if status is not None:
        runs = [run for run in runs if run.status.state == status]

    # Filter by age by excluding workflows with no start time (prioritizing youngest).
    if max_age is not None:
        now = dt.datetime.now(dt.timezone.utc)
        runs = sorted(
            [
                run
                for run in runs
                if (run.status.start_time is not None)
                and (now - run.status.start_time <= max_age)
            ],
            key=lambda run: run.status.start_time,
            reverse=True,
        )

    # Limit number of runs to return (prioritizing runs with start time and youngest).
    if limit is not None:
        limit = max(0, min(limit, len(runs)))
        runs = (
            sorted(
                [run for run in runs if run.status.start_time is not None],
                key=lambda run: run.status.start_time,
                reverse=True,
            )
            + [run for run in runs if run.status.start_time is None]
        )[:limit]

    return runs


def orq_list_workflow_runs(args: argparse.Namespace) -> ListWorkflowRunsResponse:
    """Get all workflow runs matching a set of user-defined parameters.

    Args:
        parsed args containing at least the following parameters:
        - limit: Restrict the number of runs to return, prioritizing the most
        recent. Workflows with no start time are listed last.
        - prefix: Only return runs that start with the specified string.
        - max_age: Only return runs younger than the specified maximum age.
        Workflows with no start time are omitted.
        - status: Only return runs of runs with the specified status.
        - config_name: Only return runs of runs with the specified config name.
        If omitted, default-config is used
        - additional_project_dirs: Additional directories to be searched.
        - project_dir: the current project directory.
        - all: return runs from all configs

    Returns:
        GetWorkflowRunResponse

    Raises:
        exceptions.NotFoundError: Raised from _factory.build_runtime_from_config when
            the runtime name specified in the config is not recognised.
        FileNotFoundError: Raised from read_config when the config file and/or lock
            file do not exist
        NotFoundError: Raised from read_config when a specific runtime config does not
            exist.
    """

    # Construct list of directories to search
    project_dirs = set(
        [Path(args.directory)] + [Path(dir) for dir in args.additional_project_dirs]
    )
    config = _config.read_config(args.config) if not args.all else None
    # Search the database in each directory for matching workflow runs
    runs = []
    for project_dir in project_dirs:
        runs += _orq_list_workflow_runs(
            args.limit,
            args.prefix,
            args.max_age,
            args.status,
            config,
            project_dir,
        )

    message = f"Found {len(runs)} matching workflow run{'' if len(runs)==1 else 's'}."
    return ListWorkflowRunsResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message=message,
        ),
        workflow_runs=runs,
        filters={
            "prefix": args.prefix,
            "max_age": args.max_age,
            "status": args.status,
            "config_name": args.config,
            "project_directories": project_dirs,
        },
    )
