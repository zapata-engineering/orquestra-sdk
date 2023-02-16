################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow run logs.

Usage:
    orq get logs [workflow run or taskid] [-o text|json] [-d dir] [-f|--follow]
"""

import argparse
import typing as t
from pathlib import Path

import orquestra.sdk._base._config as _config
from orquestra.sdk._base import _factory
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    GetLogsResponse,
    ResponseMetadata,
    ResponseStatusCode,
)
from orquestra.sdk.schema.workflow_run import TaskInvocationId


def _format_log_dict(logs: t.Dict[TaskInvocationId, t.List[str]]):
    # TODO: figure out a better log presentation, e.g. sort task invocations
    # topologically using the graph from workflow def.
    return [
        line
        for invocation_id, invocation_lines in logs.items()
        for line in (f"task-invocation-id: {invocation_id}", *invocation_lines)
    ]


def orq_get_logs(
    args: argparse.Namespace,
) -> t.Optional[t.Union[GetLogsResponse, ErrorResponse]]:
    """Get logs from a workflow run. Loops infinitely if "--follow" mode was selected"""

    config = _config.read_config(args.config)

    project_dir = Path(args.directory)
    runtime = _factory.build_runtime_from_config(project_dir=project_dir, config=config)

    if args.follow:
        raise NotImplementedError("Log streaming isn't implemented")
    else:
        # no follow/eager mode
        log_dict = runtime.get_workflow_logs(args.workflow_or_task_run_id)
        return GetLogsResponse(
            meta=ResponseMetadata(
                success=True,
                code=ResponseStatusCode.OK,
                message="Successfully got workflow run logs.",
            ),
            logs=_format_log_dict(log_dict),
        )
