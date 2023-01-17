################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow run logs.

Usage:
    orq get logs [workflow run or taskid] [-o text|json] [-d dir] [-f|--follow]
"""

import argparse
import sys
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
        # Special case: instead of returning a full response we await new log line
        # batches and print as they arrive. This never exists unless the user sends
        # an interrupt.
        log_batch_iterator = runtime.iter_logs(args.workflow_or_task_run_id)

        print("Listening to logs. Press Ctrl-C to quit.", file=sys.stderr)

        for log_batch in log_batch_iterator:
            for log_line in log_batch:
                print(log_line)

        # This will never be executed, but we need it to appease mypy
        return None
    else:
        # no follow/eager mode
        log_dict = runtime.get_full_logs(args.workflow_or_task_run_id)
        return GetLogsResponse(
            meta=ResponseMetadata(
                success=True,
                code=ResponseStatusCode.OK,
                message="Successfully got workflow run logs.",
            ),
            logs=_format_log_dict(log_dict),
        )
