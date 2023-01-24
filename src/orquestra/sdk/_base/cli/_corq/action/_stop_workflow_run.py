################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow run results.

Usage for V2 Workflows:
    orq stop workflow-run <workflow-run-id> -o json
"""

import argparse
from pathlib import Path

import orquestra.sdk._base._config as _config
import orquestra.sdk._base._factory as _factory
from orquestra.sdk.schema.responses import (
    ResponseMetadata,
    ResponseStatusCode,
    StopWorkflowRunResponse,
)
from orquestra.sdk.schema.workflow_run import WorkflowRunOnlyID


def orq_stop_workflow_run(
    args: argparse.Namespace,
) -> StopWorkflowRunResponse:
    """Cancel a V2 workflow run.

    Args:
        args: Parsed CLI args.

    Returns:
        StopWorkflowRunResponse
    """

    run_id = args.workflow_run_id
    config = _config.read_config(args.config)

    project_dir = Path(args.directory)
    runtime = _factory.build_runtime_from_config(project_dir=project_dir, config=config)

    runtime.stop_workflow_run(run_id)

    return StopWorkflowRunResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Successfully terminated workflow.",
        ),
        workflow_runs=[WorkflowRunOnlyID(id=run_id)],
    )
