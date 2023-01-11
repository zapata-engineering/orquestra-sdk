################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow artifacts

Usage for V2 Workflows:
    orq get artifacts workflow_run_id [task_id] [-o text|json] [-d dir]
"""

import argparse
import typing
from pathlib import Path

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _factory
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.schema.responses import (
    GetArtifactsResponse,
    ResponseMetadata,
    ResponseStatusCode,
)


def _get_artifacts(
    run_id: str,
    task_id: typing.Optional[str],
    runtime: RuntimeInterface,
) -> GetArtifactsResponse:

    artifacts = runtime.get_available_outputs(run_id)

    if task_id is not None:
        if task_id not in artifacts:
            raise exceptions.InvalidTaskIDError
        # return artifacts only for required task
        artifacts = {task_id: artifacts[task_id]}

    return GetArtifactsResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Successfully extracted workflow artifacts.",
        ),
        artifacts=artifacts,
    )


def orq_get_artifacts(
    args: argparse.Namespace,
) -> GetArtifactsResponse:

    config = _config.read_config(args.config)

    project_dir = Path(args.directory)
    runtime = _factory.build_runtime_from_config(project_dir=project_dir, config=config)

    return _get_artifacts(args.workflow_run_id, args.task_id, runtime)
