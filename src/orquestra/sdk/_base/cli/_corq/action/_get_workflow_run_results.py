################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow run results.

Usage for V2 Workflows:
    orq get workflow-run-results [workflow run id] [-o text|json] [-d dir]
"""

import argparse
from pathlib import Path
from typing import Union

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _factory, serde
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    GetWorkflowRunResultsResponse,
    ResponseMetadata,
    ResponseStatusCode,
)


def _get_results_response(
    runtime: RuntimeInterface, directory: Path, run_id: str
) -> GetWorkflowRunResultsResponse:
    """Get results from a workflow run.

    Args:
        runtime
        directory: the orquestra project directory.
        run_id: the identified of the run for which we want the results.

    Raises:
        exceptions.InvalidWorkflowRunError: raised when the run_id is not a valid run
        identifier.

    Returns:
        GetWorkflowRunResultsResponse
    """
    try:
        run_outputs = runtime.get_workflow_run_outputs_non_blocking(run_id)
    except ValueError as e:
        raise exceptions.InvalidWorkflowRunError(
            f"Invalid workflow run id: {run_id}"
        ) from e

    # NOTE: this is a far fetched assumption that output artifact formats in the
    # workflow def are set to "AUTO". The assumption should hold as long as we don't let
    # SDK users to specify custom artifact format, e.g. via the metadata.
    #
    # If we didn't have this assumption, we'd have to retrieve the Workflow Def
    # corresponding to the `run_id` and get the artifact format from there.
    assumed_format = ArtifactFormat.AUTO
    results = [
        serde.result_from_artifact(
            artifact_value=output, artifact_format=assumed_format
        )
        for output in run_outputs
    ]

    return GetWorkflowRunResultsResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Successfully got workflow run results.",
        ),
        workflow_run_id=run_id,
        workflow_results=results,
    )


def orq_get_workflow_run_results(
    args: argparse.Namespace,
) -> Union[ErrorResponse, GetWorkflowRunResultsResponse]:
    "Get a V2 workflow run results."

    config = _config.read_config(args.config)

    project_dir = Path(args.directory)
    runtime = _factory.build_runtime_from_config(project_dir=project_dir, config=config)

    run_id = args.workflow_run_id
    return _get_results_response(runtime, project_dir, run_id=run_id)
