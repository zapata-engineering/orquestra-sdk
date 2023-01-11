################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import argparse
import typing as t
import warnings
from json import JSONDecodeError
from pathlib import Path

import pydantic

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _factory
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    GetWorkflowRunResponse,
    ResponseMetadata,
    ResponseStatusCode,
)


def _get_workflow_run(
    run_id: t.Optional[str],
    project_dir: str,
    config: RuntimeConfiguration,
) -> GetWorkflowRunResponse:
    """Get workflow run information.

    Args:
        run_id (optional): id of the workflow run to be investigated. If omitted, the
        status of all workflow runs will be returned.
        project_dir: directory in which the project is stored.
        config: the RuntimeConfiguration object of the current runtime.

    Returns:
        GetWorkflowRunResponse

    Raises:
        exceptions.WorkflowNotFoundError: Raised when a specified run_id is not found.
        exceptions.InvalidWorkflowRunError: Raised when the status of a workflow run
        cannot be read.
    """
    _project_dir = Path(project_dir)

    runtime = _factory.build_runtime_from_config(_project_dir, config)

    try:
        if run_id:
            try:
                wf_runs = [runtime.get_workflow_run_status(run_id)]
            except exceptions.NotFoundError as e:
                raise exceptions.WorkflowNotFoundError(
                    f"Workflow run with ID {run_id} not found"
                ) from e
            message = f"Retrieved status of workflow run {run_id} successfully."
        else:
            warnings.warn(
                "`orq get workflow-run` without a workflow run ID is deprecated and "
                "will stop working in the future. Please use `orq list workflow-runs` "
                "instead.",
                FutureWarning,
            )
            wf_runs = runtime.get_all_workflow_runs_status()
            message = "Retrieved wf run statuses successfully."
    except (JSONDecodeError, pydantic.ValidationError) as e:
        raise exceptions.InvalidWorkflowRunError(
            "Validation error when reading workflow run status"
        ) from e

    return GetWorkflowRunResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message=message,
        ),
        workflow_runs=wf_runs,
    )


def orq_get_workflow_run(
    args: argparse.Namespace,
) -> t.Union[ErrorResponse, GetWorkflowRunResponse]:
    "Get all workflow definitions in the current project."
    config = _config.read_config(args.config)

    return _get_workflow_run(args.workflow_run_id, args.directory, config)
