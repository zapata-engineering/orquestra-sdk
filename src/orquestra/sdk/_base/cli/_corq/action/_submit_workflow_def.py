################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to submit workflow to be executed on a remote or local engine.

Usage for V2 Workflows:
    orq submit workflow-def [workflow def name] [-o text|json] [-d dir]
"""
import argparse
import logging
import typing as t
import warnings
from pathlib import Path

import orquestra.sdk._base._factory as _factory
from orquestra.sdk import WorkflowTemplate, exceptions
from orquestra.sdk._base import _config, loader
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.responses import (
    ErrorResponse,
    ResponseMetadata,
    ResponseStatusCode,
    SubmitWorkflowDefResponse,
)
from orquestra.sdk.schema.workflow_run import WorkflowRunOnlyID


def submit(
    workflow_def_name: t.Optional[str],
    project_dir: Path,
    resolved_config: RuntimeConfiguration,
    verbose: bool,
    ignore_dirty_repo: bool = False,
) -> SubmitWorkflowDefResponse:
    """Submit a workflow run.

    Raises:
        exceptions.InvalidWorkflowDefinitionError: raised when the number of workflows
            identified is not equal to 1.
        FileNotFoundError: raised from get_workflow_defs_module
    """
    workflow_defs_module = loader.get_workflow_defs_module(str(project_dir))

    if workflow_def_name:
        workflow_def: WorkflowTemplate = getattr(
            workflow_defs_module, workflow_def_name
        )
        if not isinstance(workflow_def, WorkflowTemplate):
            raise exceptions.InvalidWorkflowDefinitionError(
                "Provided workflow definition name is not a workflow function."
                " Did you miss @workflow decorator?"
            )
    else:
        workflow_defs: t.List[WorkflowTemplate] = loader.get_attributes_of_type(
            workflow_defs_module, WorkflowTemplate
        )
        if len(workflow_defs) == 0:
            raise exceptions.InvalidWorkflowDefinitionError(
                "No workflow definitions found in project."
            )
        elif len(workflow_defs) == 1:
            workflow_def = workflow_defs[0]
        else:
            raise exceptions.InvalidWorkflowDefinitionError(
                "Multiple workflow definitions found in project: "
                f"{', '.join([wf().name for wf in workflow_defs])}. "
                "Please specify the workflow definition name you would "
                "like to run as a command option."
            )

    try:
        with warnings.catch_warnings():
            if not ignore_dirty_repo:
                warnings.filterwarnings("error", category=exceptions.DirtyGitRepo)
            wf_def_model = workflow_def.model
    except exceptions.WorkflowSyntaxError as e:
        # Unfortunately the CLI and SDK have separate sets of exceptions.
        # CLI error handling is based on our exception classes,
        # so we need to wrap the error.
        raise exceptions.WorkflowDefinitionSyntaxError(e.msg) from e
    except exceptions.DirtyGitRepo as e:
        raise exceptions.DirtyGitRepoError(str(e)) from e

    runtime = _factory.build_runtime_from_config(
        project_dir=project_dir,
        config=resolved_config,
        verbose=verbose,
    )
    workflow_run_id = runtime.create_workflow_run(wf_def_model)

    return SubmitWorkflowDefResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Successfully submitted workflow.",
        ),
        workflow_runs=[WorkflowRunOnlyID(id=workflow_run_id)],
    )


def orq_submit_workflow_def(
    args: argparse.Namespace,
) -> t.Union[ErrorResponse, SubmitWorkflowDefResponse]:
    "Submit a V2 workflow to Qrquestra runtime."

    project_dir = Path(args.directory)
    wf_name = args.workflow_def_name
    verbose = args.verbose == logging.DEBUG
    ignore_dirty_repo = args.force

    resolved_config = _config.read_config(args.config)
    return submit(
        workflow_def_name=wf_name,
        project_dir=project_dir,
        resolved_config=resolved_config,
        verbose=verbose,
        ignore_dirty_repo=ignore_dirty_repo,
    )
