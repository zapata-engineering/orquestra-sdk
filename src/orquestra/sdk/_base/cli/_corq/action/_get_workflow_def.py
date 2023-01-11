################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow definitions from project.

Usage:
    orq get workflow-def [-o json|text] [-d directory]
"""

import argparse
import typing as t

from pydantic import ValidationError

import orquestra.sdk._base.loader as loader
from orquestra.sdk import WorkflowTemplate, exceptions
from orquestra.sdk.schema.responses import (
    GetWorkflowDefResponse,
    ResponseMetadata,
    ResponseStatusCode,
)


def _get_workflow_def(directory: str) -> GetWorkflowDefResponse:
    try:
        workflow_defs_module = loader.get_workflow_defs_module(directory)
    except ValidationError as e:
        raise exceptions.InvalidWorkflowDefinitionError(
            "Invalid workflow definition."
        ) from e
    except SyntaxError as e:
        raise exceptions.WorkflowDefinitionSyntaxError from e

    workflow_defs: t.List[WorkflowTemplate] = loader.get_attributes_of_type(
        workflow_defs_module, WorkflowTemplate
    )

    try:
        workflow_def_models = [
            wf_def.model for wf_def in workflow_defs if not wf_def.is_parametrized
        ]
    except exceptions.WorkflowSyntaxError as e:
        # Unfortunately the CLI and SDK have separate sets of exceptions.
        # CLI error handling is based on our exception classes,
        # so we need to wrap the error.
        raise exceptions.WorkflowDefinitionSyntaxError(e.msg) from e

    return GetWorkflowDefResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Successfully extracted workflow definitions.",
        ),
        workflow_defs=workflow_def_models,
    )


def orq_get_workflow_def(
    args: argparse.Namespace,
) -> GetWorkflowDefResponse:
    "Get all workflow definitions in the current project."

    return _get_workflow_def(args.directory)
