################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
CLI action to get workflow definitions from project.

Usage:
    orq get task-def [-o json]
"""

import argparse
import typing as t

from pydantic import ValidationError

import orquestra.sdk._base.loader as loader
from orquestra.sdk import TaskDef, exceptions
from orquestra.sdk.schema.responses import (
    GetTaskDefResponse,
    ResponseMetadata,
    ResponseStatusCode,
)


def _get_task_def(directory: str) -> GetTaskDefResponse:
    try:
        workflow_defs_module = loader.get_workflow_defs_module(directory)
    except ValidationError as e:
        raise exceptions.InvalidTaskDefinitionError(str(e)) from e
    except SyntaxError as e:
        raise exceptions.WorkflowDefinitionSyntaxError(str(e)) from e

    task_defs: t.List[TaskDef] = loader.get_attributes_of_type(
        workflow_defs_module, TaskDef
    )

    import_dict = {
        imp.id: imp for task_def in task_defs for imp in task_def.import_models
    }

    task_dict = {}
    for task_def in task_defs:
        _model = task_def.model
        task_dict[_model.id] = _model

    return GetTaskDefResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Successfully extracted task definitions.",
        ),
        task_defs=task_dict,
        imports=import_dict,
    )


def orq_get_task_def(
    args: argparse.Namespace,
) -> GetTaskDefResponse:
    "Get all task definitions in the current project."

    return _get_task_def(args.directory)
