################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Code that stores values on disk as a result of a CLI command.
"""
import json
import typing as t
from dataclasses import dataclass
from functools import singledispatch
from pathlib import Path

import dill

from orquestra.sdk._base import serde
from orquestra.sdk.schema import responses
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.workflow_run import WorkflowRunId


@dataclass(frozen=True)
class DumpDetails:
    file_path: Path
    format: ArtifactFormat


@singledispatch
def _dump_result(result: responses.WorkflowResult, path: Path):
    raise TypeError(f"Unsupported result type: {type(result)}")


@_dump_result.register
def _(result: responses.JSONResult, path: Path):
    obj = serde.deserialize(result)
    with path.open("w") as f:
        json.dump(obj, f, indent=2)


@_dump_result.register
def _(result: responses.PickleResult, path: Path):
    obj = serde.deserialize(result)
    with path.open("wb") as f:
        dill.dump(obj, f)


def _format_extension(format: ArtifactFormat) -> str:
    if format == ArtifactFormat.JSON:
        return ".json"
    elif format == ArtifactFormat.ENCODED_PICKLE:
        return ".pickle"
    else:
        raise ValueError(f"Unsupported format: {format}")


class ArtifactDumper:
    """
    Writes artifacts to files.
    """

    def dump(
        self,
        value: t.Any,
        wf_run_id: WorkflowRunId,
        output_index: int,
        dir_path: Path,
    ) -> DumpDetails:
        """
        Serialize artifact value and save it as a new file.

        Creates missing directories. Generates filenames based on ``wf_run_id`` and
        ``output_index``. Figures out the serialization format based on the object. The
        generated file extension matches the inferred format.

        No standard errors are expected to be raised.
        """
        # Dumping to strings instead of directly to byte buffer is suboptimal, but we
        # wanna use the same serialization procedure as Corq and QE.
        result_obj = serde.result_from_artifact(
            value, artifact_format=ArtifactFormat.AUTO
        )
        selected_format = result_obj.serialization_format
        extension = _format_extension(selected_format)
        file_path = dir_path / f"{wf_run_id}_{output_index}{extension}"

        dir_path.mkdir(parents=True, exist_ok=True)
        _dump_result(result_obj, file_path)

        return DumpDetails(file_path=file_path, format=selected_format)
