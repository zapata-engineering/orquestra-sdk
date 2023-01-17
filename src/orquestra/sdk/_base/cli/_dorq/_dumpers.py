################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Code that stores values on disk as a result of a CLI command.
"""
import typing as t
from pathlib import Path

from orquestra.sdk._base import serde
from orquestra.sdk.schema.workflow_run import WorkflowRunId


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
    ) -> serde.DumpDetails:
        """
        Serialize artifact value and save it as a new file.

        Creates missing directories. Generates filenames based on ``wf_run_id`` and
        ``output_index``. Figures out the serialization format based on the object. The
        generated file extension matches the inferred format.

        No standard errors are expected to be raised.
        """

        dump_details = serde.dump_to_file(
            value=value,
            dir_path=dir_path,
            file_name_prefix=f"{wf_run_id}_{output_index}",
        )

        return dump_details
