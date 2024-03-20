################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Code that stores values on disk as a result of a CLI command."""
import typing as t
from functools import singledispatchmethod
from pathlib import Path

from orquestra.sdk._base import serde
from orquestra.sdk._base._logs._interfaces import WorkflowLogs
from orquestra.sdk.schema.workflow_run import TaskInvocationId, WorkflowRunId


class WFOutputDumper:
    """Writes a workflow run's output artifacts to a file."""

    def dump(
        self,
        value: t.Any,
        wf_run_id: WorkflowRunId,
        output_index: int,
        dir_path: Path,
    ) -> serde.DumpDetails:
        """Serialize a single artifact value and save it as a new file.

        Missing directories will be created.
        Filenames will be generate based on ``wf_run_id`` and ``output_index``.
        The serialization format and file extensions are inferred based on the object.

        No standard errors are expected to be raised.
        """
        dump_dir = dir_path / wf_run_id / "wf_results"

        dump_details = serde.dump_to_file(
            value=value, dir_path=dump_dir, file_name_prefix=str(output_index)
        )

        return dump_details


class TaskOutputDumper:
    """Writes task run's output artifact to a file."""

    def dump(
        self,
        value: t.Any,
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
        output_index: int,
        dir_path: Path,
    ) -> serde.DumpDetails:
        """Serialize artifact value and save it as a new file.

        Creates missing directories. Generates filenames based on ``wf_run_id`` and
        ``output_index``.
        Figures out the serialization format based on the object.
        The generated file extension matches the inferred format.

        No standard errors are expected to be raised.
        """
        dump_dir = dir_path / wf_run_id / "task_results" / task_inv_id

        dump_details = serde.dump_to_file(
            value=value,
            dir_path=dump_dir,
            file_name_prefix=str(output_index),
        )

        return dump_details


class LogsDumper:
    """Writes logs to files."""

    @staticmethod
    def _get_logs_file(
        dir_path: Path,
        wf_run_id: WorkflowRunId,
        log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None,
    ) -> Path:
        dir_path.mkdir(parents=True, exist_ok=True)
        if log_type:
            return (
                dir_path / f"{wf_run_id}_{log_type.value.lower().replace(' ', '_')}.log"
            )
        return dir_path / f"{wf_run_id}.log"

    def dump(
        self,
        logs: t.Union[t.Mapping[TaskInvocationId, t.Sequence[str]], t.Sequence[str]],
        wf_run_id: WorkflowRunId,
        dir_path: Path,
        log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None,
    ):
        """Save logs from wf into a file.

        Creates missing directories.
        Generates filenames based on ``wf_run_id``.
        No standard errors are expected to be raised.
        """
        logs_file = self._get_logs_file(dir_path, wf_run_id, log_type=log_type)

        log_lines = self._construct_output_log_lines(logs)

        with logs_file.open("w") as f:
            f.writelines(log_lines)

        return logs_file

    @singledispatchmethod
    @staticmethod
    def _construct_output_log_lines(_, *args) -> t.List[str]:
        """Construct a list of log lines to be printed.

        This method has overloads for dict and list arguments.
        """
        raise NotImplementedError(
            f"No log lines constructor for args {args}"
        )  # pragma: no cover

    @_construct_output_log_lines.register(dict)
    @staticmethod
    def _(logs: dict) -> t.List[str]:
        outlines = []
        for task_invocation in logs:
            outlines.append(f"Logs for task invocation: {task_invocation}:\n\n")
            for log in logs[task_invocation]:
                outlines.append(log + "\n")
            outlines.append("\n\n")
        return outlines

    @_construct_output_log_lines.register(list)
    @staticmethod
    def _(logs: list) -> t.List[str]:
        return [log + "\n" for log in logs]
