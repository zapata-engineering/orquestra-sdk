################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""
Class to get logs from Ray for particular Workflow, both historical and live.
"""
import typing as t
from pathlib import Path

from orquestra.sdk._base._logs import _markers, _regrouping
from orquestra.sdk._base._logs._interfaces import WorkflowLogs
from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import WorkflowRunId


class CapturedLogLines(t.NamedTuple):
    captured_lines: t.Sequence[str]
    workflow_run_id: WorkflowRunId
    task_invocation_id: TaskInvocationId


def _iter_logs_paths(ray_temp: Path) -> t.Iterator[Path]:
    seen_paths: t.MutableSet[Path] = set()
    for file_path in ray_temp.glob("session_*/logs/*"):
        real_path = file_path.resolve()
        if real_path in seen_paths:
            continue

        yield real_path

        seen_paths.add(real_path)


def iter_user_log_paths(ray_temp: Path) -> t.Iterator[Path]:
    return filter(_regrouping.is_worker, _iter_logs_paths(ray_temp))


def iter_env_log_paths(ray_temp: Path) -> t.Iterator[Path]:
    return filter(_regrouping.is_env_setup, _iter_logs_paths(ray_temp))


def _iter_log_lines(paths: t.Iterable[Path]) -> t.Iterator[bytes]:
    for path in paths:
        with path.open("rb") as f:
            yield from f


def iter_task_logs(
    worker_file_path: Path,
) -> t.Iterator[CapturedLogLines]:
    """
    A generator over logs contained in a Ray worker's output file

    Ray workers can be reused, so this generator has to handle a number of edge cases.
        - Happy path: a Ray worker has a single "task start" and "task end" marker.
        - a Ray worker has multiple "task start" and "task end" markers.
          This happens after a worker has being reused.
        - Two (or more) "task start" markers in a row.
          This may happen when if a worker is reused and the previous "task end" marker
          failed.
        - Missing "task end" marker.
          This may happen if a worker crashes or the "task end" marker is not captured.

    Args:
        worker_file_path: The path to the output of a Ray worker.

    Returns:
        A generator that yields batches of logs relating to specific workflow runs and
        task invocations.
    """

    with worker_file_path.open() as f:
        marker_context: t.Optional[_markers.TaskStartMarker] = None
        collected_lines: t.List[str] = []

        for line in f:
            if (marker := _markers.parse_line(line)) is not None:
                if isinstance(marker, _markers.TaskStartMarker):
                    if marker_context is not None:
                        # Seeing two start markers in the row: edge case when the end
                        # marker is missing. We want to return the logs to the user
                        # anyway. They might be noisy, but it's better than nothing.
                        yield CapturedLogLines(
                            collected_lines,
                            marker_context.wf_run_id,
                            marker_context.task_inv_id,
                        )

                    marker_context = marker
                    collected_lines = []
                elif isinstance(marker, _markers.TaskEndMarker):
                    if marker_context is not None:
                        yield CapturedLogLines(
                            collected_lines,
                            marker_context.wf_run_id,
                            marker_context.task_inv_id,
                        )
                        collected_lines = []

                    marker_context = None

            else:
                # This is a standard, non-marker line.
                collected_lines.append(line.strip())

        # If something goes wrong, e.g. Ray worker crashes, the log file can end without
        # the task end marker. We want to capture the logs anyway.
        if marker_context is not None:
            yield CapturedLogLines(
                collected_lines,
                marker_context.wf_run_id,
                marker_context.task_inv_id,
            )


class DirectRayReader:
    """
    Directly reads log files produced by Ray.
    Implements the ``LogReader`` interface.

    Requires ``ray_temp`` to be consistent with the path passed when initializing the
    Ray cluster. For example, if the cluster was started with::

        ray start --head  \
                --temp-dir="~/.orquestra/ray" \
                --storage="~/.orquestra/ray_storage"

    the ``ray_temp`` needs to be ``~/.orquestra/ray``.
    """

    def __init__(self, ray_temp: Path):
        """
        Args:
            ray_temp: directory where Ray keeps its data, like ``~/.orquestra/ray``.
        """
        self._ray_temp = ray_temp

    def _get_env_setup_lines(self) -> t.Sequence[str]:
        log_paths = iter_env_log_paths(self._ray_temp)
        log_line_bytes = _iter_log_lines(log_paths)

        return [line.decode() for line in log_line_bytes]

    def _get_system_log_lines(self) -> t.Sequence[str]:
        # There is currently no concrete rule for which log files fall into the
        # category of 'system'. Since the log files exist locally for the user, we
        # simply point them to the appropriate directory rather than trying to
        # construct the category for them.
        system_warning = (
            "WARNING: we don't parse system logs for the local runtime. "
            "The log files can be found in the directory "
            f"'{self._ray_temp}'"
        )
        return [system_warning]

    def _get_other_log_lines(self) -> t.Sequence[str]:
        other_warning = (
            "WARNING: we don't parse uncategorized logs for the local runtime. "
            "The log files can be found in the directory "
            f"'{self._ray_temp}'"
        )
        return [other_warning]

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> t.List[str]:
        collected_logs: t.List[str] = []
        for log_path in iter_user_log_paths(self._ray_temp):
            for logs_batch2, wf_run_id2, task_inv_id2 in iter_task_logs(log_path):
                if wf_run_id2 != wf_run_id:
                    continue

                if task_inv_id2 != task_inv_id:
                    continue

                collected_logs.extend(logs_batch2)
        return collected_logs

    def get_workflow_logs(self, wf_run_id: WorkflowRunId) -> WorkflowLogs:
        log_paths = iter_user_log_paths(self._ray_temp)
        logs_dict: t.Dict[TaskInvocationId, t.List[str]] = {}
        for log_path in log_paths:
            for logs_batch2, wf_run_id2, task_inv_id2 in iter_task_logs(log_path):
                if wf_run_id2 != wf_run_id:
                    continue

                logs_dict.setdefault(task_inv_id2, []).extend(logs_batch2)

        env_setup = self._get_env_setup_lines()
        system = self._get_system_log_lines()
        other = self._get_other_log_lines()

        return WorkflowLogs(
            per_task=logs_dict,
            env_setup=env_setup,
            system=system,
            other=other,
        )
