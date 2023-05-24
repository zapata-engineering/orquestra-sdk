################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import re
import sys
import time
import typing as t
import warnings
from datetime import timedelta
from pathlib import Path

from ...exceptions import (
    ConfigFileNotFoundError,
    ConfigNameNotFoundError,
    ProjectInvalidError,
    UnauthorizedError,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotFoundError,
    WorkflowRunNotSucceeded,
)
from ...schema import ir
from ...schema.configs import ConfigName
from ...schema.local_database import StoredWorkflowRun
from ...schema.workflow_run import ProjectId, State, TaskInvocationId
from ...schema.workflow_run import WorkflowRun as WorkflowRunModel
from ...schema.workflow_run import WorkflowRunId, WorkflowRunMinimal, WorkspaceId
from .. import serde
from .._spaces._resolver import resolve_studio_project_ref
from ..abc import RuntimeInterface
from ._config import RuntimeConfig, _resolve_config
from ._task_run import TaskRun

COMPLETED_STATES = [State.FAILED, State.TERMINATED, State.SUCCEEDED]


class WorkflowRun:
    """
    Represents a single "execution" of a workflow. Used to get the workflow results.
    """

    @staticmethod
    def _get_stored_run(_project_dir: Path, run_id: WorkflowRunId) -> StoredWorkflowRun:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowNotFoundError: raised when no matching
            workflow exists in the database.
        """
        from orquestra.sdk._base._db import WorkflowDB

        # Get the run details from the database. Extracted from by_id method
        # to mock it in unit tests.
        with WorkflowDB.open_project_db(_project_dir) as db:
            return db.get_workflow_run(run_id)

    @classmethod
    def by_id(
        cls,
        run_id: str,
        config: t.Optional[t.Union["RuntimeConfig", str]] = None,
        project_dir: t.Optional[t.Union[Path, str]] = None,
    ) -> "WorkflowRun":
        """Get the WorkflowRun corresponding to a previous workflow run.

        Args:
            run_id: The id of the workflow run to be loaded.
            config: Determines where to look for the workflow run record. If omitted,
                we will retrieve the config name from a local cache of workflow runs
                submitted from this machine.
            project_dir: The location of the project directory. This directory must
                contain the workflows database to which this run was saved. If omitted,
                the current working directory is assumed to be the project directory.
            config_save_file: The location to which the associated configuration was
                saved. If omitted, the default config file path is used.

        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: when the run_id doesn't
                match a stored run ID.
            orquestra.sdk.exceptions.UnauthorizedError: when authorization with the
                remote runtime failed.
            orquestra.sdk.exceptions.ConfigFileNotFoundError: when the config file
                couldn't be read
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when there's no
                corresponding config entry in the config file.
        """
        _project_dir = Path(project_dir or Path.cwd())

        # Resolve config
        resolved_config: RuntimeConfig
        if config is None:
            # Shorthand: use the cached value.
            # We need to read the config name from the local DB and load the config
            # entry.
            try:
                stored_run = cls._get_stored_run(_project_dir, run_id)
            except WorkflowRunNotFoundError:
                raise

            try:
                resolved_config = RuntimeConfig.load(stored_run.config_name)
            except (ConfigFileNotFoundError, ConfigNameNotFoundError):
                raise
        else:
            resolved_config = _resolve_config(config)

        # Retrieve workflow def from the runtime:
        # - Ray stores wf def for us under a metadata entry.
        # - CE will have endpoints for getting [wf def] by [wf run ID]. See:
        #   https://zapatacomputing.atlassian.net/browse/ORQP-1317
        # - QE probably won't have endpoints for this, but the single-user limitation
        #   will be an implementation detail of `QERuntime`.
        runtime = resolved_config._get_runtime(_project_dir)
        try:
            wf_run_model = runtime.get_workflow_run_status(run_id)
        except (UnauthorizedError, WorkflowRunNotFoundError):
            raise

        workflow_run = WorkflowRun(
            run_id=run_id,
            wf_def=wf_run_model.workflow_def,
            runtime=runtime,
            config=resolved_config,
        )

        return workflow_run

    @classmethod
    def _start(cls, wf_def: ir.WorkflowDef, runtime, config, project):
        """
        Schedule workflow for execution and return WorkflowRun.
        """
        run_id = runtime.create_workflow_run(wf_def, project)

        workflow_run = WorkflowRun(
            run_id=run_id,
            wf_def=wf_def,
            runtime=runtime,
            config=config,
        )

        return workflow_run

    def __init__(
        self,
        run_id: WorkflowRunId,
        wf_def: ir.WorkflowDef,
        runtime: RuntimeInterface,
        config: t.Optional["RuntimeConfig"] = None,
    ):
        """
        Users aren't expected to use __init__() directly. Please use
        `WorkflowRun.by_id` or `WorkflowDef.run()`.

        Args:
            wf_def: the workflow being run. Workflow definition in the model
                (serializable) form.
            runtime: the adapter object used to interact with the runtime to
                submit workflow, get results, etc. Different "runtimes" like
                Ray or Quantum Engine have corresponding classes.
        """

        self._run_id = run_id
        self._wf_def = wf_def
        self._runtime = runtime
        self._config = config

    def __str__(self) -> str:
        outstr: str = ""

        outstr += f"WorkflowRun '{self._run_id}' with parameters:"
        if self._config is None:
            outstr += "\n- Runtime: In-process runtime."
        else:
            outstr += f"\n- Config name: {self._config.name}"
            outstr += f"\n- Runtime name: {self._config._runtime_name}"
        return outstr

    @property
    def config(self):
        """
        The configuration for this workflow run.
        """
        if self._config is None:
            no_config_message = (
                "This workflow run was created without a runtime configuration. "
            )
            no_config_message += "The default in-process runtime was used."
            warnings.warn(no_config_message)

        return self._config

    @property
    def run_id(self):
        """
        The run_id for this workflow run.
        """
        return self._run_id

    def wait_until_finished(self, frequency: float = 0.25, verbose=True) -> State:
        """Block until the workflow run finishes.

        This method draws no distinctions between whether the workflow run completes
        successfully, fails, or is terminated for any other reason.

        Args:
            frequency: The frequency in Hz at which the status should be checked.
            verbose: If ``True``, each iteration of the polling loop will print to
                stderr.

        Returns:
            State: The state of the finished workflow.
        """

        assert frequency > 0.0, "Frequency must be a positive non-zero value"

        status = self.get_status()

        while status == State.RUNNING or status == State.WAITING:
            sleep_time = 1.0 / frequency

            if verbose:
                print(
                    f"{self.run_id} is {status.name}. Sleeping for {sleep_time}s...",
                    file=sys.stderr,
                )

            time.sleep(sleep_time)
            status = self.get_status()

        if status not in [State.SUCCEEDED, State.TERMINATED, State.FAILED]:
            raise NotImplementedError(
                f'Workflow run with id "{self.run_id}" '
                f'finished with unrecognised state "{status}"'
            )

        if verbose:
            print(
                f"{self.run_id} is {status.name}",
                file=sys.stderr,
            )

        return status

    def stop(self):
        """
        Asks the runtime to stop the workflow run.

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError: when communication with runtime
                failed because of an auth error
            orquestra.sdk.exceptions.WorkflowRunCanNotBeTerminated if the termination
                attempt failed
        """
        try:
            self._runtime.stop_workflow_run(self.run_id)
        except (UnauthorizedError, WorkflowRunCanNotBeTerminated):
            raise

    def get_status(self) -> State:
        """
        Return the current status of the workflow.
        """
        return self.get_status_model().status.state

    def get_status_model(self) -> WorkflowRunModel:
        """
        Serializable representation of the workflow run state at a given point in time.
        """
        return self._runtime.get_workflow_run_status(self.run_id)

    def get_results(self, wait: bool = False) -> t.Sequence[t.Any]:
        """
        Retrieves workflow results, as returned by the workflow function.

        A workflow function is expected to return task outputs
        (ArtifactFutures) or constants (10, "hello", etc.). This method returns values
        of these. The order is dictated by the return statement in the workflow
        function, for example `return a, b, c` means this function returns (a, b, c).
        See also:
        https://refactored-disco-d576cb73.pages.github.io/docs/runtime/guides/workflow-syntax.html

        Args:
            wait:  whether or not to wait for workflow run completion.
                   Uses the default options for waiting, use `wait_until_finished()` for
                   more control.

        Raises:
            WorkflowRunNotFinished: when the workflow run has not finished and `wait` is
                                   False
            WorkflowRunNotSucceeded: when the workflow is no longer executing, but it did not
                succeed.
        """  # noqa 501
        if wait:
            self.wait_until_finished()

        if (state := self.get_status()) not in COMPLETED_STATES:
            raise WorkflowRunNotFinished(
                f"Workflow run with id {self.run_id} has not finished. "
                f"Current state: {state}",
                state,
            )
        try:
            results = (
                *(
                    serde.deserialize(o)
                    for o in self._runtime.get_workflow_run_outputs_non_blocking(
                        self.run_id
                    )
                ),
            )
        except WorkflowRunNotSucceeded:
            raise

        # If we only get one result back, return it directly rather than as a sequence
        if len(results) == 1:
            return results[0]

        return results

    def get_artifacts(self) -> t.Mapping[ir.TaskInvocationId, t.Any]:
        """
        Unstable: this API will change.

        Returns values calculated by this workflow's tasks. If a given task hasn't
        succeeded yet, the mapping won't contain the corresponding entry.

        Returns:
            A dictionary with an entry for each task run in the workflow. The key is the
                task's invocation ID. The value is whatever the task returned. If the
                task has 1 output, it's the dict entry's value. If the tasks has n
                outputs, the dict entry's value is a n-tuple.
        """
        # NOTE: this is a possible place for improvement. If future runtime APIs support
        # getting a subset of artifacts, we should use them here.
        inv_outputs = self._runtime.get_available_outputs(self.run_id)

        # The output shape differs across runtimes when the workflow functions returns a
        # single, packed future. See more in:
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-801
        return {
            inv_id: serde.deserialize(inv_output)
            for inv_id, inv_output in inv_outputs.items()
        }

    def get_logs(self) -> t.Mapping[TaskInvocationId, t.List[str]]:
        """
        Unstable: this API will change.

        Returns logs produced by all task runs in this workflow. If you're interested in
        only subset of tasks, consider using ``WorkflowRun.get_tasks()`` and
        ``TaskRun.get_logs()``.

        Returns:
            A dictionary where each key-value entry corresponds to a single task run.
            The key identifies a task invocation, a single node in the workflow graph.
            The value is a list of log lines produced by the corresponding task
            invocation while running this workflow.
        """
        return self._runtime.get_workflow_logs(wf_run_id=self.run_id)

    # TODO: ORQSDK-617 add filtering ability for the users
    def get_tasks(self) -> t.Set[TaskRun]:
        wf_run_model = self.get_status_model()

        return {
            TaskRun(
                task_run_id=task_run_model.id,
                task_invocation_id=task_run_model.invocation_id,
                workflow_run_id=self.run_id,
                runtime=self._runtime,
                wf_def=self._wf_def,
            )
            for task_run_model in wf_run_model.task_runs
        }


def list_workflow_runs(
    config: t.Union[ConfigName, "RuntimeConfig"],
    *,
    limit: t.Optional[int] = None,
    max_age: t.Optional[str] = None,
    state: t.Optional[t.Union[State, t.List[State]]] = None,
    project_dir: t.Optional[t.Union[Path, str]] = None,
    workspace: t.Optional[WorkspaceId] = None,
    project: t.Optional[ProjectId] = None,
) -> t.List[WorkflowRun]:
    """
    List the workflow runs, with some filters.

    Args:
        config: The name of the configuration to use.
        limit: Restrict the number of runs to return, prioritising the most recent.
        max_age: Only return runs younger than the specified maximum age.
        state: Only return runs of runs with the specified status.
        project_dir: The location of the project directory. This directory must
            contain the workflows database to which this run was saved. If omitted,
            the current working directory is assumed to be the project directory.
        workspace: Only return runs from the specified workspace when using CE.
        project: will be used to list workflows from specific workspace and project
            when using CE.

    Raises:
        ConfigNameNotFoundError: when the named config is not found in the file.
        NotImplementedError: when a filter is specified for a runtime that does not
            support it.

    Returns:
        a list of WorkflowRuns
    """
    # TODO: update docstring when platform workspace/project filtering is merged [ORQP-1479](https://zapatacomputing.atlassian.net/browse/ORQP-1479?atlOrigin=eyJpIjoiZWExMWI4MDUzYTI0NDQ0ZDg2ZTBlNzgyNjE3Njc4MDgiLCJwIjoiaiJ9) # noqa: E501

    if project and not workspace:
        raise ProjectInvalidError(
            f"The project `{project}` cannot be uniquely identified "
            "without a workspace parameter."
        )

    _project_dir = Path(project_dir or Path.cwd())

    # Resolve config
    resolved_config: RuntimeConfig = _resolve_config(config)
    # If user wasn't specific with workspace and project, we might want to resolve it
    if workspace is None and project is None:
        if _project := resolve_studio_project_ref(
            workspace, project, resolved_config.name
        ):
            workspace = _project.workspace_id
            project = _project.project_id

    # resolve runtime
    runtime = resolved_config._get_runtime(_project_dir)

    # Grab the "workflow runs" from the runtime.
    # Note: WorkflowRun means something else in runtime land. To avoid overloading, this
    #       import is aliased to WorkflowRunStatus in here.
    run_statuses: t.Sequence[WorkflowRunMinimal] = runtime.list_workflow_runs(
        limit=limit,
        max_age=_parse_max_age(max_age),
        state=state,
        workspace=workspace,
        project=project,
    )

    # We need to convert to the public API notion of a WorkflowRun
    runs = []
    for run_status in run_statuses:
        assert run_status.workflow_def is not None
        workflow_run = WorkflowRun(
            run_id=run_status.id,
            wf_def=run_status.workflow_def,
            runtime=runtime,
            config=resolved_config,
        )
        runs.append(workflow_run)
    return runs


def _parse_max_age(age: t.Optional[str]) -> t.Optional[timedelta]:
    """Parse a string specifying an age into a timedelta object.
    If the string cannot be parsed, an exception is raises.

    Args:
        age: the string to be parsed.

    Raises:
        ValueError if the age string cannot be parsed

    Returns:
        datetime.timedelta: the age specified by the 'age' string, as a timedelta.
        None: if age is None
    """
    if age is None:
        return None
    time_params = {}

    # time in format "{days}d{hours}h{minutes}m{seconds}s"

    # match one or more digits "\d+?" followed by a single character from the 'units'
    # list. Capture the digits in a group labelled 'name'
    time_capture_group = r"(?P<{name}>\d+?)[{units}]"

    re_string = ""
    name_units = (
        ("days", "dD"),
        ("hours", "hH"),
        ("minutes", "mM"),
        ("seconds", "sS"),
    )
    for name, units in name_units:
        # match each potential time unit capture group between 0 and 1 times.
        re_string += rf"({time_capture_group.format(name=name, units=units)})?"

    if parts := re.compile(re_string).fullmatch(age):
        for name, param in parts.groupdict().items():
            if param:
                time_params[name] = int(param)
        if len(time_params) > 0:
            return timedelta(**time_params)

    raise ValueError(
        'Time strings must be in the format "{days}d{hours}h{minutes}m{seconds}s". '
        "Accepted units are:\n"
        "- d or D: days\n"
        "- h or H: hours\n"
        "- m or M: minutes\n"
        "- s or S: seconds\n"
        "For example:\n"
        '- "9h32m" = 9 hours and 32 minutes,\n'
        '- "8H6S" = 8 hours and 6 seconds,\n'
        '- "10m" = 10 minutes,\n'
        '- "3D6h8M13s" = 3 days, 6 hours, 8 minutes and 13 seconds.'
    )
