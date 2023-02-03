################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Redesigned, user-facing SDK API.
"""

import json
import logging
import os
import re
import time
import typing as t
import warnings
from collections import namedtuple
from datetime import timedelta
from itertools import chain
from pathlib import Path

from packaging.version import parse as parse_version

from orquestra.sdk.schema import ir
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    ConfigName,
    RuntimeConfiguration,
    RuntimeName,
)
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import State, TaskInvocationId
from orquestra.sdk.schema.workflow_run import TaskRun as TaskRunModel
from orquestra.sdk.schema.workflow_run import TaskRunId
from orquestra.sdk.schema.workflow_run import WorkflowRun as WorkflowRunModel
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from ..exceptions import (
    ConfigFileNotFoundError,
    ConfigNameNotFoundError,
    RuntimeConfigError,
    TaskRunNotFound,
    UnauthorizedError,
    UnsavedConfigChangesError,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotFoundError,
    WorkflowRunNotStarted,
    WorkflowRunNotSucceeded,
)
from . import _config
from .abc import ArtifactValue, RuntimeInterface
from .serde import deserialize_constant

COMPLETED_STATES = [State.FAILED, State.TERMINATED, State.SUCCEEDED]


def _resolve_config(
    config: t.Union[ConfigName, "RuntimeConfig"],
) -> "RuntimeConfig":
    if isinstance(config, RuntimeConfig):
        # EZ. Passed-in explicitly.
        resolved_config = config
    elif isinstance(config, str):
        # Shorthand: just the config name.
        resolved_config = RuntimeConfig.load(config)
    else:
        raise TypeError(f"'config' is of unsupported type {type(config)}.")

    return resolved_config


class TaskRun:
    """
    Represents execution of a single task.
    """

    class _InputUnavailable:
        def __str__(self):
            return "Input for a task is Unavailable"

        def __repr__(self):
            return "InputUnavailable()"

    INPUT_UNAVAILABLE = _InputUnavailable()

    Inputs = namedtuple("Inputs", "args kwargs")

    def __init__(
        self,
        task_run_id: TaskRunId,
        task_invocation_id: TaskInvocationId,
        workflow_run_id: WorkflowRunId,
        runtime: RuntimeInterface,
        wf_def: ir.WorkflowDef,
    ):
        """
        This object isn't intended to be directly initialized. Instead, please use
        `WorkflowRun.get_tasks()`.
        """
        self._task_run_id = task_run_id
        self._task_invocation_id = task_invocation_id
        self._runtime = runtime
        self._wf_def = wf_def
        self._workflow_run_id = workflow_run_id

        # get the fn_ref from wf_def
        invocation = wf_def.task_invocations[self.task_invocation_id]
        task_def = wf_def.tasks[invocation.task_id]
        fn_ref = task_def.fn_ref
        self._fn_name = fn_ref.function_name

        # inline function ref doesn't have module
        self._module: t.Optional[str]
        if isinstance(fn_ref, ir.ModuleFunctionRef):
            self._module = fn_ref.module
        else:
            self._module = None

    @property
    def task_run_id(self) -> TaskRunId:
        return self._task_run_id

    @property
    def task_invocation_id(self) -> TaskInvocationId:
        return self._task_invocation_id

    @property
    def fn_name(self) -> str:
        return self._fn_name

    @property
    def workflow_run_id(self) -> WorkflowRunId:
        return self._workflow_run_id

    @property
    def module(self) -> t.Optional[str]:
        return self._module

    def __str__(self):
        return (
            f"TaskRun {self.task_run_id}, mod={self.module}, fn={self.fn_name}, "
            f"wf_run_id={self.workflow_run_id}, task_inv_id={self.task_invocation_id}"
        )

    def get_status(self) -> State:
        """
        Fetch current status from the runtime
        """
        wf_run_model = self._runtime.get_workflow_run_status(self.workflow_run_id)
        task_run_model = next(
            run
            for run in wf_run_model.task_runs
            if run.invocation_id == self.task_invocation_id
        )
        return task_run_model.status.state

    def get_logs(self) -> t.List[str]:
        logs_dict = self._runtime.get_full_logs(self.task_run_id)
        # NOTE: the line below will fail for Ray-produced logs until
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-676 is fixed.
        # However, this is a bug, so we don't describe this exception in this method's
        # signature, nor the functions that wrap it.
        try:
            return logs_dict[self.task_invocation_id]
        except KeyError as e:
            raise NotImplementedError(
                "Reading single task logs isn't supported yet for this runtime."
            ) from e

    def get_outputs(self) -> t.Any:
        """
        Get values calculated by this task run.

        Raises:
            TaskRunNotFound: if the task wasn't completed yet, or the ID is invalid.
        """
        # NOTE: this is a possible place for improvement. If future runtime APIs support
        # getting a subset of artifacts, we should use them here.
        workflow_artifacts = self._runtime.get_available_outputs(self.workflow_run_id)

        try:
            filtered_artifacts = {
                self.task_invocation_id: workflow_artifacts[self.task_invocation_id]
            }
        except KeyError as e:
            raise TaskRunNotFound(
                f"Output for task `{self.task_invocation_id}` in "
                f"`{self.workflow_run_id}` wasn't found. "
                "It may have failed or not be completed yet."
            ) from e

        # The runtime always returns tuples, even of the task has n_outputs = 1. We
        # need to unwrap it for user's convenience.
        return _unwrap(
            artifacts=filtered_artifacts,
            task_invocations=self._wf_def.task_invocations,
        )[self.task_invocation_id]

    def _find_invocation_by_output_id(self, output: ir.ArgumentId) -> ir.TaskInvocation:
        """
        Helper method that locates the invocation object responsible for returning
        output with given ID
        It is based on the assumption that output IDs are unique, and one output
        is returned by a single task invocation (but one invocation can produce
        multiple outputs)
        """
        return next(
            inv
            for inv in self._wf_def.task_invocations.values()
            if output in inv.output_ids
        )

    def _find_value_by_id(
        self,
        arg_id: ir.ArgumentId,
        available_outputs: t.Mapping[TaskInvocationId, t.Tuple[ArtifactValue, ...]],
    ) -> ArtifactValue:
        """
        Helper method that finds and deserializes input artifact value based on the
        artifact/argument ID. Uses values from available_outputs, or deserializes the
        constant embedded in the workflow def.
        """
        if arg_id in self._wf_def.constant_nodes:
            value = deserialize_constant(self._wf_def.constant_nodes[arg_id])
            return value

        producer_inv = self._find_invocation_by_output_id(arg_id)
        output_index = producer_inv.output_ids.index(arg_id)
        try:
            parent_output_vals = available_outputs[producer_inv.id]
        except KeyError:
            # Parent invocation ID not in available outputs => parent invocation
            # wasn't completed yet.
            return self.INPUT_UNAVAILABLE

        # A task function can return multiple values. We're only interested in one
        # of them.
        #
        # Assumption 1: RuntimeInterface.get_available_outputs() returns tuples even
        # for tasks with single output.
        #
        # Assumption 2: either a task wasn't completed yet and we don't have any
        # outputs (handled above) or we have access to all outputs of this task.
        # There shouldn't be a situation where we have access to a subset of a given
        # task's outputs.
        return parent_output_vals[output_index]

    def get_inputs(self) -> Inputs:
        """
        Get values of inputs (parameters) of a task run

        Returns:  Input namedTuple with Input.args and .kwargs parameters.
        """

        all_inv_outputs = self._runtime.get_available_outputs(self.workflow_run_id)

        task_invocation = self._wf_def.task_invocations[self.task_invocation_id]
        args = [
            self._find_value_by_id(arg_id, all_inv_outputs)
            for arg_id in task_invocation.args_ids
        ]
        kwargs = {
            param_name: self._find_value_by_id(kwarg_id, all_inv_outputs)
            for param_name, kwarg_id in task_invocation.kwargs_ids.items()
        }

        return self.Inputs(args, kwargs)

    def get_parents(self) -> t.Set["TaskRun"]:
        """
        Get parent tasks - tasks whose return values are taken as input parameters
        """
        # Note: theoretically there might be a mismatch between task invocations and
        # task runs. Task invocations come from the static workflow definition. Task
        # runs only exist if a given task invocation was scheduled for execution. If a
        # given invocation is waiting to be run, there might be no corresponding task
        # run.
        #
        # However, this method is about getting parents. We can assume that if a task
        # run A exists, its always parents exist as well; otherwise invocation A
        # wouldn't be scheduled and task run A wouldn't exist in the first place.

        # 1. Get parent invocation IDs
        task_invocation = self._wf_def.task_invocations[self.task_invocation_id]
        parent_inv_ids = set()
        # for every arg in the function
        for arg_id in chain(
            task_invocation.args_ids, task_invocation.kwargs_ids.values()
        ):
            # If it's constant, there is no parent task that produces it
            if arg_id in self._wf_def.constant_nodes:
                continue
            # find invocation that produces it
            parent_inv = self._find_invocation_by_output_id(arg_id)
            parent_inv_ids.add(parent_inv.id)

        # 2. Get parent task run models
        wf_run_model = self._runtime.get_workflow_run_status(self.workflow_run_id)
        parent_run_models: t.Sequence[TaskRunModel] = [
            run for run in wf_run_model.task_runs if run.invocation_id in parent_inv_ids
        ]

        # 3. Wrap in convenience objects
        parents = [
            TaskRun(
                task_run_id=model.id,
                task_invocation_id=model.invocation_id,
                workflow_run_id=self.workflow_run_id,
                runtime=self._runtime,
                wf_def=self._wf_def,
            )
            for model in parent_run_models
        ]

        return set(parents)


def _unwrap(
    artifacts: t.Mapping[ir.TaskInvocationId, ArtifactValue],
    task_invocations: t.Mapping[TaskInvocationId, ir.TaskInvocation],
) -> t.Mapping[ir.TaskInvocationId, t.Union[ArtifactValue, t.Tuple[ArtifactValue]]]:
    unwrapped_dict = {}
    for inv_id, outputs in artifacts.items():
        inv = task_invocations[inv_id]
        if len(inv.output_ids) == 1:
            unwrapped = outputs[0]
        else:
            unwrapped = outputs

        unwrapped_dict[inv_id] = unwrapped
    return unwrapped_dict


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

    def __init__(
        self,
        run_id: t.Optional[WorkflowRunId],
        wf_def: ir.WorkflowDef,
        runtime: RuntimeInterface,
        config: t.Optional["RuntimeConfig"] = None,
    ):
        """
        Users aren't expected to use __init__() directly. Please use
        `WorkflowRun.by_id`, `WorkflowDef.prepare()`, or `WorkflowDef.run()`.

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
        if self._run_id is None:
            outstr += "Unstarted WorkflowRun with parameters:"
        else:
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
            if self._run_id is None:
                no_config_message += (
                    "The default in-process runtime will be used at execution."
                )
            else:
                no_config_message += "The default in-process runtime was used."
            warnings.warn(no_config_message)

        return self._config

    @property
    def run_id(self):
        """
        The run_id for this workflow run.

        Raises:
            WorkflowRunNotStarted: when the workflow run has not started
        """
        workflow_not_started_message = (
            "Cannot get the run id of workflow run that hasn't started yet. "
            "You will need to call the `.start()` method prior accessing this property."
        )
        if self._run_id is None:
            raise WorkflowRunNotStarted(workflow_not_started_message)
        return self._run_id

    def start(self):
        """
        Schedule workflow for execution.
        """
        run_id = self._runtime.create_workflow_run(self._wf_def)
        self._run_id = run_id

    def wait_until_finished(self, frequency: float = 0.25) -> State:
        """Block until the workflow run finishes.

        This method draws no distinctions between whether the workflow run completes
        successfully, fails, or is terminated for any other reason.

        Args:
            frequency: The frequence in Hz at which the status should be checked.

        Raises:
            WorkflowRunNotStarted: when the workflow run has not started

        Returns:
            State: The state of the finished workflow.
        """

        assert frequency > 0.0, "Frequency must be a positive non-zero value"

        try:
            status = self.get_status()
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot wait for the completion of workflow run that hasn't started "
                "yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        while status == State.RUNNING or status == State.WAITING:
            time.sleep(1.0 / frequency)
            status = self.get_status()

        if status not in [State.SUCCEEDED, State.TERMINATED, State.FAILED]:
            raise NotImplementedError(
                f'Workflow run with id "{self.run_id}" '
                f'finished with unrecognised state "{status}"'
            )

        return status

    def stop(self):
        """
        Asks the runtime to stop the workflow run.

        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotStarted: when the workflow run was
                not started yet
            orquestra.sdk.exceptions.UnauthorizedError: when communication with runtime
                failed because of an auth error
            orquestra.sdk.exceptions.WorkflowRunCanNotBeTerminated if the termination
                attempt failed
        """
        try:
            run_id = self.run_id
        except WorkflowRunNotStarted:
            raise

        try:
            self._runtime.stop_workflow_run(run_id)
        except (UnauthorizedError, WorkflowRunCanNotBeTerminated):
            raise

    def get_status(self) -> State:
        """
        Return the current status of the workflow.

        Raises:
            WorkflowRunNotStarted: when the workflow run has not started
        """
        return self.get_status_model().status.state

    def get_status_model(self) -> WorkflowRunModel:
        """
        Serializable representation of the workflow run state at a given point in time.

        Raises:
            WorkflowRunNotStarted: if the workflow wasn't started yet.
        """
        try:
            run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get the status of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e
        return self._runtime.get_workflow_run_status(run_id)

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
            WorkflowRunNotStarted: when the workflow run has not started
            WorkflowRunNotFinished: when the workflow run has not finished and `wait` is
                                   False
            WorkflowRunNotSucceeded: when the workflow is no longer executing, but it did not
                succeed.
        """  # noqa 501
        try:
            run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get the results of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        if wait:
            self.wait_until_finished()

        if (state := self.get_status()) not in COMPLETED_STATES:
            raise WorkflowRunNotFinished(
                f"Workflow run with id {run_id} has not finished. "
                f"Current state: {state}",
                state,
            )
        try:
            return self._runtime.get_workflow_run_outputs_non_blocking(run_id)
        except WorkflowRunNotSucceeded:
            raise

    def get_artifacts(self) -> t.Mapping[ir.TaskInvocationId, t.Any]:
        """
        Unstable: this API will change.

        Returns all values returned by this workflow's tasks.

        Raises:
            WorkflowRunNotStarted: when the workflow has not started

        Returns:
            A dictionary with an entry for each task run in the workflow. The key is the
                task's invocation ID. The value is whatever the task returned. If the
                task has 1 output, it's the dict entry's value. If the tasks has n
                outputs, the dict entry's value is a n-tuple.
        """

        try:
            run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get the values of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        # NOTE: this is a possible place for improvement. If future runtime APIs support
        # getting a subset of artifacts, we should use them here.
        workflow_artifacts = self._runtime.get_available_outputs(run_id)

        # The runtime always returns tuples, even of the task has n_outputs = 1. We
        # need to unwrap it for user's convenience.
        return _unwrap(
            artifacts=workflow_artifacts, task_invocations=self._wf_def.task_invocations
        )

    def get_logs(self) -> t.Mapping[TaskInvocationId, t.List[str]]:
        """
        Unstable: this API will change.

        Returns logs produced by all task runs in this workflow. If you're interested in
        only subset of tasks, consider using ``WorkflowRun.get_tasks()`` and
        ``TaskRun.get_logs()``.

        Raises:
            WorkflowRunNotStarted: when the workflow has not started

        Returns:
            A dictionary where each key-value entry correponds to a single task run.
            The key identifies a task invocation, a single node in the workflow graph.
            The value is a list of log lines produced by the corresponding task
            invocation while running this workflow.
        """
        try:
            wf_run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get the logs of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        task_logs = self._runtime.get_full_logs(wf_run_id)

        return task_logs

    # TODO: ORQSDK-617 add filtering ability for the users
    def get_tasks(self) -> t.Set[TaskRun]:
        try:
            wf_run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get tasks of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        wf_run_model = self.get_status_model()

        return {
            TaskRun(
                task_run_id=task_run_model.id,
                task_invocation_id=task_run_model.invocation_id,
                workflow_run_id=wf_run_id,
                runtime=self._runtime,
                wf_def=self._wf_def,
            )
            for task_run_model in wf_run_model.task_runs
        }


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


def list_workflow_runs(
    config: t.Union[ConfigName, "RuntimeConfig"],
    *,
    limit: t.Optional[int] = None,
    max_age: t.Optional[str] = None,
    state: t.Optional[t.Union[State, t.List[State]]] = None,
    project_dir: t.Optional[t.Union[Path, str]] = None,
) -> t.List[WorkflowRun]:
    """Get the WorkflowRun corresponding to a previous workflow run.

    Args:
        config_name: The name of the configuration to use.
        limit: Restrict the number of runs to return, prioritising the most recent.
        prefix: Only return runs that start with the specified string.
        max_age: Only return runs younger than the specified maximum age.
        status: Only return runs of runs with the specified status.
        project_dir: The location of the project directory. This directory must
            contain the workflows database to which this run was saved. If omitted,
            the current working directory is assumed to be the project directory.
        config_save_file: The location to which the associated configuration was
            saved. If omitted, the default config file path is used.

    Raises:
        ConfigNameNotFoundError: when the named config is not found in the file.

    Returns:
        a list of WorkflowRuns
    """
    _project_dir = Path(project_dir or Path.cwd())

    # Resolve config
    resolved_config = _resolve_config(config)

    runtime = resolved_config._get_runtime(_project_dir)

    # Grab the "workflow runs" from the runtime.
    # Note: WorkflowRun means something else in runtime land. To avoid overloading, this
    #       import is aliased to WorkflowRunStatus in here.
    run_statuses: t.List[WorkflowRunModel] = runtime.list_workflow_runs(
        limit=limit, max_age=_parse_max_age(max_age), state=state
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


def _build_runtime(
    project_dir: Path, runtime_configuration: RuntimeConfiguration
) -> RuntimeInterface:  # pragma: no cover - tested in runtime repo.
    """
    Extracted from RuntimeConfig._get_runtime() to be able to mock in unit tests.

    Hopefully we'll be able to get rid of this when we merge SDK and Runtime repos.
    """
    import orquestra.sdk._base._factory  # type: ignore

    try:
        return orquestra.sdk._base._factory.build_runtime_from_config(
            project_dir=project_dir, config=runtime_configuration
        )
    except KeyError as e:
        outstr = (
            f"Runtime configuration '{runtime_configuration.config_name}' "
            f"lacks the required field '{e}'."
        )
        if e == "temp_dir":
            outstr += (
                " You may need to migrate your config file using "
                "`sdk.migrate_config_file()`"
            )
        raise RuntimeConfigError(outstr) from e


class RuntimeConfig:
    """
    Encompasses the configuration with which a workflow can be run.
    Intended to be used with the WorkflowDef class to create a run with the desired
    configuration.

    If you want to submit workflows, please do not initialise RuntimeConfig objects
    directly. Instead, factory methods are provided for the supported runtimes.

    Example usage::

        # Create a config with the desired runtime
        config_in_process = RuntimeConfig.in_process()
        config_ray = RuntimeConfig.ray()
        config_ce = RuntimeConfig.ce()
        config_qe = RuntimeConfig.qe()

        # Create the workflow run and begin its execution
        run = wf.prepare(config_in_process)
        run.start()

        # Alternatively, to create and start in one step:
        run = wf.run(config_in_process)
    """

    def __init__(
        self,
        runtime_name: str,
        name: t.Optional[str] = None,
        bypass_factory_methods=False,
    ):
        if not bypass_factory_methods:
            raise ValueError(
                "Please use the appropriate factory method for your desired runtime. "
                "Supported runtimes are:\n"
                "`RuntimeConfig.in_process()` for in-process execution,\n"
                "`RuntimeConfig.qe()` for Quantum Engine,\n"
                "`RuntimeConfig.ray()` for local Ray.\n"
                "`RuntimeConfig.ce()` for Compute Engine. \n"
            )

        self._name = name
        try:
            self._runtime_name: RuntimeName = RuntimeName(runtime_name)
        except ValueError as e:
            raise ValueError(
                f'"{runtime_name}" is not a valid runtime name. Valid names are:\n'
                + "\n".join(f'"{x.value}"' for x in RuntimeName)
            ) from e
        self._config_save_file = _config._get_config_file_path()

    def __str__(self) -> str:
        outstr = (
            f"RuntimeConfiguration '{self._name}' " f"for runtime {self._runtime_name} "
        )
        params_str = " with parameters:"
        for key in _config.RUNTIME_OPTION_NAMES:
            try:
                params_str += f"\n- {key}: {getattr(self, key)}"
            except AttributeError:
                continue
        if params_str == " with parameters:":
            outstr += "."
        else:
            outstr += params_str

        return outstr

    def __eq__(self, other) -> bool:
        if not isinstance(other, RuntimeConfig):
            return False

        return (
            self._name == other._name
            and self._runtime_name == other._runtime_name
            and self._get_runtime_options() == other._get_runtime_options()
        )

    @property
    def name(self) -> t.Optional[str]:
        if self._name is None:
            warnings.warn(
                "You are trying to access the name of a RuntimeConfig instance that "
                "has not been named. "
                "This probably means that something is happening in the wrong order."
            )
        return self._name

    @name.setter
    def name(self, value: str):
        self._name = value

    def is_saved(self) -> bool:
        """
        Check whether the runtime configuration has been saved with its current
        attributes.

        Returns:
            bool: True if this RuntimeConfiguration has a corresponding file entry that
                matches its current properties, False otherwise.
        """
        if self._name in _config.SPECIAL_CONFIG_NAME_DICT:
            return True

        if self._name is None or self._config_save_file is None:
            return False

        try:
            saved_config = self.load(str(self.name))
        except (ConfigNameNotFoundError, FileNotFoundError):
            return False

        return saved_config == self

    def _get_runtime_options(self) -> dict:
        """
        Construct a dictionary of the current runtime options.

        This is intended to translate between the user-facing API layer where runtime
        options are attributes, to the backend where we want them as a dict we can pass
        around.
        """
        runtime_options: dict = {}
        for key in _config.RUNTIME_OPTION_NAMES:
            if hasattr(self, key):
                runtime_options[key] = getattr(self, key)
        return runtime_options

    # region factories
    @classmethod
    def in_process(
        cls,
        name: t.Optional[str] = None,
    ):
        """Factory method to generate RuntimeConfig objects for in-process runtimes.

        Args:
            name: deprecated. The passed value is ignored.

        Returns:
            RuntimeConfig
        """
        if name is not None:
            warnings.warn(
                "'name' is ignored for in_process() config",
                FutureWarning,
            )
        return RuntimeConfig("IN_PROCESS", "in_process", True)

    @classmethod
    def ray(
        cls,
        name: t.Union[str, None] = None,
    ) -> "RuntimeConfig":
        """
        Config for running workflows on Ray. Makes the SDK connect to a Ray
        cluster when you .prepare() the workflow. Requires starting the Ray
        cluster separately in the background via 'ray start --head
        --storage=...'.

        Args:
            name: deprecated. The passed value is ignored.
        """
        if name is not None:
            warnings.warn(
                "'name' is ignored for ray() config",
                FutureWarning,
            )
        config = RuntimeConfig("RAY_LOCAL", "local", True)
        setattr(config, "log_to_driver", False)
        setattr(config, "configure_logging", False)

        # The paths for 'storage' and 'temp_dir' should have been passed when starting
        # the cluster, not here. Let's keep these attributes on our config object anyway
        # to retain the consistent shape
        setattr(config, "storage", None)
        setattr(config, "temp_dir", None)
        setattr(config, "address", "auto")
        return config

    @classmethod
    def qe(
        cls,
        uri: str,
        token: str,
        name: t.Union[str, None] = None,
    ) -> "RuntimeConfig":
        """
        Config for running workflows on Quantum Engine/Orquestra Platform.

        Args:
            name: Deprecated - do not use. Will be auto-generated based on URI
            uri: Address of the QE cluster on which to run the workflow.
            token: Authorisation token for access to the cluster.
        """
        if name is not None:
            warnings.warn(
                "`creating qe` config with custom name will stop working "
                "in the future. Name will be autogenerated based on uri ",
                FutureWarning,
            )

        runtime_name = RuntimeName.QE_REMOTE
        config_name = _config.generate_config_name(runtime_name, uri)

        config = RuntimeConfig(
            runtime_name,
            name=config_name,
            bypass_factory_methods=True,
        )
        setattr(config, "uri", uri)
        setattr(config, "token", token)
        _config.save_or_update(config_name, runtime_name, config._get_runtime_options())

        return config

    @classmethod
    def ce(
        cls,
        uri: str,
        token: str,
        name: t.Union[str, None] = None,
    ) -> "RuntimeConfig":
        """
        Config for running workflows on Compute Engine.

        Args:
            name: Deprecated - do not use. Will be auto-generated based on URI
            uri: Address of the CE cluster on which to run the workflow.
            token: Authorisation token for access to the cluster.
        """
        if name is not None:
            warnings.warn(
                "`creating in_process` config with custom name will stop working "
                "in the future. Please use default 'in_process' name instead ",
                FutureWarning,
            )

        runtime_name = RuntimeName.CE_REMOTE
        config_name = _config.generate_config_name(runtime_name, uri)

        config = RuntimeConfig(
            runtime_name,
            name=config_name,
            bypass_factory_methods=True,
        )
        setattr(config, "uri", uri)
        setattr(config, "token", token)
        _config.save_or_update(config_name, runtime_name, config._get_runtime_options())

        return config

    # endregion factories
    def _get_runtime(self, project_dir: t.Optional[t.Union[str, Path]] = None):
        """Build the run

        Args:
            project_dir: the path to the project directory. If omitted, the current
                working directory is used.
        Raises:
            ModuleNotFoundError: when orquestra.sdk._base is not installed.

        Returns:
            Runtime: The runtime specified by the configuration.
        """

        _project_dir: Path = Path(project_dir or Path.cwd())

        runtime_options = {}
        for key in _config.RUNTIME_OPTION_NAMES:
            try:
                runtime_options[key] = getattr(self, key)
            except AttributeError:
                continue

        return _build_runtime(
            _project_dir,
            RuntimeConfiguration(
                config_name=str(self._name),
                runtime_name=self._runtime_name,
                runtime_options=runtime_options,
            ),
        )

    # region LOADING FROM FILE
    @classmethod
    def list_configs(
        cls,
    ) -> list:
        """List previously saved configurations.

        Args:
            config_save_file: The path to the config file from which to read. If
                omitted, the default config file will be used.

        Returns:
            list: list of configurations within the save file.
        """
        return _config.read_config_names() + list(_config.UNIQUE_CONFIGS)

    @classmethod
    def load(
        cls,
        config_name: str,
    ):
        """Load an existing configuration from a file.

        Args:
            config_name: The name of the configuration to be loaded.

        Raises:
            orquestra.sdk.exceptions.ConfigFileNotFoundError
            orquestra.sdk.exceptions.ConfigNameNotFoundError

        Returns:
            RuntimeConfig: The configuration as loaded from the file.
        """

        # Doing this check here covers us for cases where the config file doesn't
        # exist but the user is trying to load one of the built in configs. There's
        # not need to create the config file in this case.
        if config_name in _config.SPECIAL_CONFIG_NAME_DICT:
            return cls._config_from_runtimeconfiguration(
                _config.read_config(config_name)
            )

        # Get the data from the save file
        _config_save_file = _config._get_config_file_path()
        with open(_config_save_file, "r") as f:
            data = json.load(f)

        # Migrate the file if necessary
        file_version = parse_version(data["version"])
        current_version = parse_version(CONFIG_FILE_CURRENT_VERSION)
        if file_version < current_version:
            warnings.warn(
                f"The config file at {_config_save_file} is out of date and will be "
                "migrated to the current version "
                f"(file has version {data['version']}, "
                f"the current version is {CONFIG_FILE_CURRENT_VERSION})."
            )
            migrate_config_file()
            with open(_config_save_file, "r") as f:
                data = json.load(f)
        elif file_version > current_version:
            raise ConfigFileNotFoundError(
                f"The config file at {_config_save_file} is a higher version than this "
                "version of the SDK supports "
                f"(file has version {data['version']}, "
                f"SDK supports versions up to {CONFIG_FILE_CURRENT_VERSION}). "
                "Please check that your version of the SDK is up to date."
            )

        # Read in the config from the file.
        try:
            config_data: RuntimeConfiguration = _config.read_config(config_name)
        except ConfigNameNotFoundError as e:
            raise ConfigNameNotFoundError(
                f"No config with name '{config_name}' "
                f"found in file {_config_save_file}. "
                f"Available names are: {cls.list_configs()}"
            ) from e

        config = cls._config_from_runtimeconfiguration(config_data)
        config._config_save_file = _config_save_file

        return config

    @classmethod
    def load_default(
        cls,
    ) -> "RuntimeConfig":
        """Load the default configuration from a file.

        Args:
            config_save_file (optional): The path to the file in which configurations
                are stored. If omitted, the default file location is used.

        Returns:
            RuntimeConfig: The configuration as loaded from the file.
        """

        config_data: RuntimeConfiguration = _config.read_config(None)
        return cls._config_from_runtimeconfiguration(config_data)

    @classmethod
    def _config_from_runtimeconfiguration(
        cls, config: RuntimeConfiguration
    ) -> "RuntimeConfig":
        """
        Convert a RuntimeConfiguration object (as used by the under-the-hood
        mechanisms) to a RuntimeConfig (python API) object.

        Args:
            config: the RuntimeConfigration object to be converted (e.g. the return from
            _config.load()).
        """
        if config.runtime_name == RuntimeName.IN_PROCESS:
            return RuntimeConfig.in_process()
        elif config.runtime_name == RuntimeName.RAY_LOCAL:
            return RuntimeConfig.ray()

        interpreted_config = RuntimeConfig(
            config.runtime_name,
            config.config_name,
            bypass_factory_methods=True,
        )
        for key in config.runtime_options:
            setattr(interpreted_config, key, config.runtime_options[key])
        return interpreted_config

    # endregion LOADING FROM FILE

    def save(
        self,
        overwrite: bool = False,
    ):
        warnings.warn(
            "RuntimeConfig.save() function is deprecated and will stop working "
            "in the future. Creating runtimeConfig saves it by default",
            FutureWarning,
        )

    def update_saved_token(self, token: str):
        """
        Update the stored auth token for this configuration. This also updates the token
        in memory for this RuntimeConfig object.

        Args:
            token: the new token.

        Raises:
            SyntaxError: When this method is called for a runtime configuration that
                does not use an authorisation token.
            ConfigNameNotFoundError: When there is no stored token to update.
            UnsavedConfigChangesError: When there are unsaved changes to the token that
                clash with the provided token
        """

        if self._runtime_name not in [RuntimeName.QE_REMOTE]:
            raise SyntaxError(
                "This runtime configuration does not require an authorisation token. "
                "Nothing has been saved."
            )

        if self._config_save_file is None:
            raise ConfigNameNotFoundError(
                "This runtime configuration has not been previously saved, "
                "so there is no saved token to update. "
                "You probably want 'save()' instead."
            )
        assert (
            self._name is not None
        ), "We have a save location but not a name for this configuration. "

        old_config = self._config_from_runtimeconfiguration(
            _config.read_config(self._name)
        )
        if self._runtime_name != old_config._runtime_name:
            raise ConfigNameNotFoundError(
                f"A runtime configuration with name {self.name} exists, but relates to "
                "a different runtime ("
                f"this config: {self._runtime_name}, "
                f"saved config: {old_config._runtime_name}"
                "). Nothing has been saved. Please check the config name and try again."
            )
        uri, saved_uri = (getattr(self, "uri"), getattr(old_config, "uri"))
        if uri != saved_uri:
            raise ConfigNameNotFoundError(
                f"A runtime configuration with name {self.name} exists, but relates to "
                "a different cluster uri ("
                f"this config: {uri}, "
                f"saved config: {saved_uri}"
                "). Nothing has been saved. Please check the config name and try again."
            )

        new_runtime_options: dict = old_config._get_runtime_options()
        new_runtime_options["token"] = token

        # We have three token values: the one that this RuntimeConfig object had prior
        # to this method being called, the one saved in the file, and the one passed to
        # this method. We want to avoid any confusion about what is happening to each
        # of them.
        if token != getattr(self, "token"):
            if getattr(self, "token") == getattr(old_config, "token"):
                # This is the most expected scenario - the RuntimeConfig matches the
                # file, and the user has provided a new token.
                self.token = token
                _config.update_config(
                    config_name=self.name, new_runtime_options=new_runtime_options
                )
                logging.info(
                    f"Updated authorisation token written to '{self.name}' "
                    f"in file '{self._config_save_file}'. "
                    "The new token is ready to be used in this runtime configuration."
                )
            elif token == old_config.token:
                # The new token is already saved, but the current token in this config
                # is different.
                raise UnsavedConfigChangesError(
                    f"The specified token is already stored in '{self.name}' "
                    f"in file '{self._config_save_file}', however your configuration "
                    "has unsaved changes to the token. If you want to save your "
                    "changes, use `save()`. If you want to undo your changes you can "
                    "either load the saved configuration, or modify the `token` "
                    "attribute directly."
                )
            else:
                # None of the three tokens are the same
                raise UnsavedConfigChangesError(
                    "Your configuration has unsaved changes to the token that differ "
                    "from the specified token. If you want to save your changes, use "
                    "`save()`. If you want to undo your changes you can load the saved "
                    "configuration. If you want to modify your changes you can edit "
                    "the `token` attribute directly."
                )
        else:  # token == self.token
            if token == old_config.token:
                # The new token, current token, and stored token are all the same,
                # nothing to do.
                logging.info(
                    f"The specified token is already stored in '{self.name}' "
                    f"in file '{self._config_save_file}'."
                )
            else:
                # The new token and current token are the same, but the file is
                # different. This happens if the user has updated the token to this
                # value, then called this method with the same argument. Given that
                # they've asked twice, it's only polite to save it for them.
                _config.update_config(
                    config_name=self.name, new_runtime_options=new_runtime_options
                )
                logging.info(
                    f"Updated authorisation token written to '{self.name}' "
                    f"in file '{self._config_save_file}'. "
                    "The new token is ready to be used in this runtime configuration."
                )

    def _as_dict(self) -> dict:
        """Construct a dictionary with the configuration's parameters formatted in line
        with the config.json schema.

        Returns:
            dict: a dictionary in the format:

                {
                    "config_name": name,
                    "runtime_name": runtime_name,
                    "runtime_options": {
                        "address" (optional): address,
                        "uri" (optional): uri,
                        "token" (optional): token,
                    },
                }
        """
        runtime_options = {}
        for key in _config.RUNTIME_OPTION_NAMES:
            if hasattr(self, key):
                runtime_options[key] = getattr(self, key)

        dict = {
            "config_name": str(self._name),
            "runtime_name": self._runtime_name,
            "runtime_options": runtime_options,
        }

        return dict


def migrate_config_file():
    """Update the stored configs."""
    # resolve list of files to migrate
    _config_file_path = _config._get_config_file_path().resolve()
    # Load existing file contents
    with open(_config_file_path, "r") as f:
        data = json.load(f)

    # Check version
    file_version = parse_version(data["version"])
    current_version = parse_version(CONFIG_FILE_CURRENT_VERSION)
    version_changed: bool = False
    if file_version > current_version:
        print(
            f"The file at {_config_file_path} cannot be migrated as its version is "
            "already greater than target version "
            f"(file is version {data['version']}, "
            f"current migration target is version {CONFIG_FILE_CURRENT_VERSION})."
        )
        return
    elif file_version < current_version:
        data["version"] = CONFIG_FILE_CURRENT_VERSION
        version_changed = True

    # Update configs
    changed: list = []
    for config_name in data["configs"]:
        if (
            data["configs"][config_name]["runtime_name"] == RuntimeName.RAY_LOCAL
            and "temp_dir" not in data["configs"][config_name]["runtime_options"]
        ):
            data["configs"][config_name]["runtime_options"]["temp_dir"] = None
            changed.append(config_name)

    # Write back to file if necessary)
    if len(changed) == 0 and not version_changed:
        print(f"No changes required for file '{_config_file_path}'")
        return
    else:
        with open(_config_file_path, "w") as f:
            f.write(json.dumps(data, indent=2))

    # Report changes to user
    print(
        f"Successfully migrated file {_config_file_path} to version "
        f"{CONFIG_FILE_CURRENT_VERSION}. "
        f"Updated {len(changed)} entr{'y' if len(changed)==1 else 'ies'}"
        f"{'.' if len(changed)==0 else ':'}"
    )
    for config_name in changed:
        print(f" - {config_name}")
