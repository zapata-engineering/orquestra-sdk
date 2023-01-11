################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Redesigned, user-facing SDK API.
"""

import json
import logging
import time
import typing as t
import warnings
from collections import namedtuple
from itertools import chain
from pathlib import Path

from packaging.version import parse as parse_version

from orquestra.sdk.schema import ir
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    RuntimeConfiguration,
    RuntimeName,
)
from orquestra.sdk.schema.workflow_run import State, TaskRunId
from orquestra.sdk.schema.workflow_run import WorkflowRun as WorkflowRunModel
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from ..exceptions import (
    ConfigFileNotFoundError,
    ConfigNameNotFoundError,
    NotFoundError,
    RuntimeConfigError,
    TaskRunNotFound,
    UnauthorizedError,
    UnsavedConfigChangesError,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotStarted,
)
from . import _config
from ._in_process_runtime import InProcessRuntime
from .abc import RuntimeInterface
from .serde import deserialize_constant

COMPLETED_STATES = [State.FAILED, State.TERMINATED, State.SUCCEEDED]


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
        task_invocation_id: TaskRunId,
        workflow_run_id: WorkflowRunId,
        runtime,
        wf_def: ir.WorkflowDef,
    ):
        self._task_run_id = task_invocation_id
        self._runtime = runtime
        self._wf_def = wf_def
        self._workflow_run_id = workflow_run_id
        # get the fn_ref from wf_def
        invocation = wf_def.task_invocations[self.task_run_id]
        fn_ref = wf_def.tasks[invocation.task_id].fn_ref
        assert hasattr(fn_ref, "function_name"), (
            f"{type(fn_ref)} doesn't have" f"function_name attribute"
        )
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
    def fn_name(self) -> str:
        return self._fn_name

    @property
    def workflow_run_id(self) -> WorkflowRunId:
        return self._workflow_run_id

    @property
    def module(self) -> t.Optional[str]:
        return self._module

    def __str__(self):
        return f"TaskRun id={self.task_run_id}, mod={self.module}, fn={self.fn_name}"

    def get_status(self) -> State:
        """
        Fetch current status from the runtime

        Returns: orquestra.sdk.schema.workflow_run.State
        """
        runs = self._runtime.get_workflow_run_status(self.workflow_run_id).task_runs
        return next(
            (run for run in runs if run.invocation_id == self.task_run_id)
        ).status.state

    def get_logs(self) -> t.Dict[TaskRunId, t.List[str]]:
        return self._runtime.get_full_logs(self.task_run_id)

    def get_outputs(self) -> t.Any:
        """
        Get values calculated by this task run.

        Raises:
            TaskRunNotFound: if the task wasn't completed yet, or the ID is invalid.
        """
        try:
            return self._runtime.get_available_outputs(self.workflow_run_id)[
                self.task_run_id
            ]
        except KeyError as e:
            raise TaskRunNotFound(
                f"Task output with id `{self.task_run_id}` not found. "
                "It may have failed or not be completed yet."
            ) from e

    def _find_invocation_by_output(self, output: ir.ArgumentId) -> ir.TaskInvocation:
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

    def _get_value_from_arg_id(self, arg_id):
        """
        return value of an input argument
        If arg_id is constant - returns deserialized value
        If its artifact_feature, try to fetch the results of it from runtime
        """
        # it is either constant node, or return value from one of its parents
        if arg_id in self._wf_def.constant_nodes:
            return deserialize_constant(self._wf_def.constant_nodes[arg_id])
        else:
            inv = self._find_invocation_by_output(arg_id)
            parent = next(
                parent for parent in self.get_parents() if parent.task_run_id == inv.id
            )
            try:
                outputs = parent.get_outputs()
            except TaskRunNotFound:
                return self.INPUT_UNAVAILABLE
            # if there is only 1 output of a task, just return that output
            artefact_index = self._wf_def.artifact_nodes[arg_id].artifact_index
            if artefact_index is None:
                return outputs
            # else we have to find the proper output out of all outputs from tasks
            else:
                return outputs[artefact_index]

    def get_inputs(self) -> Inputs:
        """
        Get values of inputs (parameters) of a task run

        Returns:  Input namedTuple with Input.args and .kwargs parameters.
        """
        task_invocation = self._wf_def.task_invocations[self.task_run_id]

        args = [
            self._get_value_from_arg_id(arg_id) for arg_id in task_invocation.args_ids
        ]
        kwargs = {
            param_name: self._get_value_from_arg_id(arg_id)
            for param_name, arg_id in task_invocation.kwargs_ids.items()
        }

        return self.Inputs(args, kwargs)

    def get_parents(self) -> t.Set["TaskRun"]:
        """
        Get parent tasks - tasks whose return values are taken as input parameters
        """
        return_set = set()
        task_invocation = self._wf_def.task_invocations[self.task_run_id]
        # for every arg in the function
        for arg_id in chain(
            task_invocation.args_ids, task_invocation.kwargs_ids.values()
        ):
            # If it's constant, there is no parent task that produces it
            if arg_id in self._wf_def.constant_nodes:
                continue
            # find invocation that produces it
            parent = self._find_invocation_by_output(arg_id)
            task_run = TaskRun(
                task_invocation_id=parent.id,
                wf_def=self._wf_def,
                runtime=self._runtime,
                workflow_run_id=self.workflow_run_id,
            )
            return_set.add(task_run)

        return return_set


class WorkflowRun:
    """
    Represents a single "execution" of a workflow. Used to get the workflow results.
    """

    @staticmethod
    def _get_stored_run(_project_dir: Path, run_id: WorkflowRunId):
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
        config_save_file: t.Optional[t.Union[Path, str]] = None,
    ) -> "WorkflowRun":
        """Get the WorkflowRun corresponding to a previous workflow run.

        Args:
            run_id: The id of the workflow run to be loaded.
            config: Determines where to look for the workflow run record. If ommited,
                we will retrieve the config name from a local cache of workflow runs
                submitted from this machine.
            project_dir: The location of the project directory. This directory must
                contain the workflows database to which this run was saved. If omitted,
                the current working directory is assumed to be the project directory.
            config_save_file: The location to which the associated configuration was
                saved. If omitted, the default config file path is used.

        Raises:
            NotFoundError: when the run_id doesn't match a stored run ID.
            ConfigNameNotFoundError: when the named config is not found in the file.

        Returns:
            WorkflowRun
        """
        _project_dir = Path(project_dir or Path.cwd())

        # Resolve config
        resolved_config: RuntimeConfig
        if isinstance(config, RuntimeConfig):
            # EZ. Passed-in explicitly.
            resolved_config = config
        elif isinstance(config, str):
            # Shorthand: just the config name.
            resolved_config = RuntimeConfig.load(
                config, config_save_file=config_save_file
            )
        elif config is None:
            # Shorthand: use the cached value.
            # We need to read the config name from the local DB and load the config
            # entry.
            stored_run = cls._get_stored_run(_project_dir, run_id)
            resolved_config = RuntimeConfig.load(
                stored_run.config_name, config_save_file=config_save_file
            )
        else:
            raise TypeError(f"'config' is of unsupported type {type(config)}.")

        # Retrieve workflow def from the runtime:
        # - Ray stores wf def for us under a metadata entry.
        # - CE will have endpoints for getting [wf def] by [wf run ID]. See:
        #   https://zapatacomputing.atlassian.net/browse/ORQP-1317
        # - QE probably won't have endpoints for this, but the single-user limitation
        #   will be an implementation detail of `QERuntime`.
        runtime = resolved_config._get_runtime(_project_dir)
        wf_def = runtime.get_workflow_run_status(run_id).workflow_def

        workflow_run = WorkflowRun(
            run_id,
            wf_def,
            runtime,
            resolved_config,
        )

        return workflow_run

    def __init__(
        self,
        run_id: t.Optional[str],
        wf_def: ir.WorkflowDef,
        runtime: t.Union[RuntimeInterface, InProcessRuntime],
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
            frequency (float, optional): The frequence in Hz at which the status should
                be checked. Defaults to 1.

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
        return self._runtime.get_workflow_run_outputs_non_blocking(run_id)

    def get_artifacts(
        self,
        tasks: t.Union[str, t.List[str], None] = None,
        *,
        only_available: bool = False,
    ) -> t.Dict[str, t.Any]:
        """
        Unstable: this API will change.

        Returns the task run artifacts from a workflow.

        Args:
            tasks: A task run ID or a list of task run IDs to return. An empty list
                   implies returning all available task run artifacts.
            only_available: If true, only available task artifacts are returned.
                            This does nothing if `tasks` is omitted or is empty.

        Raises:
            WorkflowRunNotStarted: when the workflow has not started
            TaskRunNotFound: when only_available is False and a requested task run
                            cannot be found, either because it does not exist or has not
                            been completed yet.
        Returns:
            A dictionary of the task run artifacts with the shape::
                task_run_id: values returned from the task
        """
        if tasks is None:
            tasks = []
        try:
            run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get the values of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        workflow_artifacts = self._runtime.get_available_outputs(run_id)
        task_list = [tasks] if isinstance(tasks, str) else tasks

        # If task_list is empty, return everything we got from the runtime
        if len(task_list) == 0:
            return workflow_artifacts

        if only_available:
            # if only_available is True, we return only the task run artifacts the user
            # requested, but ignore any task runs the user requested that the runtime
            # didn't return.
            workflow_artifacts = {
                task_run_id: result
                for task_run_id, result in workflow_artifacts.items()
                if task_run_id in task_list
            }
        else:
            # if only_available is False, we try to get all requested tasks and raise
            # an exception if any of them are missing
            try:
                workflow_artifacts = {
                    task_run_id: workflow_artifacts[task_run_id]
                    for task_run_id in task_list
                }
            except KeyError as e:
                missing_id = e.args[0]
                raise TaskRunNotFound(
                    f"Task run with id `{missing_id}` not found. "
                    "It may not be completed or does not exist in this WorkflowRun."
                ) from e

        return workflow_artifacts

    def get_logs(
        self,
        tasks: t.Union[TaskRunId, t.List[TaskRunId]],
        *,
        only_available: bool = False,
    ) -> t.Dict[TaskRunId, t.List[str]]:
        """
        Unstable: this API will change.

        Returns the task run logs from a workflow.
        Args:
            tasks: A task run ID or a list of task run IDs to return.
                   An empty list implies no task logs.
            only_available: If true, logs for tasks that haven't been started yet won't
                            be returned. If false, and `tasks` contain IDs of task runs
                            that haven't been started yet, this will raise an error.
        Raises:
            WorkflowRunNotStarted: when the workflow has not started
            TaskRunNotFound: when only_available is False and a requested task run
                            cannot be found, either because it does not exist or has not
                            been completed yet.
        Returns:
            A dictionary of the task run logs with the shape::
                task_run_id: List[log lines]
        """
        try:
            _ = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get the logs of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        task_list = [tasks] if isinstance(tasks, str) else tasks
        task_logs: t.Dict[TaskRunId, t.List[str]] = {}

        for task_run_id in task_list:
            try:
                # Get a single task run logs
                # Unfortunately, we return TaskInvocationId: List[str] from
                # get_full_logs. so we do this hack to get the first value from the dict
                # which (in this case) the task logs for the task_run_id.
                task_logs[task_run_id] = next(
                    iter(self._runtime.get_full_logs(task_run_id).values())
                )
            except NotFoundError as e:
                if only_available:
                    continue
                else:
                    raise TaskRunNotFound(
                        f"Task run with id `{task_run_id}` not found. "
                        "It may not be completed or does not exist in this WorkflowRun."
                    ) from e

        return task_logs

    # TODO: ORQSDK-617 add filtering ability for the users
    def get_tasks(self) -> t.Set[TaskRun]:
        try:
            run_id = self.run_id
        except WorkflowRunNotStarted as e:
            message = (
                "Cannot get tasks of a workflow run that hasn't started yet. "
                "You will need to call the `.start()` method prior to calling this "
                "method."
            )
            raise WorkflowRunNotStarted(message) from e

        return {
            TaskRun(invocation_id, run_id, runtime=self._runtime, wf_def=self._wf_def)
            for invocation_id, task_invocation in self._wf_def.task_invocations.items()
        }


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
        config_save_file: t.Optional[t.Union[str, Path]] = None,
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
        self._config_save_file = config_save_file

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
            saved_config = self.load(str(self.name), self._config_save_file)
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
    @staticmethod
    def _import_orquestra_runtime():
        import orquestra.sdk._base

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

        self._import_orquestra_runtime()

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
        config_save_file: t.Optional[t.Union[str, Path]] = None,
    ) -> list:
        """List previously saved configurations.

        Args:
            config_save_file: The path to the config file from which to read. If
                omitted, the default config file will be used.

        Returns:
            list: list of configurations within the save file.
        """
        return _config.read_config_names(config_save_file) + list(
            _config.SPECIAL_CONFIG_NAME_DICT.keys()
        )

    @classmethod
    def load(
        cls,
        config_name: str,
        config_save_file: t.Optional[t.Union[str, Path]] = None,
    ):
        """Load an existing configuration from a file.

        Args:
            config_name: The name of the configuration to be loaded.
            config_save_file (optional): The path to the file in which configurations
                are stored. If omitted, the default file location is used.

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
        _config_save_file = _config._get_config_file_path(config_save_file)
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
            migrate_config_file(_config_save_file)
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
            config_data: RuntimeConfiguration = _config.read_config(
                config_name, _config_save_file
            )
        except ConfigNameNotFoundError as e:
            raise ConfigNameNotFoundError(
                f"No config with name '{config_name}' "
                f"found in file {_config_save_file}. "
                f"Available names are: {cls.list_configs(_config_save_file)}"
            ) from e

        config = cls._config_from_runtimeconfiguration(config_data)
        config._config_save_file = _config_save_file

        return config

    @classmethod
    def load_default(
        cls,
        config_save_file: t.Optional[t.Union[str, Path]] = None,
    ) -> "RuntimeConfig":
        """Load the default configuration from a file.

        Args:
            config_save_file (optional): The path to the file in which configurations
                are stored. If omitted, the default file location is used.

        Returns:
            RuntimeConfig: The configuration as loaded from the file.
        """

        config_data: RuntimeConfiguration = _config.read_config(None, config_save_file)
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
            return RuntimeConfig.in_process(config.config_name)
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
        config_save_file: t.Optional[t.Union[str, Path]] = None,
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
            _config.read_config(self._name, config_file_path=self._config_save_file)
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


def migrate_config_file(config_file_paths: t.Optional[t.Union[str, Path, list]] = None):
    """Update the stored configs.

    Args:
        config_file_paths: Path(s) to the config file(s) to be updated. Defaults to
            ["~/.orquestra/config.json"]
    """
    # resolve list of files to migrate
    _config_file_paths: list
    if config_file_paths is None:
        _config_file_paths = [_config._get_config_file_path()]
    elif isinstance(config_file_paths, list):
        _config_file_paths = [
            _config._get_config_file_path(path) for path in config_file_paths
        ]
    else:  # str or Path
        _config_file_paths = [_config._get_config_file_path(config_file_paths)]

    for file_path in [Path(path).resolve() for path in _config_file_paths]:
        # Load existing file contents
        with open(file_path, "r") as f:
            data = json.load(f)

        # Check version
        file_version = parse_version(data["version"])
        current_version = parse_version(CONFIG_FILE_CURRENT_VERSION)
        version_changed: bool = False
        if file_version > current_version:
            print(
                f"The file at {file_path} cannot be migrated as its version is already "
                "greater than target version "
                f"(file is version {data['version']}, "
                f"current migration target is version {CONFIG_FILE_CURRENT_VERSION})."
            )
            continue
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
            print(f"No changes required for file '{file_path}'")
            continue
        else:
            with open(file_path, "w") as f:
                f.write(json.dumps(data, indent=2))

        # Report changes to user
        print(
            f"Successfully migrated file {file_path} to version "
            f"{CONFIG_FILE_CURRENT_VERSION}. "
            f"Updated {len(changed)} entr{'y' if len(changed)==1 else 'ies'}"
            f"{'.' if len(changed)==0 else ':'}"
        )
        for config_name in changed:
            print(f" - {config_name}")
