################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Repositories that encapsulate data access used by dorq commands.

The "data" layer. Shouldn't directly depend on the "view" layer.
"""
import datetime
import importlib
import os
import sys
import typing
import typing as t
import warnings
from contextlib import contextmanager

import requests

from orquestra import sdk
from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _db, loader
from orquestra.sdk._base._driver._client import DriverClient
from orquestra.sdk._base._jwt import check_jwt_without_signature_verification
from orquestra.sdk._base._qe import _client
from orquestra.sdk._base._spaces._structs import ProjectRef
from orquestra.sdk._base.abc import ArtifactValue
from orquestra.sdk.schema import _compat
from orquestra.sdk.schema.configs import ConfigName, RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.ir import TaskInvocationId, WorkflowDef
from orquestra.sdk.schema.workflow_run import (
    ProjectId,
    State,
    TaskRun,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
    WorkspaceId,
)

from ._ui import _models as ui_models


def _find_first(f: t.Callable[[t.Any], bool], it: t.Iterable):
    return next(filter(f, it))


class WorkflowRunRepo:
    def get_config_name_by_run_id(self, wf_run_id: WorkflowRunId) -> ConfigName:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: when couldn't find
                a matching record.
        """
        with _db.WorkflowDB.open_db() as db:
            stored_run = db.get_workflow_run(workflow_run_id=wf_run_id)
            return stored_run.config_name

    def list_wf_run_ids(
        self, config: ConfigName, project: ProjectRef
    ) -> t.Sequence[WorkflowRunId]:
        return [
            run.id
            for run in self.list_wf_runs(
                config, project.workspace_id, project.project_id
            )
        ]

    def list_wf_runs(
        self,
        config: ConfigName,
        workspace: t.Optional[WorkspaceId] = None,
        project: t.Optional[ProjectId] = None,
        limit: t.Optional[int] = None,
        max_age: t.Optional[str] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
    ) -> t.List[WorkflowRun]:
        """
        Asks the runtime for all workflow runs that match the filters.

        Raises:
            ConnectionError: when connection with Ray failed.
            orquestra.sdk.exceptions.UnauthorizedError: when connection with runtime
                failed because of an auth error.
        """
        try:
            wf_runs = sdk.list_workflow_runs(
                config,
                limit=limit,
                max_age=max_age,
                state=state,
                workspace=workspace,
                project=project,
            )
        except (ConnectionError, exceptions.UnauthorizedError):
            raise

        return [run.get_status_model() for run in wf_runs]

    def get_wf_by_run_id(
        self, wf_run_id: WorkflowRunId, config_name: t.Optional[ConfigName]
    ) -> WorkflowRun:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: when the wf_run_id
                doesn't match any available run ID.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
        """
        try:
            wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)
        except (
            exceptions.WorkflowRunNotFoundError,
            exceptions.ConfigNameNotFoundError,
        ):
            raise

        return wf_run.get_status_model()

    def get_task_run_id(
        self,
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
        config_name: ConfigName,
    ) -> TaskRunId:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: when the wf_run_id
                doesn't match any available run ID.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
            orquestra.sdk.exceptions.TaskInvocationNotFoundError: when the task_inv_id
                doesn't match any task invocation in this workflow.
        """
        try:
            wf_run_model = self.get_wf_by_run_id(
                wf_run_id=wf_run_id, config_name=config_name
            )
        except (
            exceptions.WorkflowRunNotFoundError,
            exceptions.ConfigNameNotFoundError,
        ):
            raise

        try:
            task_run = _find_first(
                lambda task_run: task_run.invocation_id == task_inv_id,
                wf_run_model.task_runs,
            )
        except StopIteration as e:
            raise exceptions.TaskInvocationNotFoundError(
                invocation_id=task_inv_id
            ) from e

        return task_run.id

    def submit(
        self,
        wf_def: sdk.WorkflowDef,
        config: ConfigName,
        ignore_dirty_repo: bool,
        workspace_id: t.Optional[WorkspaceId],
        project_id: t.Optional[ProjectId],
    ) -> WorkflowRunId:
        """
        Args:
            ignore_dirty_repo: if False, turns the DirtyGitRepo warning into a raised
                exception.

        Raises:
            orquestra.sdk.exceptions.DirtyGitRepo: if ``ignore_dirty_repo`` is False and
                a task def used by this workflow def has a "GitImport" and the git repo
                that contains it has uncommitted changes.
        """

        with warnings.catch_warnings():
            if not ignore_dirty_repo:
                warnings.filterwarnings("error", category=exceptions.DirtyGitRepo)

            wf_run = wf_def.run(
                config, workspace_id=workspace_id, project_id=project_id
            )

        return wf_run.run_id

    def stop(self, wf_run_id: WorkflowRunId, config_name: ConfigName):
        """
        Raises:
            orquestra.sdk.exceptions.UnauthorizedError: when communication with runtime
                failed because of an auth error
            orquestra.sdk.exceptions.WorkflowRunCanNotBeTerminated if the termination
                attempt failed
        """
        wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)

        try:
            return wf_run.stop()
        except (exceptions.UnauthorizedError, exceptions.WorkflowRunCanNotBeTerminated):
            # Other exception types aren't expected to be raised here.
            raise

    def get_wf_outputs(self, wf_run_id: WorkflowRunId, config_name: ConfigName):
        """
        Asks the runtime for workflow output values.

        If the workflow is still executing this will block until the workflow
        completion.

        Raises:
            orquestra.sdk.exceptions.NotFoundError: when the run_id doesn't match a
                stored run ID.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
            orquestra.sdk.exceptions.WorkflowRunNotSucceeded: when the workflow is no
                longer executing, but wasn't succeeded.
        """
        try:
            wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)
        except (exceptions.NotFoundError, exceptions.ConfigNameNotFoundError):
            raise

        try:
            outputs = wf_run.get_results(wait=False)
        except (exceptions.WorkflowRunNotFinished, exceptions.WorkflowRunNotSucceeded):
            raise

        # In the context of the CLI, we want the results to be a n-tuple where n is the
        # number of results. This allows us to use the len of outputs to determine the
        # n_results, and iterate over the outputs in a consistent way.
        if not isinstance(outputs, tuple):
            return (outputs,)

        return outputs

    def get_task_outputs(
        self,
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
        config_name: ConfigName,
    ) -> t.Tuple[ArtifactValue, ...]:
        """
        Asks the runtime for task output values. The output is always a n-tuple where n
        is the number of outputs in the task def metadata.

        Raises:
            orquestra.sdk.exceptions.NotFoundError: when the wf_run_id doesn't match a
                known run ID.
            orquestra.sdk.exceptions.TaskInvocationNotFoundError: when task_inv_id
                doesn't match the workflow definition.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
        """
        try:
            wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)
        except (exceptions.NotFoundError, exceptions.ConfigNameNotFoundError):
            raise

        try:
            task_run: sdk.TaskRun = _find_first(
                lambda task: task.task_invocation_id == task_inv_id, wf_run.get_tasks()
            )
        except StopIteration as e:
            raise exceptions.TaskInvocationNotFoundError(
                invocation_id=task_inv_id
            ) from e

        # TaskRun.get_outputs() returns whatever the task function returned, regardless
        # of the number of ``@task(n_outputs=...)``. For presentation we need to somehow
        # decide if we need to iterate over ``task_outputs`` or not. We base this logic
        # on the IR.
        task_outputs = task_run.get_outputs()

        wf_def = wf_run.get_status_model().workflow_def
        invocation = wf_def.task_invocations[task_inv_id]
        task_def = wf_def.tasks[invocation.task_id]

        if _compat.result_is_packed(task_def=task_def):
            # We expect ``task_outputs`` to be an iterable already.
            outputs_tuple = tuple(task_outputs)
        else:
            # ``task_outputs`` is likely to be a single object. We need to wrap it.
            outputs_tuple = (task_outputs,)

        return outputs_tuple

    def _get_wf_def_model(
        self, wf_run_id: WorkflowRunId, config_name: ConfigName
    ) -> WorkflowDef:
        """
        Raises:
            orquestra.sdk.exceptions.NotFoundError: when the wf_run_id doesn't match a
                stored run ID.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
        """
        try:
            wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)
        except (exceptions.NotFoundError, exceptions.ConfigNameNotFoundError):
            raise

        status_model = wf_run.get_status_model()
        wf_def = status_model.workflow_def

        assert wf_def is not None, (
            "We don't have workflow definition associated with the workflow "
            f"run {wf_run_id}. It shouldn't have happened."
        )
        return wf_def

    def get_task_fn_names(
        self, wf_run_id: WorkflowRunId, config_name: ConfigName
    ) -> t.Sequence[str]:
        """
        Extracts task function names used in this workflow run.

        If two different task defs have the same function name this returns a single
        name entry. This can happen when similar tasks are defined in two different
        modules.

        Raises:
            orquestra.sdk.exceptions.NotFoundError: when the wf_run_id doesn't match a
                stored run ID.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
        """

        try:
            wf_def = self._get_wf_def_model(wf_run_id, config_name)
        except (exceptions.NotFoundError, exceptions.ConfigNameNotFoundError):
            raise

        # What we're really interested in are task_runs, but it's easier to test when
        # we iterate over wf_defs's task_defs. Assumption: every task_def is being used
        # in the workflow and corresponds to a task_run.
        names_set = {
            task_def.fn_ref.function_name for task_def in wf_def.tasks.values()
        }

        return sorted(names_set)

    def get_task_inv_ids(
        self,
        wf_run_id: WorkflowRunId,
        config_name: ConfigName,
        task_fn_name: str,
    ) -> t.Sequence[TaskInvocationId]:
        """
        Selects task invocation IDs that refer to functions named ``task_fn_name``.

        Raises:
            orquestra.sdk.exceptions.NotFoundError: when the wf_run_id doesn't match a
                stored run ID.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the named config is
                not found in the file.
        """
        try:
            wf_def = self._get_wf_def_model(wf_run_id, config_name)
        except (exceptions.NotFoundError, exceptions.ConfigNameNotFoundError):
            raise

        matching_inv_ids = [
            inv.id
            for inv in wf_def.task_invocations.values()
            if wf_def.tasks[inv.task_id].fn_ref.function_name == task_fn_name
        ]

        return matching_inv_ids

    def get_wf_logs(
        self, wf_run_id: WorkflowRunId, config_name: ConfigName
    ) -> t.Mapping[TaskInvocationId, t.Sequence[str]]:
        """
        Raises:
            ConnectionError: when connection with Ray failed.
            orquestra.sdk.exceptions.UnauthorizedError: when connection with runtime
                failed because of an auth error.
        """
        try:
            wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)
        except (exceptions.NotFoundError, exceptions.ConfigNameNotFoundError):
            raise

        try:
            # While this method can also raise WorkflowRunNotStarted error we don't ever
            # expect it to happen, because we're getting workflow run by ID. Workflows
            # get their IDs at the start time.
            return wf_run.get_logs()
        except (ConnectionError, exceptions.UnauthorizedError):
            raise

    def get_task_logs(
        self,
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
        config_name: ConfigName,
    ) -> t.Mapping[TaskInvocationId, t.Sequence[str]]:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError
            orquestra.sdk.exceptions.ConfigmeNotFoundError
            orquestra.sdk.exceptions.ConfigNameNotFoundError
        """
        try:
            wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)
        except (
            exceptions.NotFoundError,
            exceptions.ConfigFileNotFoundError,
            exceptions.ConfigNameNotFoundError,
        ):
            raise

        task_runs = wf_run.get_tasks()
        try:
            task_run: sdk.TaskRun = _find_first(
                lambda task: task.task_invocation_id == task_inv_id, task_runs
            )
        except StopIteration as e:
            raise exceptions.TaskInvocationNotFoundError(task_inv_id) from e

        log_lines = task_run.get_logs()

        # Single k-v dict might seem weird but Using the same data shape for single
        # task logs and full workflow logs allows easier code sharing.
        logs_dict = {task_inv_id: log_lines}

        return logs_dict


def _ui_model_from_task_run(task_run: TaskRun, wf_def: WorkflowDef):
    invocation = wf_def.task_invocations[task_run.invocation_id]
    task_def = wf_def.tasks[invocation.task_id]
    fn_name = task_def.fn_ref.function_name

    return ui_models.WFRunSummary.TaskRow(
        task_fn_name=fn_name,
        inv_id=task_run.invocation_id,
        status=task_run.status,
        message=task_run.message,
    )


def _tasks_number_summary(wf_run: WorkflowRun) -> str:
    total = len(wf_run.task_runs)
    finished = sum(
        1 for task_run in wf_run.task_runs if task_run.status.state == State.SUCCEEDED
    )
    return f"{finished}/{total}"


def _ui_model_from_wf(wf_run: WorkflowRun):
    return ui_models.WFList.WFRow(
        workflow_run_id=wf_run.id,
        status=wf_run.status.state.value,
        tasks_succeeded=_tasks_number_summary(wf_run),
        start_time=wf_run.status.start_time,
    )


class SummaryRepo:
    """
    Performs data wrangling to derive UI models that we can show to the user.
    """

    def wf_run_summary(self, wf_run: WorkflowRun) -> ui_models.WFRunSummary:
        n_succeeded = sum(
            1
            for task_run in wf_run.task_runs
            if task_run.status.state == State.SUCCEEDED
        )
        n_total = len(wf_run.workflow_def.task_invocations)

        return ui_models.WFRunSummary(
            wf_def_name=wf_run.workflow_def.name,
            wf_run_id=wf_run.id,
            wf_run_status=wf_run.status,
            task_rows=[
                _ui_model_from_task_run(task_run, wf_def=wf_run.workflow_def)
                for task_run in wf_run.task_runs
            ],
            n_tasks_succeeded=n_succeeded,
            n_task_invocations_total=n_total,
        )

    def wf_list_summary(self, wf_runs: t.List[WorkflowRun]) -> ui_models.WFList:
        wf_runs.sort(
            key=lambda wf_run: wf_run.status.start_time
            if wf_run.status.start_time
            else datetime.datetime.fromtimestamp(0).replace(
                tzinfo=datetime.timezone.utc
            )
        )

        return ui_models.WFList(wf_rows=[_ui_model_from_wf(wf) for wf in wf_runs])


class ConfigRepo:
    """
    Wraps accessing ~/.orquestra/config.json
    """

    def list_config_names(self) -> t.Sequence[ConfigName]:
        return [
            config
            for config in sdk.RuntimeConfig.list_configs()
            if config not in _config.CLI_IGNORED_CONFIGS
        ]

    def store_token_in_config(self, uri, token, ce):
        """
        Saves the token in the config file

        Raises:
            ExpiredTokenError: if the token is expired
            InvalidTokenError: if the token is not a valid format
        """
        check_jwt_without_signature_verification(token)

        runtime_name = RuntimeName.CE_REMOTE if ce else RuntimeName.QE_REMOTE
        config_name = _config.generate_config_name(runtime_name, uri)

        config = sdk.RuntimeConfig(
            runtime_name,
            name=config_name,
            bypass_factory_methods=True,
        )
        setattr(config, "uri", uri)
        setattr(config, "token", token)
        _config.save_or_update(config_name, runtime_name, config._get_runtime_options())

        return config_name

    def read_config(self, config: ConfigName) -> RuntimeConfiguration:
        """
        Read a stored config.
        """
        return _config.read_config(config)


class SpacesRepo:
    """
    Wraps access to workspaces and projects
    """

    def list_workspaces(
        self,
        config: ConfigName,
    ):
        return sdk.list_workspaces(config)

    def list_projects(self, config: ConfigName, workspace_id):
        return sdk.list_projects(config, workspace_id)


class RuntimeRepo:
    """
    Wraps access to QE/CE clients
    """

    def get_login_url(self, uri: str, ce: bool, redirect_port: int):
        client: typing.Union[DriverClient, _client.QEClient]
        if ce:
            client = DriverClient(base_uri=uri, session=requests.Session())
        else:
            client = _client.QEClient(session=requests.Session(), base_uri=uri)
            # Ask QE for the login url to log in to the platform
        try:
            target_url = client.get_login_url(redirect_port)
        except requests.RequestException as e:
            raise exceptions.LoginURLUnavailableError(uri) from e
        return target_url


def resolve_dotted_name(module_spec: str) -> str:
    """
    Heuristic for detecting various ways to specify project modules.
    """
    if os.path.sep in module_spec or module_spec.endswith(".py"):
        # This looks like a file path!

        # "foo/bar.py" -> "foo/bar"
        file_path = os.path.splitext(module_spec)[0]

        # "foo/bar" -> ["foo", "bar"]
        path_components = file_path.split(os.path.sep)

        if path_components[0] == "src":
            # This like a "src-layout"! We need to drop the prefix.
            # More info:
            # https://setuptools.pypa.io/en/latest/userguide/package_discovery.html#src-layout
            path_components.pop(0)

        # ["foo", "bar"] -> "foo.bar"
        return ".".join(path_components)

    else:
        # This looks like dotted module name already.
        return module_spec


@contextmanager
def _extend_sys_path(sys_path_additions: t.Sequence[str]):
    original_sys_path = list(sys.path)
    sys.path[:] = [*sys_path_additions, *original_sys_path]

    try:
        yield
    finally:
        sys.path[:] = original_sys_path


class WorkflowDefRepo:
    def get_module_from_spec(self, module_spec: str):
        """
        Tries to figure out dotted module name, imports the module, and returns it.

        Raises:
            sdk.exceptions.WorkflowDefinitionModuleNotFound: if there's no module
                matching the resolved name
        """
        dotted_name = resolve_dotted_name(module_spec)

        # Enable importing packages/modules from under current working dir even if
        # they're not part of a setuptools distribution. This workaround is needed
        # because we expose the 'orq' CLI as a "console script", and it in this set up
        # PWD isn't added to 'sys.path' automatically.
        with _extend_sys_path([os.getcwd()]):
            try:
                return importlib.import_module(name=dotted_name)
            except ModuleNotFoundError:
                raise exceptions.WorkflowDefinitionModuleNotFound(
                    module_name=dotted_name, sys_path=sys.path
                )

    def get_worklow_names(self, module) -> t.Sequence[str]:
        """
        Raises:
            orquestra.sdk.exceptions.NoWorkflowDefinitionsFound when there was no
                matching wf defs found in the module.
        """
        workflows: t.Sequence[sdk.WorkflowTemplate] = loader.get_attributes_of_type(
            module, sdk.WorkflowTemplate
        )

        if len(workflows) == 0:
            raise exceptions.NoWorkflowDefinitionsFound(module_name=module.__name__)

        # Known limitation: this doesn't respect "custom_name" set in the
        # "sdk.workflow()".
        # Related ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-675

        return [wf._fn.__name__ for wf in workflows]

    def get_workflow_def(self, module, name: str) -> sdk.WorkflowDef:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowSyntaxError when the workflow of choice is
                parametrized.
        """
        wf_template = getattr(module, name)
        # Known limitation: this doesn't work with parametrized workflows.
        try:
            return wf_template()
        except exceptions.WorkflowSyntaxError:
            # Explicit re-raise
            raise
