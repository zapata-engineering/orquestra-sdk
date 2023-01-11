################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Repositories that encapsulate data access used by dorq commands.
"""
import importlib
import os
import sys
import typing as t
import warnings
from pathlib import Path

from orquestra import sdk
from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _db, _factory, loader
from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import WorkflowRun, WorkflowRunId


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

    def list_wf_run_ids(self, config: ConfigName) -> t.Sequence[WorkflowRunId]:
        """
        Asks the runtime for all workflow runs.

        Raises:
            ConnectionError: when connection with Ray failed.
            orquestra.sdk.exceptions.UnauthorizedError: when connection with runtime
                failed because of an auth error.
        """
        # TODO: replace the contents of this method with a single call to the public
        # Python API for listing workflows.
        # Jira ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-580

        runtime_configuration = _config.read_config(config)
        project_dir = Path.cwd()

        runtime = _factory.build_runtime_from_config(
            project_dir=project_dir, config=runtime_configuration
        )

        try:
            wf_runs = runtime.get_all_workflow_runs_status()
        except (ConnectionError, exceptions.UnauthorizedError):
            raise

        return [run.id for run in wf_runs]

    def get_wf_by_run_id(
        self, wf_run_id: WorkflowRunId, config_name: t.Optional[ConfigName]
    ) -> WorkflowRun:
        wf_run = sdk.WorkflowRun.by_id(wf_run_id, config_name)

        return wf_run.get_status_model()

    def submit(
        self, wf_def: sdk.WorkflowDef, config: ConfigName, ignore_dirty_repo: bool
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

            wf_run = wf_def.run(config)

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


class ConfigRepo:
    """
    Wraps accessing ~/.orquestra/config.json
    """

    def list_config_names(self) -> t.Sequence[ConfigName]:
        config_entries = sdk.RuntimeConfig.list_configs()
        return [
            # .list_configs() should probably already return "ray" and "in_process".
            # TODO: fix that in https://zapatacomputing.atlassian.net/browse/ORQSDK-674
            _config.RAY_CONFIG_NAME_ALIAS,
            _config.IN_PROCESS_CONFIG_NAME,
            *config_entries,
        ]


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


class WorkflowDefRepo:
    def get_module_from_spec(self, module_spec: str):
        """
        Tries to figure out dotted module name, imports the module, and returns it.

        Raises:
            sdk.exceptions.WorkflowDefinitionModuleNotFound: if there's no module
                matching the resolved name
        """
        dotted_name = resolve_dotted_name(module_spec)
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
