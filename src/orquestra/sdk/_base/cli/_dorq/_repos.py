################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Repositories that encapsulate data access used by dorq commands.
"""
import importlib
import os
import sys
import typing
import typing as t
import warnings
from pathlib import Path

import requests

from orquestra import sdk
from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _db, _factory, loader
from orquestra.sdk._base._driver._client import DriverClient
from orquestra.sdk._base._qe import _client
from orquestra.sdk.schema.configs import ConfigName, RuntimeName
from orquestra.sdk.schema.workflow_run import State, WorkflowRun, WorkflowRunId


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
        return [run.id for run in self.list_wf_runs(config)]

    def list_wf_runs(
        self,
        config: ConfigName,
        limit: t.Optional[int] = None,
        max_age: t.Optional[int] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
    ) -> t.Sequence[WorkflowRun]:
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

        return wf_runs

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
        return sdk.RuntimeConfig.list_configs()

    def store_token_in_config(self, uri, token, ce):
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


class RuntimeRepo:
    """
    Wraps access to QE/CE clients
    """

    def get_login_url(self, uri: str, ce: bool):
        client: typing.Union[DriverClient, _client.QEClient]
        if ce:
            client = DriverClient(base_uri=uri, session=requests.Session())
        else:
            client = _client.QEClient(session=requests.Session(), base_uri=uri)
            # Ask QE for the login url to log in to the platform
        try:
            target_url = client.get_login_url()
        except (requests.ConnectionError, requests.exceptions.MissingSchema):
            raise exceptions.UnauthorizedError(f'Cannot connect to server "{uri}"')
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
