################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
When the user doesn't pass in all the required information as CLI arguments we need to
resolve the information from other sources. This module contains the CLI argument
resolution logic extracted as components reusable across similar CLI commands.
"""
import typing as t

import orquestra.sdk._base._services as _services
from orquestra.sdk import exceptions
from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from . import _repos
from ._ui import _prompts


class ConfigResolver:
    """
    Resolves value of `config` CLI arg, making use of `wf_run_id` if already passed.
    """

    def __init__(
        self,
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_repo=_repos.ConfigRepo(),
        prompter=_prompts.Prompter(),
    ):
        self._wf_run_repo = wf_run_repo
        self._config_repo = config_repo
        self._prompter = prompter

    def resolve(
        self, wf_run_id: t.Optional[WorkflowRunId], config: t.Optional[ConfigName]
    ) -> ConfigName:
        if config is not None:
            return config

        # TODO: make sure it works with `ray` and `in_process` runtimes.
        # Jira ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-674

        if wf_run_id is not None:
            # 1.1. Attempt to get config from local cache.
            try:
                stored_name = self._wf_run_repo.get_config_name_by_run_id(wf_run_id)
                return stored_name
            except exceptions.WorkflowRunNotFoundError:
                # Ignore the exception and roll over to the "else" branch. We
                # need to ask the user for the config.
                pass

        # 1.2. Prompt the user.
        config_names = self._config_repo.list_config_names()
        selected_config = self._prompter.choice(config_names, message="Runtime config")
        return selected_config


class WFRunIDResolver:
    """
    Resolves value of `wf_run_id` based on `config`.
    """

    def __init__(
        self,
        wf_run_repo=_repos.WorkflowRunRepo(),
        prompter=_prompts.Prompter(),
    ):
        self._wf_run_repo = wf_run_repo
        self._prompter = prompter

    def resolve(
        self, wf_run_id: t.Optional[WorkflowRunId], config: ConfigName
    ) -> WorkflowRunId:
        if wf_run_id is not None:
            return wf_run_id

        # Query the runtime for suitable workflow run IDs.
        # TODO: figure out sensible filters when listing workflow runs is implemented
        # in the public API.
        # Related ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-671
        ids = self._wf_run_repo.list_wf_run_ids(config)
        selected_id = self._prompter.choice(ids, message="Workflow run ID")
        return selected_id


class ServiceResolver:
    """
    Resolves the services to manage
    """

    def resolve(
        self,
        manage_ray: t.Optional[bool],
        manage_fluent: t.Optional[bool],
        manage_all: t.Optional[bool],
    ) -> t.List[_services.Service]:

        ray = _services.RayManager()
        fluent = _services.FluentManager()

        if all(s is None for s in (manage_ray, manage_fluent, manage_all)):
            # No options passed, we only start Ray by default.
            return [ray]

        managed_services: t.List[_services.Service] = []

        if manage_ray or manage_all:
            managed_services.append(ray)

        if manage_fluent or manage_all:
            managed_services.append(fluent)

        return managed_services
