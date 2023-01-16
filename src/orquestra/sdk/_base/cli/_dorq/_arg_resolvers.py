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
from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import TaskRunId, WorkflowRunId

from . import _repos
from ._ui import _prompts


class ConfigResolver:
    """
    Resolves value of `config` CLI arg, using only config name passed
    """

    def __init__(
        self,
        config_repo=_repos.ConfigRepo(),
        prompter=_prompts.Prompter(),
    ):
        self._config_repo = config_repo
        self._prompter = prompter

    def resolve(self, config: t.Optional[ConfigName]) -> ConfigName:
        if config is not None:
            return config

        # 1.2. Prompt the user.
        config_names = self._config_repo.list_config_names()
        selected_config = self._prompter.choice(config_names, message="Runtime config")
        return selected_config


class WFConfigResolver:
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

        if wf_run_id is not None:
            # 1.1. Attempt to get config from local cache.
            try:
                stored_name = self._wf_run_repo.get_config_name_by_run_id(wf_run_id)
                return stored_name
            except exceptions.WorkflowRunNotFoundError:
                # Ignore the exception and roll over to the "else" branch. We
                # need to ask the user for the config.
                pass

        # 1.2. Prompt the user
        return ConfigResolver(self._config_repo, self._prompter).resolve(config)


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


class TaskRunIdResolver:
    """
    Finds task run ID. Assumes ``config`` name was already resolved.
    """

    def __init__(
        self,
        wf_run_repo=_repos.WorkflowRunRepo(),
        wf_run_id_resolver: t.Optional[WFRunIDResolver] = None,
        fn_name_prompter=_prompts.Prompter(),
        task_inv_prompter=_prompts.Prompter(),
    ):
        self._wf_run_repo = wf_run_repo
        self._wf_run_id_resolver = wf_run_id_resolver or WFRunIDResolver(
            wf_run_repo=wf_run_repo
        )
        self._fn_name_prompter = fn_name_prompter
        self._task_inv_prompter = task_inv_prompter

    def resolve(
        self,
        task_run_id: t.Optional[TaskRunId],
        wf_run_id: t.Optional[WorkflowRunId],
        fn_name: t.Optional[str],
        task_inv_id: t.Optional[TaskInvocationId],
        config: ConfigName,
    ) -> TaskRunId:
        if task_run_id is not None:
            # User passed task run ID directly.
            return task_run_id

        resolved_wf_run_id = self._wf_run_id_resolver.resolve(wf_run_id, config)

        if task_inv_id is not None:
            # User passed task inv ID directly.
            resolved_inv_id = task_inv_id
        else:
            if fn_name is not None:
                # User passed fn name directly.
                resolved_fn_name = fn_name
            else:
                fn_names = self._wf_run_repo.get_task_fn_names(wf_run_id, config)
                if len(fn_names) > 1:
                    resolved_fn_name = self._fn_name_prompter.choice(
                        fn_names, message="Task function name"
                    )
                else:
                    resolved_fn_name = fn_names[0]

            inv_ids = self._wf_run_repo.get_task_inv_ids(
                config=config,
                wf_run_id=resolved_wf_run_id,
                task_fn_name=resolved_fn_name,
            )
            if len(inv_ids) > 1:
                resolved_inv_id = self._task_inv_prompter.choice(
                    inv_ids, message="Task invocation ID"
                )
            else:
                resolved_inv_id = inv_ids[0]

        resolved_task_run_id = self._wf_run_repo.get_task_run_id(
            wf_run_id=resolved_wf_run_id,
            task_inv_id=resolved_inv_id,
        )

        return resolved_task_run_id


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
