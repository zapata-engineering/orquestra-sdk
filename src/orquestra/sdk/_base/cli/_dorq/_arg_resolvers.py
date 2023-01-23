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
from orquestra.sdk.schema.workflow_run import (
    State,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
)

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

    def resolve_multiple(
        self, configs: t.Optional[t.Sequence[str]]
    ) -> t.Sequence[ConfigName]:
        if configs is not None and len(configs) > 0:
            return configs

        # Prompt the user.
        config_names = self._config_repo.list_config_names()
        while (
            len(
                selected_configs := self._prompter.checkbox(
                    config_names,
                    message="Runtime config(s)",
                )
            )
            < 1
        ):
            print("Please select at least one configuration.")

        return selected_configs


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


class WFRunResolver:
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

    def resolve_id(
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

    def resolve_run(
        self, wf_run_id: t.Optional[WorkflowRunId], config: ConfigName
    ) -> WorkflowRun:
        if wf_run_id is not None:
            return self._wf_run_repo.get_wf_by_run_id(wf_run_id, config)

        # Query the runtime for suitable workflow run IDs.
        # TODO: figure out sensible filters when listing workflow runs is implemented
        # in the public API.
        # Related ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-671
        runs = self._wf_run_repo.list_wf_runs(config)
        selected_run = self._prompter.choice(
            [(run.id, run) for run in runs], message="Workflow run ID"
        )
        return selected_run


class TaskInvIDResolver:
    """
    Finds task invocation ID. Assumes workflow run ID and config were already
    resolved.
    """

    def __init__(
        self,
        wf_run_repo=_repos.WorkflowRunRepo(),
        fn_name_prompter=_prompts.Prompter(),
        task_inv_prompter=_prompts.Prompter(),
    ):
        self._wf_run_repo = wf_run_repo
        self._fn_name_prompter = fn_name_prompter
        self._task_inv_prompter = task_inv_prompter

    def resolve(
        self,
        task_inv_id: t.Optional[TaskInvocationId],
        fn_name: t.Optional[str],
        wf_run_id: WorkflowRunId,
        config: ConfigName,
    ) -> TaskInvocationId:
        if task_inv_id is not None:
            # User passed task inv ID directly.
            return task_inv_id

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
            wf_run_id=wf_run_id,
            config_name=config,
            task_fn_name=resolved_fn_name,
        )
        if len(inv_ids) > 1:
            resolved_inv_id = self._task_inv_prompter.choice(
                inv_ids, message="Task invocation ID"
            )
            return resolved_inv_id
        else:
            return inv_ids[0]


class TaskRunIDResolver:
    """
    Finds task run ID. Assumes ``config`` name was already resolved.
    """

    def __init__(
        self,
        wf_run_repo=_repos.WorkflowRunRepo(),
        wf_run_resolver: t.Optional[WFRunResolver] = None,
        task_inv_id_resolver: t.Optional[TaskInvIDResolver] = None,
    ):
        self._wf_run_repo = wf_run_repo
        self._wf_run_resolver = wf_run_resolver or WFRunResolver(
            wf_run_repo=wf_run_repo
        )
        self._task_inv_id_resolver = task_inv_id_resolver or TaskInvIDResolver(
            wf_run_repo=wf_run_repo
        )

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

        resolved_wf_run_id = self._wf_run_resolver.resolve_id(wf_run_id, config)

        resolved_inv_id = self._task_inv_id_resolver.resolve(
            task_inv_id=task_inv_id,
            fn_name=fn_name,
            wf_run_id=resolved_wf_run_id,
            config=config,
        )

        resolved_task_run_id = self._wf_run_repo.get_task_run_id(
            wf_run_id=resolved_wf_run_id,
            task_inv_id=resolved_inv_id,
            config_name=config,
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


class WFRunFilterResolver:
    """
    Resolves the values of filters to be applied to lists of workflow runs.
    """

    def __init__(self, prompter=_prompts.Prompter()):
        self._prompter = prompter

    def resolve_limit(
        self, limit: t.Optional[int] = None, interactive: t.Optional[bool] = False
    ) -> t.Union[int, None]:
        if limit is not None:
            return limit

        if interactive:
            return self._prompter.ask_for_int(
                message=(
                    "Enter maximum number of results to display. "
                    "If 'None', all results will be displayed."
                ),
                default="None",
                allow_none=True,
            )

        return None

    def resolve_max_age(
        self, max_age: t.Optional[str] = None, interactive: t.Optional[bool] = False
    ) -> t.Union[str, None]:
        if max_age is not None:
            return max_age

        if interactive:
            return self._prompter.ask_for_str(
                message=(
                    "Maximum age of run to display. "
                    "If 'None', all results will be displayed."
                ),
                default="None",
                allow_none=True,
            )

        return None

    def resolve_state(
        self,
        states: t.Optional[t.List[str]] = [],
        interactive: t.Optional[bool] = False,
    ) -> t.Union[t.List[State], None]:
        """
        Resolve a string representing one or more workflow run states into a list of
        State enums. Where a string is not provided, or is not a valid State,
        """
        _selected_states: t.List[str] = []
        _invalid_states: t.List[str] = []

        if states is not None and len(states) > 0:
            # The user has passed in one or more state arguments, iterate through them
            # and check that they're valid states.
            for state in states:
                try:
                    _ = State(state)
                except ValueError:
                    _invalid_states.append(state)
                else:
                    _selected_states.append(state)

            # If there are no invalid states, return the converted states. Otherwise,
            # tell the user which state arguments weren't valid.
            if len(_invalid_states) == 0:
                return [State(state) for state in _selected_states]
            else:
                print(
                    "The following arguments are not valid states:"
                    f"\n{_invalid_states}"
                    "\nPlease select from valid state(s)."
                )

        if interactive or len(_invalid_states) > 0:
            # If the user provided some states, start with those ones selected. This
            # way if they made a typo they only have to reselect the state they got
            # wrong, rather than redoing the entire selection. Otherwise, start with
            # everything selected to save time for people who don't want to filter by
            # state, but used the interactive flag for other filters
            _all_valid_states = [e.value for e in State]

            if len(_selected_states) == 0:
                _selected_states = _all_valid_states

            resolved_states = self._prompter.checkbox(
                choices=_all_valid_states,
                default=_selected_states,
                message="Workflow Run State(s)",
            )
            return [State(state) for state in resolved_states]

        return None
