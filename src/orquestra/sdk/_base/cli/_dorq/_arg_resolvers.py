################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
When the user doesn't pass in all the required information as CLI arguments we need to
resolve the information from other sources. This module contains the CLI argument
resolution logic extracted as components reusable across similar CLI commands.
"""
import typing as t

from orquestra.sdk import exceptions
from orquestra.sdk._base import _services
from orquestra.sdk._base._config import IN_PROCESS_CONFIG_NAME
from orquestra.sdk._base._spaces._structs import ProjectRef
from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.ir import TaskInvocationId
from orquestra.sdk.schema.workflow_run import (
    ProjectId,
    State,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
    WorkspaceId,
)

from . import _repos
from ._ui import _presenters, _prompts


def _check_for_in_process(config_names: t.Sequence[ConfigName]):
    if IN_PROCESS_CONFIG_NAME in config_names:
        raise exceptions.InProcessFromCLIError()


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
            _check_for_in_process((config,))
            return config

        # 1.2. Prompt the user.
        config_names = self._config_repo.list_config_names()
        selected_config = self._prompter.choice(config_names, message="Runtime config")
        return selected_config

    def resolve_stored_config_for_login(
        self, config: t.Optional[ConfigName]
    ) -> ConfigName:
        """
        Resolve the name of a config for logging in.

        This functions similarly to `resolve`, however we enforce two conditions:
        1. The resolved config name must correspond to a stored config
        2. The stored config must specify a URL
        """
        config_names = self._config_repo.list_config_names()
        valid_configs = [
            name
            for name in config_names
            if "uri" in self._config_repo.read_config(name).runtime_options.keys()
        ]

        if config is not None and config in valid_configs:
            return config

        message = "Please select a valid config"

        # The user specified a config, but it doesn't exist
        if config is not None and config not in config_names:
            message = f"No config '{config}' found in file. " + message
        elif config is not None and config in config_names:
            message = (
                f"Cannot log in with '{config}' as it relates to local runs. " + message
            )

        return self._prompter.choice(valid_configs, message=message)

    def resolve_multiple(
        self, configs: t.Optional[t.Sequence[str]]
    ) -> t.Sequence[ConfigName]:
        if configs is not None and len(configs) > 0:
            _check_for_in_process(configs)
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
            _check_for_in_process((config,))
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


class SpacesResolver:
    """
    Resolve values related to the workspace / project paradigm.
    """

    def __init__(
        self,
        spaces=_repos.SpacesRepo(),
        prompter=_prompts.Prompter(),
        presenter=_presenters.PromptPresenter(),
    ):
        self._spaces_repo = spaces
        self._prompter = prompter
        self._presenter = presenter

    def resolve_workspace_id(
        self,
        config: ConfigName,
        workspace_id: t.Optional[WorkspaceId] = None,
    ) -> WorkspaceId:
        """
        Resolve the value of the workspace ID.

        If the ID hasn't been specified, prompts the user to pick from the available
        workspaces.
        """
        if workspace_id is not None:
            return workspace_id

        workspaces = self._spaces_repo.list_workspaces(config)
        labels = self._presenter.workspaces_list_to_prompt(workspaces)
        prompt_choices = [(label, ws) for label, ws in zip(labels, workspaces)]
        selected_id = self._prompter.choice(prompt_choices, message="Workspace")

        return selected_id.workspace_id

    def resolve_project_id(
        self,
        config: ConfigName,
        workspace_id: WorkspaceId,
        project_id: t.Optional[ProjectId] = None,
        optional: bool = False,
    ) -> t.Optional[ProjectId]:
        """
        Resolve the value of the Project ID.

        If the ID hasn't been specified, prompts the user to pick from the available
        projects.

        If `optional` is set to True, adds an `All` option that returns `None` for the
        ID.
        """

        if project_id is not None:
            return project_id

        projects = self._spaces_repo.list_projects(config, workspace_id)
        labels = self._presenter.project_list_to_prompt(projects)
        if optional:
            projects.append(None)
            labels.append("All")
        prompt_choices = [(label, project) for label, project in zip(labels, projects)]

        selected_id = self._prompter.choice(prompt_choices, message="Projects")
        if selected_id is not None:
            return selected_id.project_id
        return None


class WFRunResolver:
    """
    Resolves value of `wf_run_id` based on `config`.
    """

    def __init__(
        self,
        wf_run_repo=_repos.WorkflowRunRepo(),
        prompter=_prompts.Prompter(),
        presenter=_presenters.PromptPresenter(),
        spaces_resolver=SpacesResolver(),
    ):
        self._wf_run_repo = wf_run_repo
        self._prompter = prompter
        self._presenter = presenter
        self._spaces_resolver = spaces_resolver

    def resolve_id(
        self, wf_run_id: t.Optional[WorkflowRunId], config: ConfigName
    ) -> WorkflowRunId:
        if wf_run_id is not None:
            return wf_run_id

        try:
            resolved_workspace_id = self._spaces_resolver.resolve_workspace_id(config)
            resolved_project_id = self._spaces_resolver.resolve_project_id(
                config, workspace_id=resolved_workspace_id
            )
        except exceptions.WorkspacesNotSupportedError:
            # if run on runtime that doesn't support workspaces
            resolved_workspace_id = None
            resolved_project_id = None

        wfs = self._wf_run_repo.list_wf_runs(
            config, workspace=resolved_workspace_id, project=resolved_project_id
        )

        wfs, tabulated_labels = self._presenter.wf_list_for_prompt(wfs)
        prompt_choices = [(label, wf.id) for label, wf in zip(tabulated_labels, wfs)]
        selected_id = self._prompter.choice(prompt_choices, message="Workflow run ID")

        return selected_id

    def resolve_run(
        self, wf_run_id: t.Optional[WorkflowRunId], config: ConfigName
    ) -> WorkflowRun:
        if wf_run_id is not None:
            return self._wf_run_repo.get_wf_by_run_id(wf_run_id, config)

        try:
            resolved_workspace_id = self._spaces_resolver.resolve_workspace_id(config)
            resolved_project_id = self._spaces_resolver.resolve_project_id(
                config, workspace_id=resolved_workspace_id
            )
        except exceptions.WorkspacesNotSupportedError:
            resolved_workspace_id = None
            resolved_project_id = None

        runs = self._wf_run_repo.list_wf_runs(
            config, workspace=resolved_workspace_id, project=resolved_project_id
        )

        runs, tabulated_labels = self._presenter.wf_list_for_prompt(runs)
        prompt_choices = [(label, wf) for label, wf in zip(tabulated_labels, runs)]

        selected_run = self._prompter.choice(prompt_choices, message="Workflow run ID")

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
        manage_all: t.Optional[bool],
    ) -> t.List[_services.Service]:
        ray = _services.RayManager()

        if all(s is None for s in (manage_ray, manage_all)):
            # No options passed, we only start Ray by default.
            return [ray]

        managed_services: t.List[_services.Service] = []

        if manage_ray or manage_all:
            managed_services.append(ray)

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
