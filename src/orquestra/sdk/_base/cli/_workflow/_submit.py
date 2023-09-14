################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq workflow submit'."""
import typing as t

from orquestra.sdk import exceptions

from .. import _arg_resolvers, _repos
from .._ui import _presenters, _prompts


class Action:
    """Encapsulates app-related logic for handling ``orq workflow submit``.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._submit.Action``.
    """

    def __init__(
        self,
        prompter=_prompts.Prompter(),
        submit_presenter=_presenters.WFRunPresenter(),
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        wf_def_repo=_repos.WorkflowDefRepo(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_resolver: t.Optional[_arg_resolvers.ConfigResolver] = None,
        spaces_resolver: t.Optional[_arg_resolvers.SpacesResolver] = None,
    ):
        # text IO
        self._prompter = prompter
        self._submit_presenter = submit_presenter
        self._error_presenter = error_presenter

        # data sources
        self._wf_run_repo = wf_run_repo
        self._wf_def_repo = wf_def_repo
        self._config_resolver = config_resolver or _arg_resolvers.ConfigResolver(
            prompter=prompter
        )
        self._space_resolver = spaces_resolver or _arg_resolvers.SpacesResolver(
            prompter=prompter
        )

    def on_cmd_call(
        self,
        module: str,
        name: t.Optional[str],
        config: t.Optional[str],
        workspace_id: t.Optional[str],
        project_id: t.Optional[str],
        force: bool,
    ):
        try:
            self._on_cmd_call_with_exceptions(
                module, name, config, workspace_id, project_id, force
            )
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        module: str,
        name: t.Optional[str],
        config: t.Optional[str],
        workspace_id: t.Optional[str],
        project_id: t.Optional[str],
        force: bool,
    ):
        """Implementation of the command action. Doesn't catch exceptions."""
        # 1. Resolve config, workspace and project
        resolved_config = self._config_resolver.resolve(config)

        try:
            resolved_workspace_id = self._space_resolver.resolve_workspace_id(
                resolved_config, workspace_id
            )
            resolved_project_id = self._space_resolver.resolve_project_id(
                resolved_config, resolved_workspace_id, project_id
            )
        except exceptions.WorkspacesNotSupportedError:
            resolved_workspace_id = None
            resolved_project_id = None

        # 2. Resolve module with workflow defs
        try:
            resolved_module = self._wf_def_repo.get_module_from_spec(module)
        except exceptions.WorkflowDefinitionModuleNotFound:
            # Explicit re-raise
            raise

        # 3. Resolve the definition of the workflow to run
        resolved_fn_name: str
        if name is not None:
            # If passed in explicitly by the user: we take it as-is.
            resolved_fn_name = name
        else:
            # Get all workflow def names in the module
            try:
                wf_names = self._wf_def_repo.get_worklow_names(resolved_module)
            except exceptions.NoWorkflowDefinitionsFound:
                # Explicit re-raise
                raise

            if len(wf_names) == 1:
                # If there's only one workflow def: we submit it.
                resolved_fn_name = wf_names[0]
            else:
                # If there's more workflow defs in this module: we need to ask the user
                # which one to submit.
                resolved_fn_name = self._prompter.choice(
                    wf_names, message="Workflow definition"
                )

        resolved_wf_def = self._wf_def_repo.get_workflow_def(
            resolved_module, resolved_fn_name
        )
        try:
            wf_run_id = self._wf_run_repo.submit(
                resolved_wf_def,
                resolved_config,
                ignore_dirty_repo=force,
                workspace_id=resolved_workspace_id,
                project_id=resolved_project_id,
            )
        except exceptions.DirtyGitRepo:
            # Ask the user for the decision.
            override = self._prompter.confirm(
                "One of the tasks is defined in a repo with uncommitted local"
                " changes. Submit the workflow anyway?",
                default=False,
            )
            if override:
                wf_run_id = self._wf_run_repo.submit(
                    resolved_wf_def,
                    resolved_config,
                    ignore_dirty_repo=True,
                    workspace_id=workspace_id,
                    project_id=project_id,
                )
            else:
                # abort
                return

        self._submit_presenter.show_submitted_wf_run(wf_run_id)
