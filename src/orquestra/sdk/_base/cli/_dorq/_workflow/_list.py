################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for 'orq workflow list'.
"""
import typing as t

from orquestra.sdk import exceptions as exceptions
from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import ProjectId, WorkflowRun, WorkspaceId

from .. import _arg_resolvers, _repos
from .._ui import _presenters


class Action:
    """
    Encapsulates app-related logic for handling ``orq workflow list``.
    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._list.Action``.
    """

    def __init__(
        self,
        presenter=_presenters.WFRunPresenter(),
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        summary_repo=_repos.SummaryRepo(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        spaces_resolver: t.Optional[_arg_resolvers.SpacesResolver] = None,
        config_resolver: t.Optional[_arg_resolvers.ConfigResolver] = None,
        wf_run_filter_resolver: t.Optional[_arg_resolvers.WFRunFilterResolver] = None,
    ):
        # data sources
        self._wf_run_repo = wf_run_repo

        # arg resolvers
        self._config_resolver = config_resolver or _arg_resolvers.ConfigResolver()
        self._spaces_resolver = spaces_resolver or _arg_resolvers.SpacesResolver()
        self._wf_run_filter_resolver = (
            wf_run_filter_resolver or _arg_resolvers.WFRunFilterResolver()
        )

        self._summary_repo = summary_repo
        # text IO
        self._presenter = presenter
        self._error_presenter = error_presenter

    def on_cmd_call(
        self,
        config: t.Optional[t.Sequence[str]],
        limit: t.Optional[int],
        max_age: t.Optional[str],
        state: t.Optional[t.List[str]],
        workspace_id: t.Optional[str],
        project_id: t.Optional[str],
        interactive: t.Optional[bool] = False,
    ):
        try:
            self._on_cmd_call_with_exceptions(
                config=config,
                limit=limit,
                max_age=max_age,
                state=state,
                workspace_id=workspace_id,
                project_id=project_id,
                interactive=interactive,
            )
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        config: t.Optional[t.Sequence[str]],
        limit: t.Optional[int],
        max_age: t.Optional[str],
        state: t.Optional[t.List[str]],
        workspace_id: t.Optional[str],
        project_id: t.Optional[str],
        interactive: t.Optional[bool] = False,
    ):
        # Resolve Arguments
        resolved_configs: t.Sequence[
            ConfigName
        ] = self._config_resolver.resolve_multiple(config)
        resolved_limit = self._wf_run_filter_resolver.resolve_limit(
            limit, interactive=interactive
        )
        resolved_max_age = self._wf_run_filter_resolver.resolve_max_age(
            max_age, interactive=interactive
        )
        resolved_state = self._wf_run_filter_resolver.resolve_state(
            state, interactive=interactive
        )

        # Get wf runs for each config
        wf_runs: t.List[WorkflowRun] = []
        for resolved_config in resolved_configs:
            # Resolve Workspace and Project for this config
            workspace: t.Optional[WorkspaceId] = None
            project: t.Optional[ProjectId] = None

            # If nethier workspace or project are specified, leave both as None.
            if workspace_id or project_id:
                try:
                    workspace = self._spaces_resolver.resolve_workspace_id(
                        resolved_config, workspace_id
                    )
                    project = self._spaces_resolver.resolve_project_id(
                        resolved_config, workspace, project_id, optional=True
                    )
                except exceptions.WorkspacesNotSupportedError:
                    # if handling on the runtime that doesn't support workspaces and
                    # projects - project and workspace are already set to None, so
                    # nothing to do.
                    assert project is None and workspace is None, (
                        "The project and workspace resolvers disagree about whether "
                        "spaces are supported. Please report this as a bug."
                    )
                    pass

            wf_runs += self._wf_run_repo.list_wf_runs(
                resolved_config,
                project=project,
                workspace=workspace,
                limit=resolved_limit,
                max_age=resolved_max_age,
                state=resolved_state,
            )

        summary = self._summary_repo.wf_list_summary(wf_runs)
        # Display to the user
        self._presenter.show_wf_list(summary)
