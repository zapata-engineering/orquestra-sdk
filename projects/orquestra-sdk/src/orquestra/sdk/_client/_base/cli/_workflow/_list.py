################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq workflow list'."""
import typing as t

from orquestra.workflow_shared import exceptions as exceptions
from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.schema.workflow_run import (
    WorkflowRunSummary,
    WorkspaceId,
)

from .. import _arg_resolvers, _repos
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq workflow list``.

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
        config: t.Optional[str],
        limit: t.Optional[int],
        max_age: t.Optional[str],
        state: t.Optional[t.List[str]],
        workspace_id: t.Optional[str],
        interactive: t.Optional[bool] = False,
    ):
        try:
            self._on_cmd_call_with_exceptions(
                config=config,
                limit=limit,
                max_age=max_age,
                state=state,
                workspace_id=workspace_id,
                interactive=interactive,
            )
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        config: t.Optional[str],
        limit: t.Optional[int],
        max_age: t.Optional[str],
        state: t.Optional[t.List[str]],
        workspace_id: t.Optional[str],
        interactive: t.Optional[bool] = False,
    ):
        # Resolve Arguments
        resolved_config: ConfigName = self._config_resolver.resolve(config)

        resolved_limit = self._wf_run_filter_resolver.resolve_limit(
            limit, interactive=interactive
        )
        resolved_max_age = self._wf_run_filter_resolver.resolve_max_age(
            max_age, interactive=interactive
        )
        resolved_state = self._wf_run_filter_resolver.resolve_state(
            state, interactive=interactive
        )

        # Resolve Workspace and Project for this config
        workspace: t.Optional[WorkspaceId] = None

        try:
            workspace = self._spaces_resolver.resolve_workspace_id(
                resolved_config, workspace_id
            )
        except exceptions.WorkspacesNotSupportedError:
            pass

        wf_runs: t.List[WorkflowRunSummary] = self._wf_run_repo.list_wf_run_summaries(
            resolved_config,
            workspace=workspace,
            limit=resolved_limit,
            max_age=resolved_max_age,
            state=resolved_state,
        )

        summary = self._summary_repo.wf_list_summary(wf_runs)
        # Display to the user
        self._presenter.show_wf_list(summary)
