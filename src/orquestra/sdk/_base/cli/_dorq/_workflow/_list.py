################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for 'orq workflow list'.
"""
import typing as t

from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import State, WorkflowRun, WorkflowRunId

from .. import _arg_resolvers, _repos
from .._ui import _presenters


class Action:
    """
    Encapsulates app-related logic for handling ``orq workflow view``.
    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._list.Action``.
    """

    def __init__(
        self,
        presenter=_presenters.WrappedCorqOutputPresenter(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_resolver: t.Optional[_arg_resolvers.ConfigResolver] = None,
        wf_run_filter_resolver: t.Optional[_arg_resolvers.WFRunFilterResolver] = None,
    ):
        # data sources
        self._wf_run_repo = wf_run_repo

        # arg resolvers
        self._config_resolver = config_resolver or _arg_resolvers.ConfigResolver()
        self._wf_run_filter_resolver = (
            wf_run_filter_resolver or _arg_resolvers.WFRunFilterResolver()
        )

        # text IO
        self._presenter = presenter

    def on_cmd_call(
        self,
        config: t.Optional[t.Sequence[str]],
        limit: t.Optional[int],
        max_age: t.Optional[str],
        state: t.Optional[t.List[str]],
        interactive: t.Optional[bool] = False,
    ):
        try:
            self._on_cmd_call_with_exceptions(
                config=config,
                limit=limit,
                max_age=max_age,
                state=state,
                interactive=interactive,
            )
        except Exception as e:
            self._presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        config: t.Optional[t.Sequence[str]],
        limit: t.Optional[int],
        max_age: t.Optional[str],
        state: t.Optional[t.List[str]],
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
            wf_runs += self._wf_run_repo.list_wf_runs(
                resolved_config,
                limit=resolved_limit,
                max_age=resolved_max_age,
                state=resolved_state,
            )

        # Display to the user
        self._presenter.show_wf_runs_list(wf_runs)
