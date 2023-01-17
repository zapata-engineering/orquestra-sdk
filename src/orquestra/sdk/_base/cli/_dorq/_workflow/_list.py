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
        show_all: t.Optional[bool] = False,
        interactive: t.Optional[bool] = False,
    ):
        # This should be handled by the mutually exclusive constraint in the entry, but
        # we'll keep this here to be safe.
        assert not (show_all and interactive)

        if (not (show_all or interactive)) and (
            None in (limit, max_age, state, show_all)
        ):
            # neither the interactive nor all flags are set, and one or more filters
            # are not set. Warn the user that the unset filters will not be applied.
            filter_dict = {"limit": limit, "max_age": max_age, "state": state}

            unset_filters = [key for key in filter_dict if filter_dict[key] is None]
            print(
                "The following filters have not been set and will not be applied: "
                f"{unset_filters}."
                "\nTo set filters interactively use the '--interactive (-i)' flag, "
                "or '--all (-a)' to silence this warning."
                "\nAvailable filters are:"
                "\n  '--limit'  : maximum number of results to display for each config."
                "\n  '--max_age': upper limit for age of results to display."
                "\n  '--state'  : only show results with the specified state "
                "(can be used multiple times)."
            )

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
