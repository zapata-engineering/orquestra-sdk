################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq workflow view'."""
import typing as t

from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.schema.workflow_run import WorkflowRunId

from .. import _arg_resolvers, _repos
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq workflow view``.

    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._view.Action``.
    """

    def __init__(
        self,
        wf_run_presenter=_presenters.WFRunPresenter(),
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        summary_repo=_repos.SummaryRepo(),
        config_resolver=_arg_resolvers.WFConfigResolver(),
        wf_run_resolver=_arg_resolvers.WFRunResolver(),
    ):
        # arg resolvers
        self._config_resolver = config_resolver
        self._wf_run_resolver = wf_run_resolver

        self._summary_repo = summary_repo

        # text IO
        self._wf_run_presenter = wf_run_presenter
        self._error_presenter = error_presenter

    def on_cmd_call(self, *args, **kwargs):
        try:
            self._on_cmd_call_with_exceptions(*args, **kwargs)
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self, wf_run_id: t.Optional[WorkflowRunId], config: t.Optional[ConfigName]
    ):
        # The order of resolving config and run ID is important. It dictactes the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        wf_run = self._wf_run_resolver.resolve_run(wf_run_id, resolved_config)
        summary = self._summary_repo.wf_run_summary(wf_run)

        self._wf_run_presenter.show_wf_run(summary)
