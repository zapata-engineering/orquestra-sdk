################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################

"""Code for 'orq workflow graph'."""

import typing as t

from orquestra.sdk._shared.schema.configs import ConfigName
from orquestra.sdk._shared.schema.workflow_run import WorkflowRunId

from .. import _arg_resolvers
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq workflow graph``.

    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._graph.Action``.
    """

    def __init__(
        self,
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        graph_presenter=_presenters.GraphPresenter(),
        config_resolver=_arg_resolvers.WFConfigResolver(),
        wf_run_resolver=_arg_resolvers.WFRunResolver(),
    ):
        # arg resolvers
        self._config_resolver = config_resolver
        self._wf_run_resolver = wf_run_resolver

        # text IO
        self._error_presenter = error_presenter

        # graphical IO
        self._graph_presenter = graph_presenter

    def on_cmd_call(self, *args, **kwargs):
        try:
            self._on_cmd_call_with_exceptions(*args, **kwargs)
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        workflow: t.Optional[t.Union[WorkflowRunId, str]],
        config: t.Optional[ConfigName],
        workspace_id: t.Optional[str],
        wf_run_id: t.Optional[WorkflowRunId],
        module: t.Optional[str],
        name: t.Optional[str],
    ):
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        wf_run = self._wf_run_resolver.resolve_run(wf_run_id, resolved_config)

        self._graph_presenter.view(wf_run.workflow_def)
