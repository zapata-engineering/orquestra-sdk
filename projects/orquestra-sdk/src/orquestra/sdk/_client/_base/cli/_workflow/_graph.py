################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################

"""Code for 'orq workflow graph'."""

import typing as t
from pathlib import Path

from graphviz import Digraph  # type: ignore
from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.schema.workflow_run import WorkflowRun, WorkflowRunId

from .. import _arg_resolvers, _repos
from .._ui import _presenters, _prompts


class Action:
    """Encapsulates app-related logic for handling ``orq workflow graph``.

    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._graph.Action``.
    """

    def __init__(
        self,
        prompter=_prompts.Prompter(),
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        graph_presenter=_presenters.GraphPresenter(),
        config_resolver=_arg_resolvers.WFConfigResolver(),
        wf_run_resolver=_arg_resolvers.WFRunResolver(),
        wf_def_resolver=_arg_resolvers.WFDefResolver(),
        wf_def_repo=_repos.WorkflowDefRepo(),
    ):
        # arg resolvers
        self._config_resolver = config_resolver
        self._wf_run_resolver = wf_run_resolver
        self._wf_def_resolver = wf_def_resolver

        # text IO
        self._prompter = prompter
        self._error_presenter = error_presenter

        # graphical IO
        self._graph_presenter = graph_presenter

        # data sources
        self._wf_def_repo = wf_def_repo

    def on_cmd_call(self, *args, **kwargs):
        try:
            self._on_cmd_call_with_exceptions(*args, **kwargs)
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        # Submitted workflow run options
        config: t.Optional[ConfigName] = None,
        wf_run_id: t.Optional[WorkflowRunId] = None,
        # Local workflow def options
        module: t.Optional[str] = None,
        name: t.Optional[str] = None,
        # Applies in all cases
        file: t.Optional[Path] = None,
    ):
        # Set up combinations of args that correspond to the local definition path and
        # the previously submitted workflow path.
        submitted_args = [config, wf_run_id]
        local_args = [module, name]

        if any([arg is not None for arg in local_args]):
            # At least one argument unique to the local workflowdef path has been passed
            graph = self._resolve_local_workflow_def_graph(module, name)
        elif any([arg is not None for arg in submitted_args]):
            # At least one argument uniwue to the previously submitted workflowdef path
            # has been passed.
            graph = self._resolve_submitted_workflow_def_graph(config, wf_run_id)
        else:
            # We can't tell which path we're on from which args are provided, try each
            # path.
            try:
                graph = self._resolve_local_workflow_def_graph(module, name)
            except (exceptions.WorkflowDefinitionModuleNotFound, AssertionError):
                graph = self._resolve_submitted_workflow_def_graph(config, wf_run_id)

        # Display the graph
        self._graph_presenter.view(graph, file)

    def _resolve_local_workflow_def_graph(
        self, module: t.Optional[str], name: t.Optional[str]
    ) -> Digraph:
        """Resolve a graph from a local workflow definition."""
        resolved_module_spec = self._wf_def_resolver.resolve_module_spec(module)

        resolved_module = self._wf_def_repo.get_module_from_spec(resolved_module_spec)

        resolved_fn_name = self._wf_def_resolver.resolve_fn_name(resolved_module, name)

        resolved_wf_def = self._wf_def_repo.get_workflow_def(
            resolved_module, resolved_fn_name
        )
        return resolved_wf_def.graph

    def _resolve_submitted_workflow_def_graph(
        self, config: t.Optional[ConfigName], wf_run_id: t.Optional[WorkflowRunId]
    ) -> Digraph:
        """Resolve a graph from the definition of a submitted workflow."""
        resolved_config: ConfigName = self._config_resolver.resolve(wf_run_id, config)
        wf_run: WorkflowRun = self._wf_run_resolver.resolve_run(
            wf_run_id, resolved_config
        )
        return self._wf_def_repo.wf_def_to_graphviz(wf_run.workflow_def)
