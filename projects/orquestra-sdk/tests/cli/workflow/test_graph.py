################################################################################
# © Copyright 2024  Zapata Computing Inc.
################################################################################

"""
Unit tests for 'orq wf graph' glue code.
"""

from pathlib import Path
from typing import Optional
from unittest.mock import create_autospec

import pytest
from orquestra.workflow_shared.schema.ir import WorkflowDef
from orquestra.workflow_shared.schema.workflow_run import WorkflowRun

from orquestra.sdk._client._base.cli import _arg_resolvers, _repos
from orquestra.sdk._client._base.cli._ui import _presenters, _prompts
from orquestra.sdk._client._base.cli._workflow import _graph


@pytest.mark.parametrize("file", [None, create_autospec(Path)])
class TestAction:
    """
    Test boundaries::
        [_graph.Action]->[arg resolvers]
                       ->[repos]
                       ->[presenter]
    """

    class TestDataPassing:
        """
        Verifies how we pass variables between subcomponents.
        """

        @staticmethod
        def test_previously_submitted_workflow_path(file: Optional[Path]):
            """
            Verifies how we pass variables between subcomponents.
            """
            # Given
            # CLI inputs
            config = "<config sentinel>"
            wf_run_id = "<wf run ID sentinel>"

            # Resolved values
            resolved_config = "<resolved config>"

            # Mocks
            wf_run = create_autospec(
                WorkflowRun, workflow_def=create_autospec(WorkflowDef)
            )

            error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            graph_presenter = create_autospec(_presenters.GraphPresenter)
            config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config
            wf_run_resolver = create_autospec(_arg_resolvers.WFRunResolver)
            wf_run_resolver.resolve_run.return_value = wf_run
            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.wf_def_to_graphviz.return_value = (
                resolved_graph := "<resolved graph>"
            )
            wf_def_resolver = create_autospec(_arg_resolvers.WFDefResolver)
            prompter = create_autospec(_prompts.Prompter)

            action = _graph.Action(
                prompter=prompter,
                error_presenter=error_presenter,
                graph_presenter=graph_presenter,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
                wf_def_resolver=wf_def_resolver,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(wf_run_id=wf_run_id, config=config, file=file)

            # Then
            # We should pass input CLI args to config resolver.
            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_resolver.resolve_run.assert_called_with(wf_run_id, resolved_config)

            # We expect printing the workflow run returned from the repo.
            graph_presenter.view.assert_called_with(resolved_graph, file)
            error_presenter.assert_not_called()
            prompter.assert_not_called()
            wf_def_resolver.assert_not_called()

        @staticmethod
        def test_local_workflowdef_path(file: Optional[Path]):
            """
            Verifies how we pass variables between subcomponents.
            """
            # Given
            # CLI inputs
            module = "<module sentinel>"
            name = "<name sentinel>"

            # Mocks
            error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            graph_presenter = create_autospec(_presenters.GraphPresenter)
            config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)
            wf_run_resolver = create_autospec(_arg_resolvers.WFRunResolver)
            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_resolver = create_autospec(_arg_resolvers.WFDefResolver)
            prompter = create_autospec(_prompts.Prompter)

            wf_def_resolver.resolve_module_spec.return_value = (
                resolved_module_spec := "<resolved module spec sentinel>"
            )
            wf_def_repo.get_module_from_spec.return_value = (
                resolved_module := "<resolved module sentinel>"
            )
            wf_def_resolver.resolve_fn_name.return_value = (
                resolved_name := "<resolved fn name sentinel>"
            )
            wf_def_repo.get_workflow_def.return_value = create_autospec(
                WorkflowRun, graph=(resolved_graph := "<graph sentinel>")
            )

            action = _graph.Action(
                prompter=prompter,
                error_presenter=error_presenter,
                graph_presenter=graph_presenter,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
                wf_def_resolver=wf_def_resolver,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(module=module, name=name, file=file)

            # Then
            wf_def_resolver.resolve_module_spec.assert_called_once_with(module)
            wf_def_repo.get_module_from_spec.assert_called_once_with(
                resolved_module_spec
            )
            wf_def_resolver.resolve_fn_name.assert_called_once_with(
                resolved_module, name
            )
            wf_def_repo.get_workflow_def.assert_called_once_with(
                resolved_module, resolved_name
            )
            graph_presenter.view.assert_called_once_with(resolved_graph, file)

            error_presenter.assert_not_called()
            prompter.assert_not_called()
            wf_def_resolver.assert_not_called()
            config_resolver.assert_not_called()
