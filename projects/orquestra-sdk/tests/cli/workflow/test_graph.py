################################################################################
# © Copyright 2024  Zapata Computing Inc.
################################################################################

"""
Unit tests for 'orq wf graph' glue code.
"""

from unittest.mock import create_autospec

from orquestra.sdk._client._base.cli import _arg_resolvers, _repos
from orquestra.sdk._client._base.cli._ui import _presenters
from orquestra.sdk._client._base.cli._workflow import _graph
from orquestra.sdk._shared.schema.ir import WorkflowDef
from orquestra.sdk._shared.schema.workflow_run import WorkflowRun


class TestAction:
    """
    Test boundaries::
        [_graph.Action]->[arg resolvers]
                       ->[repos]
                       ->[presenter]
    """

    @staticmethod
    def test_data_passing():
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        wf_run_id = "<wf run ID sentinel>"
        config = "<config sentinel>"

        # Resolved values
        resolved_config = "<resolved config>"

        # Mocks
        wf_run = create_autospec(
            WorkflowRun, workflow_def=(wf_def := create_autospec(WorkflowDef))
        )

        error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

        graph_presenter = create_autospec(_presenters.GraphPresenter)

        config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)
        config_resolver.resolve.return_value = resolved_config

        wf_run_resolver = create_autospec(_arg_resolvers.WFRunResolver)
        wf_run_resolver.resolve_run.return_value = wf_run

        action = _graph.Action(
            error_presenter=error_presenter,
            graph_presenter=graph_presenter,
            config_resolver=config_resolver,
            wf_run_resolver=wf_run_resolver,
        )

        # When
        action.on_cmd_call(wf_run_id=wf_run_id, config=config)

        # Then
        # We should pass input CLI args to config resolver.
        config_resolver.resolve.assert_called_with(wf_run_id, config)

        # We should pass resolved_config to run ID resolver.
        wf_run_resolver.resolve_run.assert_called_with(wf_run_id, resolved_config)

        # We expect printing the workflow run returned from the repo.
        graph_presenter.view.assert_called_with(wf_def)
        error_presenter.assert_not_called()
