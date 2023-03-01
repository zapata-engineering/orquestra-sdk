################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf view' glue code.
"""
import typing as t
from unittest.mock import create_autospec

from orquestra.sdk._base.cli._dorq import _arg_resolvers, _repos
from orquestra.sdk._base.cli._dorq._ui import _presenters
from orquestra.sdk._base.cli._dorq._workflow import _view


class TestAction:
    """
    Test boundaries::
        [_view.Action]->[arg resolvers]
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
        summary: t.Any = "<wf run summary sentinel>"

        # Resolved values
        resolved_config = "<resolved config>"

        # Mocks
        wf_run_presenter = create_autospec(_presenters.WFRunPresenter)
        error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

        summary_repo = create_autospec(_repos.SummaryRepo)
        summary_repo.wf_run_summary.return_value = summary

        config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)
        config_resolver.resolve.return_value = resolved_config

        wf_run_resolver = create_autospec(_arg_resolvers.WFRunResolver)
        wf_run_resolver.resolve_id.return_value = wf_run_id

        action = _view.Action(
            wf_run_presenter=wf_run_presenter,
            error_presenter=error_presenter,
            summary_repo=summary_repo,
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
        wf_run_presenter.show_wf_run.assert_called_with(summary)
        error_presenter.assert_not_called()
