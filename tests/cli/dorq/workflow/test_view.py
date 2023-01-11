################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf view' glue code.
"""

from unittest.mock import Mock

from orquestra.sdk._base.cli._dorq._workflow import _view


class TestAction:
    """
    Test boundaries::
        [_stop.Action]->[arg resolvers]
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
        resolved_id = "<resolved ID>"
        resolved_config = "<resolved config>"

        # Mocks
        presenter = Mock()
        wf_run_repo = Mock()
        wf_run = "<wf run sentinel>"
        wf_run_repo.get_wf_by_run_id.return_value = wf_run

        config_resolver = Mock()
        config_resolver.resolve.return_value = resolved_config

        wf_run_id_resolver = Mock()
        wf_run_id_resolver.resolve.return_value = resolved_id

        action = _view.Action(
            presenter=presenter,
            wf_run_repo=wf_run_repo,
            config_resolver=config_resolver,
            wf_run_id_resolver=wf_run_id_resolver,
        )

        # When
        action.on_cmd_call(wf_run_id=wf_run_id, config=config)

        # Then
        # We should pass input CLI args to config resolver.
        config_resolver.resolve.assert_called_with(wf_run_id, config)

        # We should pass resolved_config to run ID resolver.
        wf_run_id_resolver.resolve.assert_called_with(wf_run_id, resolved_config)

        # We should pass resolved values to run repo.
        wf_run_repo.get_wf_by_run_id.assert_called_with(
            wf_run_id=resolved_id, config_name=resolved_config
        )

        # We expect printing the workflow run returned from the repo.
        presenter.show_wf_run.assert_called_with(wf_run)
