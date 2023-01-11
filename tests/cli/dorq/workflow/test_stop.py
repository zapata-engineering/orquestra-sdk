################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base.cli._dorq._workflow import _stop


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

        config_resolver = Mock()
        config_resolver.resolve.return_value = resolved_config

        wf_run_id_resolver = Mock()
        wf_run_id_resolver.resolve.return_value = resolved_id

        action = _stop.Action(
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
        wf_run_repo.stop.assert_called_with(resolved_id, resolved_config)
        # We should pass resolved ID to presenter.
        presenter.show_stopped_wf_run.assert_called_with(resolved_id)

    @staticmethod
    @pytest.mark.parametrize("exc", [ConnectionError(), exceptions.UnauthorizedError()])
    def test_handling_errors(exc):
        # Given
        # CLI inputs
        wf_run_id = "<wf run ID sentinel>"
        config = "<config sentinel>"

        # Mocks
        presenter = Mock()

        wf_run_repo = Mock()
        wf_run_repo.stop.side_effect = exc

        config_resolver = Mock()

        wf_run_id_resolver = Mock()

        action = _stop.Action(
            presenter=presenter,
            wf_run_repo=wf_run_repo,
            config_resolver=config_resolver,
            wf_run_id_resolver=wf_run_id_resolver,
        )

        # When
        action.on_cmd_call(wf_run_id, config)

        # Then
        # We expect telling the user about the error.
        presenter.show_error.assert_called_with(exc)
