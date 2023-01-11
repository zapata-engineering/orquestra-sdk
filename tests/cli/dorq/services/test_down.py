################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from unittest.mock import MagicMock, Mock, PropertyMock

import pytest

from orquestra.sdk._base.cli._dorq._services import _down


class TestAction:
    """
    Test boundaries::
        [_up.Action]->[arg resolvers]
                    ->[presenter]
    """

    @staticmethod
    def test_data_passing():
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        manage_ray = None
        manage_fluentbit = None
        manage_all = None

        # Resolved values
        service = Mock()
        type(service).name = PropertyMock(return_value="mocked")
        service.is_running.return_value = True
        resolved_services = [service]

        # Mocks
        presenter = MagicMock()
        presenter.show_progress.return_value.__enter__.return_value = resolved_services
        presenter.show_progress.return_value.__exit__.return_value = False

        service_resolver = Mock()
        service_resolver.resolve.return_value = resolved_services

        action = _down.Action(
            presenter=presenter,
            service_resolver=service_resolver,
        )

        # When
        action.on_cmd_call(
            manage_ray=manage_ray, manage_fluent=manage_fluentbit, manage_all=manage_all
        )

        # Then
        # We should pass input CLI args to config resolver.
        service_resolver.resolve.assert_called_with(
            manage_ray, manage_fluentbit, manage_all
        )

        service.down.assert_called()
        service.is_running.assert_called()

        # We should call the progress bar and print any services
        presenter.show_progress.assert_called_with(resolved_services, label="Stopping")
        presenter.show_services.assert_called()
