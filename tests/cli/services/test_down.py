################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for ``orq services up`` CLI action.
"""
import inspect
import subprocess
from unittest.mock import create_autospec

import pytest

from orquestra.sdk._client._base import _services
from orquestra.sdk._client._base.cli import _arg_resolvers
from orquestra.sdk._client._base.cli._services import _down
from orquestra.sdk._client._base.cli._ui import _presenters
from orquestra.sdk.shared.schema.responses import ServiceResponse


class TestAction:
    """
    Test boundary::

        [_down.Action]->[Prompter]
    """

    class TestPassingAllValues:
        @staticmethod
        @pytest.fixture
        def service():
            service = create_autospec(_services.Service)
            service.name = "testing"
            service.is_running.return_value = False

            return service

        @staticmethod
        @pytest.fixture
        def action(service):
            service_resolver = create_autospec(_arg_resolvers.ServiceResolver)
            service_resolver.resolve.return_value = [service]

            action = _down.Action(
                service_resolver=service_resolver,
            )

            return action

        @staticmethod
        def test_success(service: _services.Service, action):
            # Given
            presenter = create_autospec(_presenters.ServicePresenter)
            action._presenter = presenter

            # When
            action.on_cmd_call(manage_ray=None, manage_all=None)

            # Then
            action._presenter.progress_spinner.assert_called_with("Stopping")
            action._presenter.show_services.assert_called_with(
                services=[
                    ServiceResponse(name=service.name, is_running=False, info=None)
                ]
            )

        @staticmethod
        def test_failure(service, action: _down.Action):
            # Given
            service.down.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=[
                    "ray",
                    "stop",
                ],
                output=b"",
                stderr=inspect.cleandoc(
                    """
                        Could not terminate `...` due to ...
                    """
                ).encode(),
            )
            presenter = create_autospec(_presenters.ServicePresenter)
            action._presenter = presenter

            # When
            action.on_cmd_call(manage_ray=None, manage_all=None)

            # Then
            action._presenter.show_failure.assert_called_with(
                [
                    ServiceResponse(
                        name=service.name,
                        is_running=True,
                        info=inspect.cleandoc(
                            """
                               command:
                               ['ray', 'stop']
                               stdout:
                               stderr:
                               Could not terminate `...` due to ...
                           """  # noqa: E501
                        ),
                    )
                ]
            )
