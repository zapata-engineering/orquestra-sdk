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
from orquestra.sdk._client._base.cli._services import _up
from orquestra.sdk._client._base.cli._ui import _presenters
from orquestra.sdk._shared.schema.responses import ServiceResponse


class TestAction:
    """
    Test boundary::

        [_up.Action]->[Prompter]
    """

    class TestPassingAllValues:
        @staticmethod
        @pytest.fixture
        def service():
            service = create_autospec(_services.Service)
            service.name = "testing"

            return service

        @staticmethod
        @pytest.fixture
        def action(service):
            service_resolver = create_autospec(_arg_resolvers.ServiceResolver)
            service_resolver.resolve.return_value = [service]

            action = _up.Action(
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
            action._presenter.progress_spinner.assert_called_with("Starting")
            action._presenter.show_services.assert_called_with(
                services=[
                    ServiceResponse(name=service.name, is_running=True, info="Started!")
                ]
            )

        @staticmethod
        def test_failure(service, action: _up.Action):
            # Given
            presenter = create_autospec(_presenters.ServicePresenter)
            action._presenter = presenter
            service.up.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=[
                    "ray",
                    "start",
                    "--head",
                    "--temp-dir=.",
                    "--storage=.",
                    "--plasma-directory=.",
                ],
                output=(
                    "Usage stats collection is disabled.\nLocal node IP: 127.0.0.1"
                ).encode(),
                stderr=inspect.cleandoc(
                    """
                        File "pyarrow/error.pxi", line 100, in pyarrow.lib.check_status
                        pyarrow.lib.ArrowInvalid: URI has empty scheme: '.'
                    """
                ).encode(),
            )

            # When
            action.on_cmd_call(manage_ray=None, manage_all=None)

            # Then
            action._presenter.show_failure.assert_called_with(
                [
                    ServiceResponse(
                        name=service.name,
                        is_running=False,
                        info=inspect.cleandoc(
                            """
                               command:
                               ['ray', 'start', '--head', '--temp-dir=.', '--storage=.', '--plasma-directory=.']
                               stdout:
                               Usage stats collection is disabled.
                               Local node IP: 127.0.0.1
                               stderr:
                               File "pyarrow/error.pxi", line 100, in pyarrow.lib.check_status
                               pyarrow.lib.ArrowInvalid: URI has empty scheme: '.'
                           """  # noqa: E501
                        ),
                    )
                ]
            )
