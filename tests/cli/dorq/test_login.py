from unittest.mock import MagicMock, Mock, PropertyMock, call

import pytest

from orquestra.sdk._base.cli._dorq._login import _login
from orquestra.sdk.schema.responses import ResponseStatusCode


class TestAction:
    """
    Test boundaries::
        [_up.Action]->[arg resolvers]
                    ->[presenter]
    """

    @staticmethod
    @pytest.mark.parametrize("ce", [True, False])
    def test_passed_server_no_token(ce):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        url = "my_url"
        config_url = "config_url"
        token = None

        # Mocks
        exception_presenter = MagicMock()
        login_presenter = MagicMock()
        runtime_repo = MagicMock()
        config_repo = MagicMock()
        runtime_repo.get_login_url.return_value = config_url

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        # We should get the login url from QE
        assert runtime_repo.mock_calls.count(call.get_login_url(url, ce)) == 1
        assert (
            login_presenter.mock_calls.count(call.prompt_for_login(config_url, url))
            == 1
        )

    @staticmethod
    @pytest.mark.parametrize("ce", [True, False])
    def test_passed_server_and_token(ce):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        url = "my_url"
        token = "my_token"

        config_name = "cfg"
        exception_presenter = MagicMock()
        login_presenter = MagicMock()
        runtime_repo = MagicMock()
        config_repo = MagicMock()
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        # We should get the login url from QE
        assert (
            config_repo.mock_calls.count(call.store_token_in_config(url, token, ce))
            == 1
        )
        assert (
            login_presenter.mock_calls.count(call.prompt_config_saved(url, config_name))
            == 1
        )
