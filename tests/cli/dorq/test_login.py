from unittest.mock import MagicMock, Mock, call, create_autospec

import pytest

from orquestra.sdk._base.cli._dorq._login import _login
from orquestra.sdk._base.cli._dorq._repos import ConfigRepo, RuntimeRepo
from orquestra.sdk._base.cli._dorq._ui._presenters import (
    LoginPresenter,
    WrappedCorqOutputPresenter,
)


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
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)

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
        exception_presenter.show_error.assert_not_called()
        login_presenter.prompt_for_login.assert_called_once_with(config_url, url, ce)

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
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)

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
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(url, token, ce)
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)
