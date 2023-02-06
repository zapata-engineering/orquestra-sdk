from unittest.mock import PropertyMock, create_autospec

import pytest

from orquestra.sdk._base.cli._dorq._login import _login
from orquestra.sdk._base.cli._dorq._repos import ConfigRepo, RuntimeRepo, TokenRepo
from orquestra.sdk._base.cli._dorq._ui._presenters import (
    LoginPresenter,
    WrappedCorqOutputPresenter,
)


@pytest.mark.parametrize("ce", [True, False])
class TestAction:
    """
    Test boundaries::
        [_up.Action]->[arg resolvers]
                    ->[presenter]
    """

    @staticmethod
    def test_passed_server_no_token(ce):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        url = "my_url"
        login_url = "config_url"
        config_name = "cfg"
        token = None
        retrieved_token = "mocked_token"

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        token_repo = create_autospec(TokenRepo)

        token_repo.get_token.return_value = (retrieved_token, login_url)
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            token_repo=token_repo,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        exception_presenter.show_error.assert_not_called()
        token_repo.get_token.assert_called_once_with(url, ce)
        config_repo.store_token_in_config.assert_called_once_with(
            url, retrieved_token, ce
        )
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)

    @staticmethod
    def test_passed_server_no_token_auto_login_failed(ce):
        # Given
        # CLI inputs
        url = "my_url"
        login_url = "config_url"
        config_name = "cfg"
        token = None
        retrieved_token = None

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        token_repo = create_autospec(TokenRepo)

        token_repo.get_token.return_value = (retrieved_token, login_url)
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            token_repo=token_repo,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        exception_presenter.show_error.assert_not_called()
        token_repo.get_token.assert_called_once_with(url, ce)
        config_repo.store_token_in_config.assert_not_called()
        login_presenter.prompt_config_saved.assert_not_called()
        login_presenter.prompt_for_login.assert_called_once_with(login_url, url, ce)

    @staticmethod
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
        token_repo = create_autospec(TokenRepo)

        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            token_repo=token_repo,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        # We should get the login url from QE
        exception_presenter.show_error.assert_not_called()
        token_repo.get_token.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(url, token, ce)
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)
