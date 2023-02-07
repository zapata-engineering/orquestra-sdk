import asyncio
from unittest.mock import Mock, PropertyMock, create_autospec

import pytest
from aiohttp import web

from orquestra.sdk._base.cli._dorq._login import _login, _login_server
from orquestra.sdk._base.cli._dorq._repos import ConfigRepo, RuntimeRepo
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

    @pytest.fixture
    def async_sleep(self, monkeypatch: pytest.MonkeyPatch):
        sleep = create_autospec(asyncio.sleep)
        monkeypatch.setattr(asyncio, "sleep", sleep)
        return sleep

    @staticmethod
    def test_passed_server_no_token(async_sleep, ce):
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
        login_server = create_autospec(_login_server.LoginServer)

        config_repo.store_token_in_config.return_value = config_name
        runtime_repo.get_login_url.return_value = login_url
        type(login_server).token = PropertyMock(return_value=retrieved_token)

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        async_sleep.assert_called_once()
        exception_presenter.show_error.assert_not_called()
        login_presenter.open_url_in_browser.assert_called_once_with(login_url)
        login_presenter.print_login_help.assert_called_once()
        config_repo.store_token_in_config.assert_called_once_with(
            url, retrieved_token, ce
        )
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)

    @staticmethod
    def test_passed_server_no_token_server_exits_gracefully(
        monkeypatch: pytest.MonkeyPatch, ce
    ):
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
        login_server = create_autospec(_login_server.LoginServer)

        config_repo.store_token_in_config.return_value = config_name
        runtime_repo.get_login_url.return_value = login_url
        type(login_server).token = PropertyMock(return_value=retrieved_token)

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
        )
        monkeypatch.setattr(
            action, "_get_token_from_server", Mock(side_effect=web.GracefulExit)
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(
            url, retrieved_token, ce
        )
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)

    @staticmethod
    def test_passed_server_no_token_auto_login_failed(async_sleep, ce):
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
        login_server = create_autospec(_login_server.LoginServer)

        config_repo.store_token_in_config.return_value = config_name
        runtime_repo.get_login_url.return_value = login_url
        login_presenter.open_url_in_browser.return_value = False
        type(login_server).token = PropertyMock(return_value=retrieved_token)

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        async_sleep.assert_not_called()
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_not_called()
        login_presenter.prompt_config_saved.assert_not_called()
        login_presenter.prompt_for_login.assert_called_once_with(login_url, url, ce)

    @staticmethod
    def test_passed_server_and_token(async_sleep, ce):
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
        login_server = create_autospec(_login_server.LoginServer)

        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
        )

        # When
        action.on_cmd_call(url=url, token=token, ce=ce)

        # Then
        # We should get the login url from QE
        async_sleep.assert_not_called()
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(url, token, ce)
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)
