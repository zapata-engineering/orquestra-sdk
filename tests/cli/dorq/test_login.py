################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import asyncio
from unittest.mock import Mock, PropertyMock, create_autospec

import pytest
from aiohttp import web

from orquestra.sdk._base.cli._dorq._arg_resolvers import ConfigResolver
from orquestra.sdk._base.cli._dorq._login import _login, _login_server
from orquestra.sdk._base.cli._dorq._repos import ConfigRepo, RuntimeRepo
from orquestra.sdk._base.cli._dorq._ui._presenters import (
    LoginPresenter,
    WrappedCorqOutputPresenter,
)
from orquestra.sdk._base.cli._dorq._ui._prompts import Prompter
from orquestra.sdk.exceptions import ExpiredTokenError, InvalidTokenError
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName


@pytest.mark.parametrize("runtime_name", [RuntimeName.CE_REMOTE, RuntimeName.QE_REMOTE])
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
    def test_passed_server_no_token(async_sleep, runtime_name):
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
        config = None

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)

        config_repo.store_token_in_config.return_value = config_name
        runtime_repo.get_login_url.return_value = login_url
        type(login_server).token = PropertyMock(return_value=retrieved_token)

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
        )

        # When
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # Then
        async_sleep.assert_called_once()
        config_resolver.resolve.assert_not_called()
        exception_presenter.show_error.assert_not_called()
        login_presenter.open_url_in_browser.assert_called_once_with(login_url)
        login_presenter.print_login_help.assert_called_once()
        config_repo.store_token_in_config.assert_called_once_with(
            url, retrieved_token, runtime_name
        )
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)

    @staticmethod
    def test_passed_server_no_token_server_exits_gracefully(
        monkeypatch: pytest.MonkeyPatch, runtime_name
    ):
        # Given
        # CLI inputs
        url = "my_url"
        login_url = "config_url"
        config_name = "cfg"
        token = None
        retrieved_token = "mocked_token"
        config = None

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)

        config_repo.store_token_in_config.return_value = config_name
        runtime_repo.get_login_url.return_value = login_url
        type(login_server).token = PropertyMock(return_value=retrieved_token)

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
        )
        monkeypatch.setattr(
            action, "_get_token_from_server", Mock(side_effect=web.GracefulExit)
        )

        # When
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # Then
        config_resolver.resolve_stored_config_for_login.assert_not_called()
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(
            url, retrieved_token, runtime_name
        )
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)

    @staticmethod
    def test_passed_server_no_token_auto_login_failed(async_sleep, runtime_name):
        # Given
        # CLI inputs
        url = "my_url"
        login_url = "config_url"
        config_name = "cfg"
        token = None
        retrieved_token = None
        config = None

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)

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
            config_resolver=config_resolver,
        )

        # When
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # Then
        async_sleep.assert_not_called()
        config_resolver.resolve_stored_config_for_login.assert_not_called()
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_not_called()
        login_presenter.prompt_config_saved.assert_not_called()
        login_presenter.prompt_for_login.assert_called_once_with(
            login_url, url, runtime_name
        )

    @staticmethod
    def test_passed_server_and_token(async_sleep, runtime_name):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        url = "my_url"
        token = "my_token"
        config = None

        # Mocks
        config_name = "cfg"
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)

        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
        )

        # When
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # Then
        # We should get the login url from QE
        async_sleep.assert_not_called()
        config_resolver.resolve_stored_config_for_login.assert_not_called()
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(
            url, token, runtime_name
        )
        login_presenter.prompt_config_saved.assert_called_once_with(url, config_name)

    @staticmethod
    def test_passed_config_no_token(async_sleep, runtime_name):
        # GIVEN
        # CLI inputs
        url = None
        token = None
        config = "<cli config sentinel>"

        login_url = "config_url"
        config_name = "cfg"
        retrieved_token = "mocked_token"

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)
        loaded_runtime_config = create_autospec(RuntimeConfiguration)

        loaded_runtime_config.runtime_options = {"uri": login_url}
        loaded_runtime_config.runtime_name = runtime_name
        config_resolver.resolve_stored_config_for_login.return_value = config_name
        config_repo.read_config.return_value = loaded_runtime_config
        config_repo.store_token_in_config.return_value = config_name
        type(login_server).token = PropertyMock(return_value=retrieved_token)

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
        )

        # WHEN
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # THEN
        async_sleep.assert_called_once()
        config_resolver.resolve_stored_config_for_login.assert_called_once_with(config)
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(
            login_url, retrieved_token, runtime_name
        )
        login_presenter.prompt_config_saved.assert_called_once_with(
            login_url, config_name
        )

    @staticmethod
    def test_passed_config_and_token(async_sleep, runtime_name):
        # GIVEN
        # CLI inputs
        url = None
        token = "<cli token sentinel>"
        config = "<cli config sentinel>"

        login_url = "config_url"
        config_name = "cfg"

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)
        loaded_runtime_config = create_autospec(RuntimeConfiguration)

        loaded_runtime_config.runtime_options = {"uri": login_url}
        loaded_runtime_config.runtime_name = runtime_name
        config_resolver.resolve_stored_config_for_login.return_value = config_name
        config_repo.read_config.return_value = loaded_runtime_config
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
        )

        # WHEN
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # THEN
        async_sleep.assert_not_called()
        config_resolver.resolve_stored_config_for_login.assert_called_once_with(config)
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once_with(
            login_url, token, runtime_name
        )
        login_presenter.prompt_config_saved.assert_called_once_with(
            login_url, config_name
        )

    @staticmethod
    def test_passed_config_and_server_exits_gracefully(async_sleep, runtime_name):
        # GIVEN
        # CLI inputs
        url = "<cli url sentinel>"
        token = "<cli token sentinel>"
        config = "<cli config sentinel>"

        login_url = "config_url"
        config_name = "cfg"

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)
        loaded_runtime_config = create_autospec(RuntimeConfiguration)

        loaded_runtime_config.runtime_options = {"uri": login_url}
        loaded_runtime_config.runtime_name = runtime_name
        config_resolver.resolve_stored_config_for_login.return_value = config_name
        config_repo.read_config.return_value = loaded_runtime_config
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
        )

        # WHEN
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # THEN
        async_sleep.assert_not_called()
        config_resolver.resolve_stored_config_for_login.assert_not_called()
        exception_presenter.show_error.assert_called_once()
        config_repo.store_token_in_config.assert_not_called()
        login_presenter.prompt_config_saved.assert_not_called()

    @staticmethod
    def test_mismatch_between_cli_and_stored_runtime_config(async_sleep, runtime_name):
        # GIVEN
        # CLI inputs
        url = None
        token = None
        config = "<cli config sentinel>"

        login_url = "config_url"
        config_name = "cfg"

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)
        prompter = create_autospec(Prompter)

        prompter.confirm.return_value = True

        loaded_runtime_config = create_autospec(RuntimeConfiguration)
        loaded_runtime_config.runtime_options = {"uri": login_url}
        loaded_runtime_config.runtime_name = (
            RuntimeName.CE_REMOTE
            if runtime_name == RuntimeName.QE_REMOTE
            else RuntimeName.QE_REMOTE
        )
        loaded_runtime_config.config_name = config_name
        config_resolver.resolve_stored_config_for_login.return_value = config_name
        config_repo.read_config.return_value = loaded_runtime_config
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
            prompter=prompter,
        )

        # WHEN
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # THEN
        # called
        prompter.confirm.assert_called_once_with(
            f"Config '{config_name}' will be changed from "
            f"{loaded_runtime_config.runtime_name} to "
            f"{runtime_name}. Continue?",
            True,
        )
        async_sleep.assert_called_once()
        config_resolver.resolve_stored_config_for_login.assert_called_once_with(config)
        login_presenter.prompt_config_saved.assert_called_once_with(
            login_url, config_name
        )
        # not called
        exception_presenter.show_error.assert_not_called()
        config_repo.store_token_in_config.assert_called_once()

    @staticmethod
    def test_mismatch_between_cli_and_stored_runtime_config_user_cancels(
        async_sleep, runtime_name
    ):
        # GIVEN
        # CLI inputs
        url = None
        token = None
        config = "<cli config sentinel>"

        login_url = "config_url"
        config_name = "cfg"

        # Mocks
        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)
        prompter = create_autospec(Prompter)

        prompter.confirm.return_value = False

        loaded_runtime_config = create_autospec(RuntimeConfiguration)
        loaded_runtime_config.runtime_options = {"uri": login_url}
        loaded_runtime_config.runtime_name = (
            RuntimeName.CE_REMOTE
            if runtime_name == RuntimeName.QE_REMOTE
            else RuntimeName.QE_REMOTE
        )
        loaded_runtime_config.config_name = config_name
        config_resolver.resolve.return_value = config_name
        config_repo.read_config.return_value = loaded_runtime_config
        config_repo.store_token_in_config.return_value = config_name

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
            prompter=prompter,
        )

        # WHEN
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # THEN
        # called
        config_resolver.resolve_stored_config_for_login.assert_called_once_with(config)
        prompter.confirm.assert_called_once_with(
            f"Config '{config_name}' will be changed from "
            f"{loaded_runtime_config.runtime_name} to "
            f"{runtime_name}. Continue?",
            True,
        )
        # not called
        async_sleep.assert_not_called()
        exception_presenter.show_error.assert_called_once()
        config_repo.store_token_in_config.assert_not_called()
        login_presenter.prompt_config_saved.assert_not_called()

    @staticmethod
    @pytest.mark.parametrize(
        "exception",
        (
            InvalidTokenError(),
            ExpiredTokenError(),
        ),
    )
    def test_invalid_token(async_sleep, runtime_name, exception):
        # Given
        # CLI inputs
        url = "my_url"
        token = "my_token"
        config = None

        exception_presenter = create_autospec(WrappedCorqOutputPresenter)
        login_presenter = create_autospec(LoginPresenter)
        runtime_repo = create_autospec(RuntimeRepo)
        config_repo = create_autospec(ConfigRepo)
        login_server = create_autospec(_login_server.LoginServer)
        config_resolver = create_autospec(ConfigResolver)
        prompter = create_autospec(Prompter)

        exception = InvalidTokenError()
        config_repo.store_token_in_config.side_effect = exception

        action = _login.Action(
            exception_presenter=exception_presenter,
            login_presenter=login_presenter,
            runtime_repo=runtime_repo,
            config_repo=config_repo,
            login_server=login_server,
            config_resolver=config_resolver,
            prompter=prompter,
        )

        # When
        action.on_cmd_call(
            config=config, url=url, token=token, runtime_name=runtime_name
        )

        # Then
        async_sleep.assert_not_called()
        exception_presenter.show_error.assert_called_once_with(exception)
        config_repo.store_token_in_config.assert_called_once_with(
            url, token, runtime_name
        )
        login_presenter.prompt_config_saved.assert_not_called()
