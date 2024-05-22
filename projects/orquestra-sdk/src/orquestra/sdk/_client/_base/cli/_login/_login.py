################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq login'."""
import asyncio
import typing as t

from aiohttp import web
from orquestra.workflow_shared.exceptions import (
    LocalConfigLoginError,
    UserCancelledPrompt,
)
from orquestra.workflow_shared.schema.configs import RemoteRuntime

from .. import _arg_resolvers, _repos
from .._ui import _presenters, _prompts
from ._login_server import LoginServer


class Action:
    """Encapsulates app-related logic for handling ``orq login``."""

    def __init__(
        self,
        exception_presenter=_presenters.WrappedCorqOutputPresenter(),
        login_presenter=_presenters.LoginPresenter(),
        config_repo=_repos.ConfigRepo(),
        runtime_repo=_repos.RuntimeRepo(),
        login_server=LoginServer(),
        config_resolver=_arg_resolvers.ConfigResolver(),
        prompter=_prompts.Prompter(),
    ):
        # presenters
        self._exception_presenter: _presenters.WrappedCorqOutputPresenter = (
            exception_presenter
        )
        self._login_presenter: _presenters.LoginPresenter = login_presenter
        self._prompter: _prompts.Prompter = prompter

        # data sources
        self._config_repo: _repos.ConfigRepo = config_repo
        self._runtime_repo: _repos.RuntimeRepo = runtime_repo
        self._login_server: LoginServer = login_server
        self._config_resolver: _arg_resolvers.ConfigResolver = config_resolver

    def on_cmd_call(
        self,
        config: t.Optional[str],
        url: t.Optional[str],
        token: t.Optional[str],
        runtime_name: RemoteRuntime,
    ):
        """Call the login command action, catching any exceptions that arise."""
        try:
            self._on_cmd_call_with_exceptions(config, url, token, runtime_name)
        except Exception as e:
            self._exception_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        config: t.Optional[str],
        url: t.Optional[str],
        token: t.Optional[str],
        runtime_name: RemoteRuntime,
    ):
        """Implementation of the command action. Doesn't catch exceptions."""
        assert bool(config) ^ bool(url), (
            "orq login action was called with arguments "
            f"'config = {config}, url = {url}'. "
            "Exactly one of these arguments must be provided, but this constraint "
            "should have been handled at CLI entry."
        )

        _url: str = ""
        if config:
            loaded_config = self._config_repo.read_config(
                self._config_resolver.resolve_stored_config_for_login(config)
            )
            try:
                _url = loaded_config.runtime_options["uri"]
            except KeyError as e:
                raise LocalConfigLoginError(
                    f"Cannot log in with '{loaded_config.config_name}' "
                    "as this config does not include a server URL. "
                    "It is likely that this config is for local execution, "
                    "and therefore can be used without logging in."
                ) from e

            # If the CLI disagrees with the stored config about which runtime to use,
            # prompt the user to agree to changing the config to match the cli args.
            if runtime_name != loaded_config.runtime_name:
                if not self._prompter.confirm(
                    f"Config '{loaded_config.config_name}' will be changed from "
                    f"{loaded_config.runtime_name} to {runtime_name}. "
                    "Continue?",
                    True,
                ):
                    raise UserCancelledPrompt()

        _url = url or _url

        if token is None:
            self._prompt_for_login(_url, runtime_name)
        else:
            self._save_token(_url, token, runtime_name)

    def _prompt_for_login(self, url: str, runtime_name: RemoteRuntime):
        try:
            asyncio.run(self._get_token_from_server(url, runtime_name, 60))
        except (web.GracefulExit, KeyboardInterrupt):
            pass
        if self._login_server.token is None:
            # We didn't get a token, this means the collector timed out or otherwise
            # couldn't receive a token
            # In this case, print out the manual instructions
            self._login_presenter.prompt_for_login(self._login_url, url)
            return
        self._save_token(url, self._login_server.token, runtime_name)

    def _save_token(self, url, token, runtime_name: RemoteRuntime):
        config_name = self._config_repo.store_token_in_config(url, token, runtime_name)
        self._login_presenter.prompt_config_saved(url, config_name, runtime_name)

    async def _get_token_from_server(
        self, url: str, runtime_name: RemoteRuntime, timeout: int
    ):
        try:
            port = await self._login_server.start(url)
            self._login_url = self._runtime_repo.get_login_url(url, runtime_name, port)
            browser_opened = self._login_presenter.open_url_in_browser(self._login_url)
            if browser_opened:
                self._login_presenter.print_login_help()
                await asyncio.sleep(timeout)
        finally:
            await self._login_server.cleanup()
