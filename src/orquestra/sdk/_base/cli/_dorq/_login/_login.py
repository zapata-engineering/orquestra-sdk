################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for 'orq login'.
"""
import asyncio
import typing as t

from aiohttp import web

from orquestra.sdk.exceptions import RuntimeConfigError
from orquestra.sdk.schema.configs import RuntimeName

from .. import _arg_resolvers, _repos
from .._ui import _presenters, _prompts
from ._login_server import LoginServer


class Action:
    """
    Encapsulates app-related logic for handling `orq login`.
    """

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
        ce: bool,
    ):
        """
        Call the login command action, catching any exceptions that arise.
        """
        try:
            self._on_cmd_call_with_exceptions(config, url, token, ce)
        except Exception as e:
            self._exception_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        config: t.Optional[str],
        url: t.Optional[str],
        token: t.Optional[str],
        ce: bool,
    ):
        """
        Implementation of the command action. Doesn't catch exceptions.
        """
        assert bool(config) ^ bool(url), (
            "orq login action was called with arguments "
            f"'config = {config}, url = {url}'. "
            "Exactly one of these arguments must be provided, but this constraint "
            "should have been handled at CLI entry. Please report this as a bug."
        )

        if config:
            loaded_config = self._config_repo.read_config(
                self._config_resolver.resolve(config)
            )

            if ce and loaded_config.runtime_name != RuntimeName.CE_REMOTE:
                # CE has been set for a previously non-ce config. Rather than
                # overwrite, we tell the user about the mismatch, and give them the
                # explicit command to overwrite the old config if they want to.
                raise RuntimeConfigError(
                    f"Cannot log into config '{config}' to use a CE runtime as the "
                    f"stored configuration is for a '{loaded_config.runtime_name}' "
                    "runtime.\nTo update this config to use CE, please use the "
                    "following command:\n"
                    f"orq login -s {loaded_config.runtime_options['uri']} --ce"
                )

            ce = loaded_config.runtime_name == RuntimeName.CE_REMOTE
            url = loaded_config.runtime_options["uri"]

        if token is None:
            self._prompt_for_login(url, ce)
        else:
            self._save_token(url, token, ce)

    def _prompt_for_login(self, url: str, ce: bool):
        try:
            asyncio.run(self._get_token_from_server(url, ce, 60))
        except (web.GracefulExit, KeyboardInterrupt):
            pass
        if self._login_server.token is None:
            # We didn't get a token, this means the collector timed out or otherwise
            # couldn't receive a token
            # In this case, print out the manual instructions
            self._login_presenter.prompt_for_login(self._login_url, url, ce)
            return
        self._save_token(url, self._login_server.token, ce)

    def _save_token(self, url, token, ce: bool):
        config_name = self._config_repo.store_token_in_config(url, token, ce)
        self._login_presenter.prompt_config_saved(url, config_name)

    async def _get_token_from_server(self, url, ce, timeout):
        try:
            port = await self._login_server.start(url)
            self._login_url = self._runtime_repo.get_login_url(url, ce, port)
            browser_opened = self._login_presenter.open_url_in_browser(self._login_url)
            if browser_opened:
                self._login_presenter.print_login_help()
                await asyncio.sleep(timeout)
        finally:
            await self._login_server.cleanup()
