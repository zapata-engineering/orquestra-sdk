################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for 'orq login'.
"""
import typing as t

from .. import _repos
from .._ui import _presenters


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
    ):
        # presenters
        self._exception_presenter: _presenters.WrappedCorqOutputPresenter = (
            exception_presenter
        )
        self._login_presenter: _presenters.LoginPresenter = login_presenter

        # data sources
        self._config_repo: _repos.ConfigRepo = config_repo
        self._runtime_repo: _repos.RuntimeRepo = runtime_repo

    def on_cmd_call(self, url: str, token: t.Optional[str], ce: bool):
        try:
            self._on_cmd_call_with_exceptions(url, token, ce)
        except Exception as e:
            self._exception_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        url: str,
        token: t.Optional[str],
        ce: bool,
    ):
        """
        Implementation of the command action. Doesn't catch exceptions.
        """
        if token is None:
            self._prompt_for_login(url, ce)
        else:
            self._save_token(url, token, ce)

    def _prompt_for_login(self, url: str, ce: bool):
        login_url = self._runtime_repo.get_login_url(url, ce)
        self._login_presenter.prompt_for_login(login_url, url, ce)

    def _save_token(self, url, token, ce: bool):
        config_name = self._config_repo.store_token_in_config(url, token, ce)
        self._login_presenter.prompt_config_saved(url, config_name)
