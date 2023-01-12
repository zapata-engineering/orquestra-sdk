################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for 'orq login'.
"""
import typing as t

from orquestra.sdk import exceptions

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
        qe_repo=_repos.QEClientRepo(),
    ):
        self._exception_presenter = exception_presenter
        self._login_presenter = login_presenter
        # data sources
        self._config_repo = config_repo
        self._qe_repo = qe_repo

    def on_cmd_call(self, url: str, token: t.Optional[str]):
        try:
            self._on_cmd_call_with_exceptions(url, token)
        except Exception as e:
            self._exception_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        url: str,
        token: t.Optional[str],
    ):
        """
        Implementation of the command action. Doesn't catch exceptions.
        """
        if token is None:
            self._prompt_for_login(url)
        else:
            self._save_token(url, token)

    def _prompt_for_login(self, url: str):
        login_url = self._qe_repo.get_login_url(url)
        self._login_presenter.prompt_for_login(login_url, url)

    def _save_token(self, url, token):
        config_name = self._config_repo.store_token_in_config(url, token)
        self._login_presenter.prompt_config_saved(url, config_name)
