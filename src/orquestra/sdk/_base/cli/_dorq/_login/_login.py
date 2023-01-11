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
    Encapsulates app-related logic for handling ``orq workflow submit``.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._submit.Action``.
    """

    def __init__(
        self,
        presenter=_presenters.WrappedCorqOutputPresenter(),
        config_repo=_repos.ConfigRepo(),
        qe_repo=_repos.QeClientRepo(),
    ):
        self._presenter = presenter

        # data sources
        self._config_repo = config_repo
        self._qe_repo = qe_repo

    def on_cmd_call(
        self, url: str, token: t.Optional[str]
    ):
        try:
            self._on_cmd_call_with_exceptions(url, token)
        except Exception as e:
            self._presenter.show_error(e)

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
        print("Please follow this URL to proceed with login:")
        print(login_url)
        print("Then save the token using command: \n"
              f"orq login -s {url} -t <paste your token here>")

    def _save_token(self, url, token):
        config_name = self._config_repo.store_token_in_config(url, token)
        print(f"Token saved in config file. "
              f"Configuration name for {url} is {config_name}")
