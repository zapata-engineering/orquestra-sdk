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
    def test_passed_server_no_token(capsys):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        url = "my_url"
        token = None

        # Mocks
        presenter = MagicMock()
        qe_repo = MagicMock()
        config_repo = MagicMock()

        action = _login.Action(
            presenter=presenter,
            qe_repo=qe_repo,
            config_repo=config_repo
        )

        # When
        action.on_cmd_call(url=url, token=token)

        # Then
        # We should get the login url from QE
        assert qe_repo.mock_calls.count(call.get_login_url(url))
        captured = capsys.readouterr()
        assert "Please follow this URL to proceed with login" in captured.out
        assert "Then save the token using command:" in captured.out
        assert f"orq login -s {url} -t <paste your token here>" in captured.out

    @staticmethod
    def test_passed_server_and_token(capsys):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given
        # CLI inputs
        url = "my_url"
        token = "my_token"

        presenter = MagicMock()
        qe_repo = MagicMock()
        config_repo = MagicMock()

        action = _login.Action(
            presenter=presenter,
            qe_repo=qe_repo,
            config_repo=config_repo
        )

        # When
        action.on_cmd_call(url=url, token=token)

        # Then
        # We should get the login url from QE
        assert config_repo.mock_calls.count(call.store_token_in_config(url, token))
        captured = capsys.readouterr()
        assert f"Configuration name for {url} is " in captured.out
