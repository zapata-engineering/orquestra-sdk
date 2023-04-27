################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Tests for CLI prompters.
"""

from unittest.mock import Mock

import pytest

from orquestra.sdk._base.cli._dorq._ui._prompts import Prompter
from orquestra.sdk.exceptions import UserCancelledPrompt

prompter = Prompter()


class TestChoice:
    class TestWithSingleOption:
        @staticmethod
        def test_user_accepts_selection(monkeypatch):
            mock_confirm = Mock()
            mock_confirm.return_value = True
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)

            chosen = prompter.choice(["A"], "<message sentinel>")

            assert chosen == "A"
            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with A?",
                default=True,
            )

        @staticmethod
        def test_raises_exception_when_user_rejects_selection(monkeypatch):
            mock_confirm = Mock()
            mock_confirm.return_value = False
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)

            with pytest.raises(UserCancelledPrompt):
                prompter.choice(["A"], "<message sentinel>")

            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with A?",
                default=True,
            )
