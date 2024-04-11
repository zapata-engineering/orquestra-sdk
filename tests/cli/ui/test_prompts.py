################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Tests for CLI prompters.
"""

from unittest.mock import Mock

import inquirer  # type: ignore # noqa
import pytest

from orquestra.sdk._client._base.cli._ui._prompts import SINGLE_INPUT, Prompter
from orquestra.sdk.shared.exceptions import NoOptionsAvailableError, UserCancelledPrompt

prompter = Prompter()


class TestChoice:
    class TestWithSingleOption:
        @staticmethod
        def test_user_accepts_selection_single_values(monkeypatch):
            mock_confirm = Mock()
            mock_list = Mock()
            mock_confirm.return_value = True
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "List", mock_list)

            chosen = prompter.choice(["A"], "<message sentinel>")

            assert chosen == "A"
            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with A?",
                default=True,
            )
            mock_list.assert_not_called()

        @staticmethod
        def test_user_accepts_selection_tuples(monkeypatch):
            mock_confirm = Mock()
            mock_list = Mock()
            mock_confirm.return_value = True
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "List", mock_list)

            chosen = prompter.choice([("name", "value")], "<message sentinel>")

            assert chosen == "value"
            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with name?",
                default=True,
            )
            mock_list.assert_not_called()

        @staticmethod
        def test_raises_exception_when_user_rejects_selection(monkeypatch):
            mock_confirm = Mock()
            mock_list = Mock()
            mock_confirm.return_value = False
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "List", mock_list)

            with pytest.raises(UserCancelledPrompt):
                prompter.choice(["A"], "<message sentinel>")

            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with A?",
                default=True,
            )

    class TestWithZeroOptions:
        @staticmethod
        def test_raises_exception_when_there_are_no_options(monkeypatch):
            mock_confirm = Mock()
            mock_list = Mock()
            mock_confirm.return_value = False
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "List", mock_list)

            with pytest.raises(NoOptionsAvailableError):
                prompter.choice([], "<message sentinel>")

            mock_confirm.assert_not_called()
            mock_list.assert_not_called()

    @staticmethod
    def test_user_accepts_selection(monkeypatch):
        mock_confirm = Mock()
        mock_prompt = Mock()
        mock_prompt.return_value = {SINGLE_INPUT: "<choice sentinel>"}
        monkeypatch.setattr(Prompter, "confirm", mock_confirm)
        monkeypatch.setattr(inquirer, "prompt", mock_prompt)

        chosen = prompter.choice(["A", "B"], "<message sentinel>")

        assert chosen == "<choice sentinel>"
        mock_confirm.assert_not_called()
        mock_prompt.assert_called_once()


class TestCheckbox:
    class TestSingleOption:
        @staticmethod
        def test_user_accepts_selection_single_values(monkeypatch):
            mock_confirm = Mock()
            mock_checkbox = Mock()
            mock_confirm.return_value = True
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "Checkbox", mock_checkbox)

            chosen = prompter.checkbox(["A"], "<message sentinel>")

            assert chosen == ["A"]
            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with A?",
                default=True,
            )
            mock_checkbox.assert_not_called()

        @staticmethod
        def test_user_accepts_selection_tuples(monkeypatch):
            mock_confirm = Mock()
            mock_checkbox = Mock()
            mock_confirm.return_value = True
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "Checkbox", mock_checkbox)

            chosen = prompter.checkbox([("name", "value")], "<message sentinel>")

            assert chosen == ["value"]
            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with name?",
                default=True,
            )
            mock_checkbox.assert_not_called()

        @staticmethod
        def test_raises_exception_when_user_rejects_selection(monkeypatch):
            mock_confirm = Mock()
            mock_checkbox = Mock()
            mock_confirm.return_value = False
            monkeypatch.setattr(Prompter, "confirm", mock_confirm)
            monkeypatch.setattr(inquirer, "Checkbox", mock_checkbox)

            with pytest.raises(UserCancelledPrompt):
                prompter.checkbox(["A"], "<message sentinel>")

            mock_confirm.assert_called_once_with(
                "<message sentinel> - only one option is available. Proceed with A?",
                default=True,
            )

    @staticmethod
    def test_raises_exception_when_there_are_no_options(monkeypatch):
        mock_confirm = Mock()
        mock_checkbox = Mock()
        mock_confirm.return_value = False
        monkeypatch.setattr(Prompter, "confirm", mock_confirm)
        monkeypatch.setattr(inquirer, "Checkbox", mock_checkbox)

        with pytest.raises(NoOptionsAvailableError):
            prompter.choice([], "<message sentinel>")

        mock_confirm.assert_not_called()
        mock_checkbox.assert_not_called()
