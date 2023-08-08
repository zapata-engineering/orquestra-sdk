################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""
Tests that validate parsing CLI groups and commands.
"""

import sys
from unittest.mock import ANY, Mock, create_autospec

import pytest

from orquestra.sdk._base.cli import _entry
from orquestra.sdk._base.cli._login import _login
from orquestra.sdk._base.cli._workflow import _list
from orquestra.sdk.schema.configs import RuntimeName


@pytest.fixture()
def entrypoint(monkeypatch):
    def _entrypoint(command: list):
        argv = [
            # Before we switch the setuptools entrypoint, the way to use dorq is
            # 'python -m orquestra.sdk._base.cli._dorq._main'. In this case, Python
            # sets first argv to the module path.
            _entry.__file__,
            # The group and command.
            *command,
        ]
        monkeypatch.setattr(sys, "argv", argv)

    return _entrypoint


class TestCommandTreeAssembly:
    """
    Validates that dorq command tree was assembled correctly and each command is
    accessible.

    Test boundary: [argv] -> [_entry.main()] -> [sys.exit()]
                                             -> [stdout]
    """

    @staticmethod
    @pytest.mark.parametrize(
        "cmd",
        [
            [],
            ["workflow"],
            ["workflow", "view"],
            ["workflow", "submit"],
            ["workflow", "stop"],
            ["workflow", "logs"],
            ["workflow", "results"],
            ["task"],
            ["task", "results"],
            ["task", "logs"],
            ["up"],
            ["down"],
            ["status"],
            ["login"],
        ],
    )
    @pytest.mark.parametrize(
        "help_flag",
        [
            "-h",
            "--help",
        ],
    )
    def test_printing_help(monkeypatch, capsys, cmd, help_flag, entrypoint):
        """
        Prints help to validate that a given command is achievable.
        """
        # Given
        entrypoint(cmd + [help_flag])

        mock_exit = Mock()
        monkeypatch.setattr(sys, "exit", mock_exit)

        # When
        _entry.main()

        # Then
        captured = capsys.readouterr()
        # We assume that a valid help string includes the command itself. This is a
        # heuristic for validating printed help.
        assert " ".join(cmd) in captured.out

        # If the command isn't achievable, both argparse and click return status code 2.
        mock_exit.assert_called_with(0)


class TestList:
    @pytest.mark.parametrize(
        "config, expected_config",
        [
            (["-c", "foo"], "foo"),
            (["--config", "foo"], "foo"),
            ([], None),
        ],
    )
    class TestListOptions:
        @staticmethod
        @pytest.mark.parametrize(
            "limit, expected_limit, max_age, expected_max_age, state, "
            "expected_state, workspace, expected_workspace",
            [
                (
                    ["-l", "10"],
                    10,
                    ["-t", "bar"],
                    "bar",
                    ["-s", "foobar"],
                    ("foobar",),
                    ["-w", "ws"],
                    "ws",
                ),
                (
                    ["--limit", "17"],
                    17,
                    ["--max-age", "bar"],
                    "bar",
                    ["--state", "foobar"],
                    ("foobar",),
                    ["--workspace-id", "3"],
                    "3",
                ),
                (
                    [],
                    None,
                    [],
                    None,
                    [],
                    (),
                    [],
                    None,
                ),
            ],
        )
        def test_filters(
            monkeypatch,
            entrypoint,
            config,
            limit,
            max_age,
            state,
            workspace,
            expected_config,
            expected_limit,
            expected_max_age,
            expected_state,
            expected_workspace,
        ):
            """
            The `orq workflow list` command does some type conversion and sets some
            default values. This test confirms that valid arguments are correctly
            converted and passed to the action.
            """
            # Given
            entrypoint(
                ["workflow", "list"] + config + limit + max_age + state + workspace
            )

            mock_exit = Mock()
            monkeypatch.setattr(sys, "exit", mock_exit)
            mock_action = Mock()
            monkeypatch.setattr(
                _list.Action,
                "on_cmd_call",
                mock_action,
            )

            # When
            _entry.main()

            # Then
            mock_action.assert_called_once_with(
                *[
                    expected_config,
                    expected_limit,
                    expected_max_age,
                    expected_state,
                    expected_workspace,
                    False,
                ]
            )
            mock_exit.assert_called_with(0)

        @staticmethod
        @pytest.mark.parametrize(
            "interactive, expected_interactive",
            [
                (["-i"], True),
                (["--interactive"], True),
                ([], False),
            ],
        )
        def test_interactive_flag(
            entrypoint,
            monkeypatch,
            config,
            interactive,
            expected_config,
            expected_interactive,
        ):
            # Given
            entrypoint(["workflow", "list"] + config + interactive)

            mock_exit = Mock()
            monkeypatch.setattr(sys, "exit", mock_exit)
            mock_action = Mock()
            monkeypatch.setattr(
                _list.Action,
                "on_cmd_call",
                mock_action,
            )

            # When
            _entry.main()

            # Then
            mock_action.assert_called_once_with(
                *[
                    expected_config,
                    None,  # limit
                    None,  # max_age
                    (),  # state
                    None,  # workspace
                    expected_interactive,
                ]
            )
            mock_exit.assert_called_with(0)


class TestLogin:
    @pytest.fixture
    def mock_login_action(self, monkeypatch: pytest.MonkeyPatch):
        mock_exit = Mock()
        monkeypatch.setattr(sys, "exit", mock_exit)
        action_mock = Mock()
        monkeypatch.setattr(_login.Action, "on_cmd_call", action_mock)
        return action_mock

    @pytest.mark.parametrize(
        "flag, expected_runtime",
        (
            # Default
            ([], RuntimeName.CE_REMOTE),
        ),
    )
    def test_with_flag(self, entrypoint, mock_login_action, flag, expected_runtime):
        # Given
        entrypoint(["login", "-c", "test"] + flag)

        # When
        _entry.main()

        # Then
        mock_login_action.assert_called_with(
            config=ANY, url=ANY, token=ANY, runtime_name=expected_runtime
        )


class TestVersion:
    @staticmethod
    def test_version_flag_shown_in_help(capsys, entrypoint, monkeypatch):
        """
        Prints help to validate that a given command is achievable.
        """
        # Given
        entrypoint(["-h"])
        mock_exit = Mock()
        monkeypatch.setattr(sys, "exit", mock_exit)

        # When
        _entry.main()

        # Then
        captured = capsys.readouterr()
        # We assume that a valid help string includes the command itself. This is a
        # heuristic for validating printed help.
        assert "-V, --version  Show the version and exit." in captured.out

        # If the command isn't achievable, both argparse and click return status code 2.
        mock_exit.assert_called_with(0)

    @staticmethod
    def test_shows_version(capsys, entrypoint, monkeypatch: pytest.MonkeyPatch):
        entrypoint(["--version"])
        mock_exit = Mock()
        monkeypatch.setattr(sys, "exit", mock_exit)

        _entry.main()

        captured = capsys.readouterr()
        assert captured.out.startswith("Orquestra Workflow SDK, version ")
