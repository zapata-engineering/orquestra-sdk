################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""
Tests that validate parsing CLI groups and commands.
"""

import sys
from unittest.mock import Mock

import pytest

from orquestra.sdk._base.cli._dorq import _entry
from orquestra.sdk._base.cli._dorq._workflow import _list


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
            (["-c", "foo"], ("foo",)),
            (["--config", "foo"], ("foo",)),
            (
                ["-c", "foo", "--config", "bar"],
                (
                    "foo",
                    "bar",
                ),
            ),
            ([], ()),
        ],
    )
    class TestListOptions:
        @staticmethod
        @pytest.mark.parametrize(
            "limit, expected_limit",
            [
                (["-l", "10"], 10),
                (["--limit", "17"], 17),
                ([], None),
            ],
        )
        @pytest.mark.parametrize(
            "max_age, expected_max_age",
            [
                (["-t", "bar"], "bar"),
                (["--max-age", "bar"], "bar"),
                ([], None),
            ],
        )
        @pytest.mark.parametrize(
            "state, expected_state",
            [
                (["-s", "foobar"], ("foobar",)),
                (["--state", "foobar"], ("foobar",)),
                (["-s", "foobar", "--state", "barfoo"], ("foobar", "barfoo")),
                ([], ()),
            ],
        )
        def test_filters(
            monkeypatch,
            entrypoint,
            config,
            limit,
            max_age,
            state,
            expected_config,
            expected_limit,
            expected_max_age,
            expected_state,
        ):
            """
            The `orq workflow list` command does some type conversion and sets some
            default values. This test confirms that valid arguments are correctly
            converted and passed to the action.
            """
            # Given
            entrypoint(["workflow", "list"] + config + limit + max_age + state)

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
                    expected_interactive,
                ]
            )
            mock_exit.assert_called_with(0)
