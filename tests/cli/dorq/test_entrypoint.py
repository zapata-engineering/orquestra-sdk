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
            ["up"],
            ["down"],
            ["status"],
        ],
    )
    @pytest.mark.parametrize(
        "help_flag",
        [
            "-h",
            "--help",
        ],
    )
    def test_printing_help(monkeypatch, capsys, cmd, help_flag):
        """
        Prints help to validate that a given command is achievable.
        """
        # Given
        argv = [
            # Before we switch the setuptools entrypoint, the way to use dorq is
            # 'python -m orquestra.sdk._base.cli._dorq._main'. In this case, Python
            # sets first argv to the module path.
            _entry.__file__,
            # The group and command.
            *cmd,
            # The trailing flag has two forms.
            help_flag,
        ]
        monkeypatch.setattr(sys, "argv", argv)

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
