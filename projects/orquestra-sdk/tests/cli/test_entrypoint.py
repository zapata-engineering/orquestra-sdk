################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################

"""
Tests that validate parsing CLI groups and commands.
"""

import sys
from unittest.mock import ANY, Mock

import pytest

from orquestra.sdk._client._base.cli import _entry
from orquestra.sdk._client._base.cli._login import _login
from orquestra.sdk._client._base.cli._services import _down, _up
from orquestra.sdk._client._base.cli._workflow import _graph, _list
from orquestra.sdk._shared.schema.configs import RuntimeName


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
            ["workflow", "graph"],
            ["task"],
            ["task", "results"],
            ["task", "logs"],
            ["up"],
            ["down"],
            ["status"],
            ["restart"],
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


class TestRestart:
    @pytest.fixture
    def mock_up_action(self, monkeypatch: pytest.MonkeyPatch):
        action_mock = Mock()
        monkeypatch.setattr(_up.Action, "on_cmd_call", action_mock)
        return action_mock

    @pytest.fixture
    def mock_down_action(self, monkeypatch: pytest.MonkeyPatch):
        action_mock = Mock()
        monkeypatch.setattr(_down.Action, "on_cmd_call", action_mock)
        return action_mock

    @staticmethod
    @pytest.mark.parametrize("ray_arg, ray_value", [([], None), (["--ray"], True)])
    @pytest.mark.parametrize("all_arg, all_value", [([], None), (["--all"], True)])
    def test_calls_down_up(
        entrypoint,
        mock_up_action,
        mock_down_action,
        monkeypatch,
        ray_arg,
        ray_value,
        all_arg,
        all_value,
    ):
        # Given
        mock_exit = Mock()
        monkeypatch.setattr(sys, "exit", mock_exit)
        entrypoint(["restart"] + ray_arg + all_arg)

        # When
        _entry.main()

        # Then
        mock_down_action.assert_called_once_with(
            manage_ray=ray_value, manage_all=all_value
        )
        mock_up_action.assert_called_once_with(
            manage_ray=ray_value, manage_all=all_value
        )


class TestGraph:
    class TestGraphOptions:

        @staticmethod
        @pytest.mark.parametrize(
            "config, expected_config",
            [
                (["-c", "foo"], "foo"),
                (["--config", "foo"], "foo"),
                ([], None),
            ],
        )
        @pytest.mark.parametrize(
            "wf_run_id, expected_wf_run_id",
            [
                (["--id", "baz"], "baz"),
                ([], None),
            ],
        )
        def test_submitted_workflow(
            entrypoint,
            monkeypatch,
            config: list[str],
            expected_config: str,
            wf_run_id: list[str],
            expected_wf_run_id: str,
        ):
            # GIVEN
            entrypoint(["workflow", "graph"] + config + wf_run_id)

            monkeypatch.setattr(sys, "exit", mock_exit := Mock())
            monkeypatch.setattr(
                _graph.Action,
                "on_cmd_call",
                mock_action := Mock(),
            )

            # When
            _entry.main()

            # Then
            mock_action.assert_called_once_with(
                workflow=None,
                config=expected_config,
                wf_run_id=expected_wf_run_id,
                module=None,
                name=None,
            )
            mock_exit.assert_called_with(0)

        @staticmethod
        @pytest.mark.parametrize(
            "module, expected_module",
            [
                (["-m", "foo"], "foo"),
                (["--module", "foo"], "foo"),
                ([], None),
            ],
        )
        @pytest.mark.parametrize(
            "name, expected_name",
            [
                (["-n", "bar"], "bar"),
                (["--name", "bar"], "bar"),
                ([], None),
            ],
        )
        def test_local_definition(
            entrypoint,
            monkeypatch,
            module: list[str],
            expected_module: str,
            name: list[str],
            expected_name: str,
        ):
            # GIVEN
            entrypoint(["workflow", "graph"] + module + name)

            monkeypatch.setattr(sys, "exit", mock_exit := Mock())
            monkeypatch.setattr(
                _graph.Action,
                "on_cmd_call",
                mock_action := Mock(),
            )

            # When
            _entry.main()

            # Then
            mock_action.assert_called_once_with(
                workflow=None,
                config=None,
                wf_run_id=None,
                module=expected_module,
                name=expected_name,
            )
            mock_exit.assert_called_with(0)

        @staticmethod
        @pytest.mark.parametrize(
            "additional_options, expected_options",
            [
                (
                    [],
                    {
                        "config": None,
                        "wf_run_id": None,
                        "module": None,
                        "name": None,
                    },
                ),
                (
                    ["-c", "foo"],
                    {
                        "config": "foo",
                        "wf_run_id": None,
                        "module": None,
                        "name": None,
                    },
                ),
                (
                    ["-n", "foo"],
                    {
                        "config": None,
                        "wf_run_id": None,
                        "module": None,
                        "name": "foo",
                    },
                ),
            ],
        )
        def test_indeterminate(
            entrypoint,
            monkeypatch,
            additional_options: list[str],
            expected_options: dict,
        ):
            # GIVEN
            entrypoint(
                ["workflow", "graph", "<WORKFLOW SENTINEL>"] + additional_options
            )

            monkeypatch.setattr(sys, "exit", mock_exit := Mock())
            monkeypatch.setattr(
                _graph.Action,
                "on_cmd_call",
                mock_action := Mock(),
            )

            # When
            _entry.main()

            # Then
            mock_action.assert_called_once_with(
                workflow="<WORKFLOW SENTINEL>",
                **expected_options,
            )
            mock_exit.assert_called_with(0)

        @staticmethod
        @pytest.mark.parametrize(
            "arguments",
            [
                ["foo", "-m", "bar"],
                ["foo", "--id", "bar"],
                ["-m", "foo", "-c", "bar"],
                ["-m", "foo", "--id", "bar"],
                ["-n", "foo", "-c", "bar"],
                ["-n", "foo", "--id", "bar"],
            ],
            ids=(
                "Indeterminate workflow arg plus module option",
                "Indeterminate workflow arg plut id option",
                "module (local definition) and config (previously submitted)",
                "module (local definition) and workflow id (previously submitted)",
                "name (local definition) and config (previously submitted)",
                "name (local definition) and workflow id (previously submitted)",
            ),
        )
        def test_clashing_options(
            entrypoint, monkeypatch, arguments: list[str], capsys: pytest.CaptureFixture
        ):
            """
            There are two use cases for `orq wf graph` - a local definition and a
            previously submitted workflow. Here we test that combinations of arguments
            that mix between these cases are rejected.
            """
            # GIVEN
            entrypoint(["workflow", "graph"] + arguments)

            monkeypatch.setattr(sys, "exit", mock_exit := Mock())
            monkeypatch.setattr(
                _graph.Action,
                "on_cmd_call",
                mock_action := Mock(),
            )

            # When
            _entry.main()

            # Then
            mock_action.assert_not_called()
            mock_exit.assert_called_with(2)
            assert (
                "the following parameters are mutually exclusive"
                in capsys.readouterr().err
            )
