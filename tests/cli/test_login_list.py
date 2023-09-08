################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from unittest.mock import Mock, create_autospec

from orquestra.sdk._base.cli._config._list import Action
from orquestra.sdk.schema.configs import RuntimeConfiguration


class TestAction:
    @staticmethod
    def test_data_passing(monkeypatch):
        # Given
        exception_presenter = Mock()
        config_presenter = Mock()
        config_repo = Mock()
        prompter = Mock()
        monkeypatch.setattr(
            "orquestra.sdk._base._jwt.check_jwt_without_signature_verification",
            Mock(return_value=True),
        )

        config_repo.list_remote_config_names = Mock(
            return_value=["<config name sentinel 1>", "<config name sentinel 2>"]
        )
        configs = [
            create_autospec(RuntimeConfiguration),
            create_autospec(RuntimeConfiguration),
        ]
        configs[0].runtime_options = {"token": "<token sentinel 1>"}
        configs[0].config_name = "<config name sentinel 3>"
        configs[1].runtime_options = {"token": "<token sentinel 2>"}
        configs[1].config_name = "<config name sentinel 4>"
        config_repo.read_config = Mock(side_effect=configs)

        action = Action(
            exception_presenter=exception_presenter,
            config_presenter=config_presenter,
            config_repo=config_repo,
            prompter=prompter,
        )

        # When
        action._on_cmd_call_with_exceptions()

        # Then
        exception_presenter.assert_not_called()
        config_presenter.print_configs_list.assert_called_once_with(
            configs,
            {"<config name sentinel 3>": False, "<config name sentinel 4>": False},
            message="Stored Logins:",
        )
        config_repo.list_remote_config_names.assert_called_once_with()
        assert [call.args for call in config_repo.read_config.call_args_list] == [
            ("<config name sentinel 1>",),
            ("<config name sentinel 2>",),
        ]
        prompter.assert_not_called()

    def test_no_remote_configs(monkeypatch, capsys):
        # Given
        exception_presenter = Mock()
        config_repo = Mock()
        prompter = Mock()

        config_repo.list_remote_config_names = Mock(return_value=[])

        action = Action(
            exception_presenter=exception_presenter,
            config_repo=config_repo,
            prompter=prompter,
        )

        # When
        action._on_cmd_call_with_exceptions()

        # Then
        assert "No remote configs available" in capsys.readouterr().out
        exception_presenter.assert_not_called()
        config_repo.list_remote_config_names.assert_called_once_with()
        prompter.assert_not_called()
