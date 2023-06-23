################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf results' glue code.
"""

from pathlib import Path
from unittest.mock import Mock, call, create_autospec

import pytest

from orquestra.sdk._base.cli._dorq._arg_resolvers import WFConfigResolver, WFRunResolver
from orquestra.sdk._base.cli._dorq._dumpers import LogsDumper
from orquestra.sdk._base.cli._dorq._repos import WorkflowRunRepo
from orquestra.sdk._base.cli._dorq._ui._presenters import WrappedCorqOutputPresenter
from orquestra.sdk._base.cli._dorq._workflow import _logs


class TestAction:
    """
    Test boundaries::
        [_output.Action]->[arg resolvers]
                        ->[repos]
                        ->[dumper]
                        ->[presenter]
    """

    class TestDataPassing:
        """
        Verifies how we pass variables between subcomponents.
        """

        @staticmethod
        @pytest.fixture
        def action():
            # Resolved values
            resolved_id = "<resolved ID>"
            resolved_config = "<resolved config>"

            # Mocks
            presenter = create_autospec(WrappedCorqOutputPresenter)
            dumper = create_autospec(LogsDumper)
            wf_run_repo = create_autospec(WorkflowRunRepo)

            logs = Mock()
            logs.per_task = {
                "task_inv1": ["my_log_1", "my_log_2"],
                "task_inv2": ["log3"],
            }
            logs.system = ["sys_log_1", "sys_log_2"]
            logs.env_setup = ["env_log_1", "env_log_2"]
            logs.other = []
            wf_run_repo.get_wf_logs.return_value = logs

            config_resolver = create_autospec(WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_resolver = create_autospec(WFRunResolver)
            wf_run_resolver.resolve_id.return_value = resolved_id

            action = _logs.Action(
                presenter=presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
            )

            return action

        @staticmethod
        @pytest.mark.parametrize("task_switch", [True, False])
        @pytest.mark.parametrize("system_switch", [True, False])
        @pytest.mark.parametrize("env_setup_switch", [True, False])
        def test_no_download_dir(action, task_switch, system_switch, env_setup_switch):
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = None
            action._wf_run_resolver.resolve_log_switches.return_value = (
                task_switch,
                system_switch,
                env_setup_switch,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id,
                config=config,
                download_dir=download_dir,
                task="<task sentinel>",
                system="<system sentinel>",
                env_setup="<env sentinel>",
            )

            # Then
            # We should pass input CLI args to config resolver.
            action._presenter.show_error.assert_not_called()
            action._config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            resolved_config = action._config_resolver.resolve.return_value
            action._wf_run_resolver.resolve_id.assert_called_with(
                wf_run_id, resolved_config
            )

            # We should pass resolved values to run repo.
            resolved_wf_run_id = action._wf_run_resolver.resolve_id.return_value
            action._wf_run_repo.get_wf_logs.assert_called_with(
                wf_run_id=resolved_wf_run_id,
                config_name=resolved_config,
            )

            # The log switches should be passed to the wf logs switches resolver
            action._wf_run_resolver.resolve_log_switches.assert_called_once_with(
                "<task sentinel>", "<system sentinel>", "<env sentinel>"
            )

            # We expect printing the workflow run returned from the repo.
            if task_switch:
                task_logs = action._wf_run_repo.get_wf_logs.return_value.per_task
                action._presenter.show_logs.assert_any_call(
                    task_logs, log_type="per task"
                )
            if system_switch:
                sys_logs = action._wf_run_repo.get_wf_logs.return_value.system
                action._presenter.show_logs.assert_any_call(sys_logs, log_type="system")
            if env_setup_switch:
                env_setup_logs = action._wf_run_repo.get_wf_logs.return_value.env_setup
                action._presenter.show_logs.assert_any_call(
                    env_setup_logs, log_type="env setup"
                )
            assert action._presenter.show_logs.call_count == sum(
                [task_switch, system_switch, env_setup_switch]
            )

            # We don't expect any dumps.
            assert action._dumper.dump.mock_calls == []

        @staticmethod
        @pytest.mark.parametrize("task_switch", [True, False])
        @pytest.mark.parametrize("system_switch", [True, False])
        @pytest.mark.parametrize("env_setup_switch", [True, False])
        def test_download_dir_passed(
            action, task_switch, system_switch, env_setup_switch
        ):
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = Path("/cool/path")
            action._wf_run_resolver.resolve_log_switches.return_value = (
                task_switch,
                system_switch,
                env_setup_switch,
            )

            # Custom mocks
            dumped_path = "<dumped path sentinel>"
            action._dumper.dump.return_value = dumped_path

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id,
                config=config,
                download_dir=download_dir,
                task="<task sentinel>",
                system="<system sentinel>",
                env_setup="<env sentinel>",
            )

            # Then
            # We should pass input CLI args to config resolver.
            action._presenter.show_error.assert_not_called()
            action._config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            resolved_config = action._config_resolver.resolve.return_value
            action._wf_run_resolver.resolve_id.assert_called_with(
                wf_run_id, resolved_config
            )

            # We should pass resolved values to run repo.
            resolved_wf_run_id = action._wf_run_resolver.resolve_id.return_value
            action._wf_run_repo.get_wf_logs.assert_called_with(
                wf_run_id=resolved_wf_run_id,
                config_name=resolved_config,
            )

            # The log switches should be passed to the wf logs switches resolver
            action._wf_run_resolver.resolve_log_switches.assert_called_once_with(
                "<task sentinel>", "<system sentinel>", "<env sentinel>"
            )

            # Expect dumping logs to the FS
            if task_switch:
                task_logs = action._wf_run_repo.get_wf_logs.return_value.per_task
                action._dumper.dump.assert_any_call(
                    task_logs, resolved_wf_run_id, download_dir, log_type="per task"
                )
                action._presenter.show_dumped_wf_logs.assert_any_call(
                    dumped_path, log_type="per task"
                )
            if system_switch:
                sys_logs = action._wf_run_repo.get_wf_logs.return_value.system
                action._dumper.dump.assert_any_call(
                    sys_logs, resolved_wf_run_id, download_dir, log_type="system"
                )
                action._presenter.show_dumped_wf_logs.assert_any_call(
                    dumped_path, log_type="system"
                )
            if env_setup_switch:
                env_setup_logs = action._wf_run_repo.get_wf_logs.return_value.env_setup
                action._dumper.dump.assert_any_call(
                    env_setup_logs,
                    resolved_wf_run_id,
                    download_dir,
                    log_type="env setup",
                )
                action._presenter.show_dumped_wf_logs.assert_any_call(
                    dumped_path, log_type="env setup"
                )
            assert action._dumper.dump.call_count == sum(
                [task_switch, system_switch, env_setup_switch]
            )
            assert action._presenter.show_dumped_wf_logs.call_count == sum(
                [task_switch, system_switch, env_setup_switch]
            )

            # Do not print logs to stdout
            action._presenter.show_logs.assert_not_called()
