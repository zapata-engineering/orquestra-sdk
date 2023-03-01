################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf results' glue code.
"""

from pathlib import Path
from unittest.mock import create_autospec

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

            logs = {"task_inv1": ["my_log_1", "my_log_2"], "task_inv2": ["log3"]}
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
        def test_no_download_dir(action):
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = None

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id, config=config, download_dir=download_dir
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

            # We expect printing the workflow run returned from the repo.
            logs = action._wf_run_repo.get_wf_logs.return_value
            action._presenter.show_logs.assert_called_with(logs)

            # We don't expect any dumps.
            assert action._dumper.dump.mock_calls == []

        @staticmethod
        def test_download_dir_passed(action):
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = Path("/cool/path")

            # Custom mocks
            dumped_path = "<dumped path sentinel>"
            action._dumper.dump.return_value = dumped_path

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id, config=config, download_dir=download_dir
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

            # Expect dumping logs to the FS
            logs = action._wf_run_repo.get_wf_logs.return_value
            action._dumper.dump.assert_called_with(
                logs, resolved_wf_run_id, download_dir
            )

            # Do not print logs to stdout
            action._presenter.show_logs.assert_not_called()

            # Expect info presented to the user abouyt the dump
            action._presenter.show_dumped_wf_logs.assert_called_with(dumped_path)
