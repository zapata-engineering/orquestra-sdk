################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq task logs' glue code.
"""

from pathlib import Path
from unittest.mock import create_autospec

import pytest

from orquestra.sdk._base.cli._dorq._arg_resolvers import (
    TaskInvIDResolver,
    WFConfigResolver,
    WFRunResolver,
)
from orquestra.sdk._base.cli._dorq._dumpers import LogsDumper
from orquestra.sdk._base.cli._dorq._repos import WorkflowRunRepo
from orquestra.sdk._base.cli._dorq._task import _logs
from orquestra.sdk._base.cli._dorq._ui._presenters import WrappedCorqOutputPresenter


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
            resolved_invocation_id = "<resolved invocation id>"

            # Mocks
            presenter = create_autospec(WrappedCorqOutputPresenter)

            dumper = create_autospec(LogsDumper)
            dumped_path = "<dumped path sentinel>"
            dumper.dump.return_value = dumped_path

            wf_run_repo = create_autospec(WorkflowRunRepo)

            logs = {"task_inv": ["my_log_1", "my_log_2"]}
            wf_run_repo.get_task_logs.return_value = logs

            config_resolver = create_autospec(WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_resolver = create_autospec(WFRunResolver)
            wf_run_resolver.resolve_id.return_value = resolved_id

            task_inv_id_resolver = create_autospec(TaskInvIDResolver)
            task_inv_id_resolver.resolve.return_value = resolved_invocation_id

            return _logs.Action(
                presenter=presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
                task_inv_id_resolver=task_inv_id_resolver,
            )

        @staticmethod
        def test_no_download_dir(action):
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = None
            task_inv_id = "<my inv ID>"
            fn_name = "<my task fn name>"

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id,
                config=config,
                download_dir=download_dir,
                fn_name=fn_name,
                task_inv_id=task_inv_id,
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

            resolved_wf_run_id = action._wf_run_resolver.resolve_id.return_value
            action._task_inv_id_resolver.resolve.assert_called_with(
                task_inv_id, fn_name, resolved_wf_run_id, resolved_config
            )

            # We should pass resolved values to run repo.
            resolved_inv_id = action._task_inv_id_resolver.resolve.return_value
            action._wf_run_repo.get_task_logs.assert_called_with(
                wf_run_id=resolved_wf_run_id,
                config_name=resolved_config,
                task_inv_id=resolved_inv_id,
            )

            # We expect printing the workflow run returned from the repo.
            logs = action._wf_run_repo.get_task_logs.return_value
            action._presenter.show_logs.assert_called_with(logs)

            # We don't expect any dumps.
            assert action._dumper.dump.mock_calls == []

        @staticmethod
        def test_download_dir_passed(action):
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = Path("/tmp/my/awesome/dir")
            task_inv_id = "<my inv ID>"
            fn_name = "<my task fn name>"

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id,
                config=config,
                download_dir=download_dir,
                fn_name=fn_name,
                task_inv_id=task_inv_id,
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

            resolved_wf_run_id = action._wf_run_resolver.resolve_id.return_value
            action._task_inv_id_resolver.resolve.assert_called_with(
                task_inv_id, fn_name, resolved_wf_run_id, resolved_config
            )

            # We should pass resolved values to run repo.
            resolved_inv_id = action._task_inv_id_resolver.resolve.return_value
            action._wf_run_repo.get_task_logs.assert_called_with(
                wf_run_id=resolved_wf_run_id,
                config_name=resolved_config,
                task_inv_id=resolved_inv_id,
            )

            # Do not print logs to stdout
            action._presenter.show_logs.assert_not_called()

            # Expect dumping logs to the FS.
            logs = action._wf_run_repo.get_task_logs.return_value
            action._dumper.dump.assert_called_with(
                logs, resolved_wf_run_id, download_dir
            )

            dumped_path = action._dumper.dump.return_value
            action._presenter.show_dumped_wf_logs.assert_called_with(dumped_path)
