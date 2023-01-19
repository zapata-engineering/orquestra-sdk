################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq task logs' glue code.
"""

from pathlib import Path
from unittest.mock import create_autospec

from orquestra.sdk._base.cli._dorq._arg_resolvers import (
    WFConfigResolver,
    WFRunIDResolver,
    TaskInvIDResolver,
)
from orquestra.sdk._base.cli._dorq._dumpers import LogsDumper
from orquestra.sdk._base.cli._dorq._repos import WorkflowRunRepo
from orquestra.sdk._base.cli._dorq._ui._presenters import WrappedCorqOutputPresenter
from orquestra.sdk._base.cli._dorq._task import _logs


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
        def test_no_download_dir():
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = None
            task_inv_id = "<my inv ID>"
            fn_name = "<my task fn name>"

            # Resolved values
            resolved_id = "<resolved ID>"
            resolved_config = "<resolved config>"
            resolved_invocation_id = "<resolved invocation id>"

            # Mocks
            presenter = create_autospec(WrappedCorqOutputPresenter)
            dumper = create_autospec(LogsDumper)
            wf_run_repo = create_autospec(WorkflowRunRepo)

            fake_logs = {"task_inv": ["my_log_1", "my_log_2"]}
            wf_run_repo.get_task_logs.return_value = fake_logs

            config_resolver = create_autospec(WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_id_resolver = create_autospec(WFRunIDResolver)
            wf_run_id_resolver.resolve.return_value = resolved_id

            task_inv_id_resolver = create_autospec(TaskInvIDResolver)
            task_inv_id_resolver.resolve.return_value = resolved_invocation_id

            action = _logs.Action(
                presenter=presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_id_resolver=wf_run_id_resolver,
                task_inv_id_resolver=task_inv_id_resolver,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id, config=config, download_dir=download_dir, fn_name=fn_name, task_invocation_id=task_inv_id
            )

            # Then
            # We should pass input CLI args to config resolver.
            presenter.show_error.assert_not_called()

            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_id_resolver.resolve.assert_called_with(wf_run_id, resolved_config)

            task_inv_id_resolver.resolve.assert_called_with(task_inv_id, fn_name, resolved_id, resolved_config)

            # We should pass resolved values to run repo.
            wf_run_repo.get_task_logs.assert_called_with(
                wf_run_id=resolved_id, config_name=resolved_config, task_inv_id=resolved_invocation_id,
            )

            # We expect printing the workflow run returned from the repo.
            presenter.show_logs.assert_called_with(fake_logs)

            # We don't expect any dumps.
            assert dumper.mock_calls == []

        @staticmethod
        def test_download_dir_passed():
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = Path("/tmp/my/awesome/dir")
            task_inv_id = "<my inv ID>"
            fn_name = "<my task fn name>"

            # Resolved values
            resolved_id = "<resolved ID>"
            resolved_config = "<resolved config>"
            resolved_invocation_id = "<resolved invocation id>"

            # Mocks
            presenter = create_autospec(WrappedCorqOutputPresenter)

            path_to_logs = "returns whatever"
            dumper = create_autospec(LogsDumper)
            dumper.dump.return_value = path_to_logs

            wf_run_repo = create_autospec(WorkflowRunRepo)

            fake_logs = {"task_inv": ["my_log_1", "my_log_2"]}
            wf_run_repo.get_task_logs.return_value = fake_logs

            config_resolver = create_autospec(WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_id_resolver = create_autospec(WFRunIDResolver)
            wf_run_id_resolver.resolve.return_value = resolved_id

            task_inv_id_resolver = create_autospec(TaskInvIDResolver)
            task_inv_id_resolver.resolve.return_value = resolved_invocation_id

            action = _logs.Action(
                presenter=presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_id_resolver=wf_run_id_resolver,
                task_inv_id_resolver=task_inv_id_resolver,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id, config=config, download_dir=download_dir, fn_name=fn_name, task_invocation_id=task_inv_id
            )

            # Then
            # We should pass input CLI args to config resolver.
            presenter.show_error.assert_not_called()
            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_id_resolver.resolve.assert_called_with(wf_run_id, resolved_config)

            task_inv_id_resolver.resolve.assert_called_with(task_inv_id, fn_name, resolved_id, resolved_config)

            # We should pass resolved values to run repo.
            wf_run_repo.get_task_logs.assert_called_with(
                wf_run_id=resolved_id, config_name=resolved_config,
                task_inv_id=resolved_invocation_id,
            )

            # Do not print logs to stdout
            presenter.show_logs.assert_not_called()
            presenter.show_dumped_wf_logs.assert_called_with(path_to_logs)
            dumper.dump.assert_called_with(fake_logs, resolved_id, download_dir)
