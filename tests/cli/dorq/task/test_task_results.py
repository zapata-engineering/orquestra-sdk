################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq task results' glue code.
"""

from pathlib import Path
from unittest.mock import create_autospec

import pytest

from orquestra.sdk._base.cli._dorq._arg_resolvers import (
    TaskInvIDResolver,
    WFConfigResolver,
    WFRunResolver,
)
from orquestra.sdk._base.cli._dorq._dumpers import TaskOutputDumper
from orquestra.sdk._base.cli._dorq._repos import WorkflowRunRepo
from orquestra.sdk._base.cli._dorq._task import _results
from orquestra.sdk._base.cli._dorq._ui._presenters import (
    ArtifactPresenter,
    WrappedCorqOutputPresenter,
)


class TestAction:
    """
    Test boundaries::
        [_results.Action]->[arg resolvers]
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
            resolved_wf_run_id = "<resolved ID>"
            resolved_config = "<resolved config>"
            resolved_inv_id = "<resolved inv id>"

            # Mocks
            error_presenter = create_autospec(WrappedCorqOutputPresenter)
            artifact_presenter = create_autospec(ArtifactPresenter)
            dumper = create_autospec(TaskOutputDumper)
            wf_run_repo = create_autospec(WorkflowRunRepo)

            fake_outputs = ["my_log_1", "my_log_2"]
            wf_run_repo.get_task_outputs.return_value = fake_outputs

            config_resolver = create_autospec(WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_resolver = create_autospec(WFRunResolver)
            wf_run_resolver.resolve_id.return_value = resolved_wf_run_id

            task_inv_id_resolver = create_autospec(TaskInvIDResolver)
            task_inv_id_resolver.resolve.return_value = resolved_inv_id

            action = _results.Action(
                error_presenter=error_presenter,
                artifact_presenter=artifact_presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
                task_inv_id_resolver=task_inv_id_resolver,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id,
                config=config,
                download_dir=download_dir,
                fn_name=fn_name,
                task_inv_id=task_inv_id,
            )

            # Then
            assert error_presenter.method_calls == []

            # We should pass input CLI args to config resolver.
            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_resolver.resolve_id.assert_called_with(wf_run_id, resolved_config)

            task_inv_id_resolver.resolve.assert_called_with(
                task_inv_id, fn_name, resolved_wf_run_id, resolved_config
            )

            # We should pass resolved values to run repo.
            wf_run_repo.get_task_outputs.assert_called_with(
                wf_run_id=resolved_wf_run_id,
                task_inv_id=resolved_inv_id,
                config_name=resolved_config,
            )

            # We expect printing the outputs returned from the repo.
            artifact_presenter.show_task_outputs.assert_called_with(
                values=fake_outputs,
                wf_run_id=resolved_wf_run_id,
                task_inv_id=resolved_inv_id,
            )

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
            resolved_wf_run_id = "<resolved ID>"
            resolved_config = "<resolved config>"
            resolved_inv_id = "<resolved inv id>"

            # Mocks
            error_presenter = create_autospec(WrappedCorqOutputPresenter)
            artifact_presenter = create_autospec(ArtifactPresenter)

            # path_to_logs = "returns whatever"
            # details = create_autospec(DumpDetails)
            dump_details = "<dump details sentinel>"
            dumper = create_autospec(TaskOutputDumper)
            dumper.dump.return_value = dump_details

            wf_run_repo = create_autospec(WorkflowRunRepo)

            fake_outputs = ["my_log_1", "my_log_2"]
            wf_run_repo.get_task_outputs.return_value = fake_outputs

            config_resolver = create_autospec(WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_resolver = create_autospec(WFRunResolver)
            wf_run_resolver.resolve_id.return_value = resolved_wf_run_id

            task_inv_id_resolver = create_autospec(TaskInvIDResolver)
            task_inv_id_resolver.resolve.return_value = resolved_inv_id

            action = _results.Action(
                error_presenter=error_presenter,
                artifact_presenter=artifact_presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
                task_inv_id_resolver=task_inv_id_resolver,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id,
                config=config,
                download_dir=download_dir,
                fn_name=fn_name,
                task_inv_id=task_inv_id,
            )

            # Then
            assert error_presenter.method_calls == []

            # We should pass input CLI args to config resolver.
            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_resolver.resolve_id.assert_called_with(wf_run_id, resolved_config)

            task_inv_id_resolver.resolve.assert_called_with(
                task_inv_id, fn_name, resolved_wf_run_id, resolved_config
            )

            # We should pass resolved values to run repo.
            wf_run_repo.get_task_outputs.assert_called_with(
                wf_run_id=resolved_wf_run_id,
                task_inv_id=resolved_inv_id,
                config_name=resolved_config,
            )

            # Do not print artifact summary.
            artifact_presenter.show_task_outputs.assert_not_called()

            # Dump the two artifacts.
            dumper.dump.assert_any_call(
                value=fake_outputs[0],
                wf_run_id=resolved_wf_run_id,
                task_inv_id=resolved_inv_id,
                output_index=0,
                dir_path=download_dir,
            )
            dumper.dump.assert_any_call(
                value=fake_outputs[1],
                wf_run_id=resolved_wf_run_id,
                task_inv_id=resolved_inv_id,
                output_index=1,
                dir_path=download_dir,
            )

            # Print two dump details, one for each task output.
            artifact_presenter.show_dumped_artifact.assert_called_with(dump_details)
            assert artifact_presenter.show_dumped_artifact.call_count == len(
                fake_outputs
            )
