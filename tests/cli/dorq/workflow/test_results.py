################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf results' glue code.
"""

from pathlib import Path
from unittest.mock import create_autospec

from orquestra.sdk._base.cli._dorq import _arg_resolvers, _dumpers, _repos
from orquestra.sdk._base.cli._dorq._ui import _presenters
from orquestra.sdk._base.cli._dorq._workflow import _results


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

            # Resolved values
            resolved_id = "<resolved ID>"
            resolved_config = "<resolved config>"

            # Mocks
            artifact_presenter = create_autospec(_presenters.ArtifactPresenter)
            error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            dumper = create_autospec(_dumpers.WFOutputDumper)
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            fake_outputs = [object(), "hello", None]
            wf_run_repo.get_wf_outputs.return_value = fake_outputs

            config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_resolver = create_autospec(_arg_resolvers.WFRunResolver)
            wf_run_resolver.resolve_id.return_value = resolved_id

            action = _results.Action(
                artifact_presenter=artifact_presenter,
                error_presenter=error_presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id, config=config, download_dir=download_dir
            )

            # Then
            # We should pass input CLI args to config resolver.
            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_resolver.resolve_id.assert_called_with(wf_run_id, resolved_config)

            # We should pass resolved values to run repo.
            wf_run_repo.get_wf_outputs.assert_called_with(
                wf_run_id=resolved_id, config_name=resolved_config
            )

            # We expect printing the workflow run returned from the repo.
            artifact_presenter.show_workflow_outputs.assert_called_with(
                fake_outputs, resolved_id
            )

            # We don't expect any dumps.
            assert dumper.mock_calls == []

        @staticmethod
        def test_with_download_dir():
            # Given
            # CLI inputs
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            download_dir = Path("some") / "dir"

            # Resolved values
            resolved_id = "<resolved ID>"
            resolved_config = "<resolved config>"

            # Mocks
            artifact_presenter = create_autospec(_presenters.ArtifactPresenter)
            error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            dumper = create_autospec(_dumpers.WFOutputDumper)

            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            fake_outputs = [object(), "hello", None]
            wf_run_repo.get_wf_outputs.return_value = fake_outputs

            config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)
            config_resolver.resolve.return_value = resolved_config

            wf_run_resolver = create_autospec(_arg_resolvers.WFRunResolver)
            wf_run_resolver.resolve_id.return_value = resolved_id

            action = _results.Action(
                artifact_presenter=artifact_presenter,
                error_presenter=error_presenter,
                dumper=dumper,
                wf_run_repo=wf_run_repo,
                config_resolver=config_resolver,
                wf_run_resolver=wf_run_resolver,
            )

            # When
            action.on_cmd_call(
                wf_run_id=wf_run_id, config=config, download_dir=download_dir
            )

            # Then
            # We should pass input CLI args to config resolver.
            config_resolver.resolve.assert_called_with(wf_run_id, config)

            # We should pass resolved_config to run ID resolver.
            wf_run_resolver.resolve_id.assert_called_with(wf_run_id, resolved_config)

            # We should pass resolved values to run repo.
            wf_run_repo.get_wf_outputs.assert_called_with(
                wf_run_id=resolved_id, config_name=resolved_config
            )

            # We expect a summary printed for each output
            assert len(artifact_presenter.mock_calls) == len(fake_outputs)

            # We expect a dump for each output
            assert len(dumper.mock_calls) == len(fake_outputs)

    @staticmethod
    def test_presents_errors():
        # Given
        error_presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
        config_resolver = create_autospec(_arg_resolvers.WFConfigResolver)

        err = RuntimeError()
        config_resolver.resolve.side_effect = err

        action = _results.Action(
            error_presenter=error_presenter,
            config_resolver=config_resolver,
        )

        # When
        action.on_cmd_call(wf_run_id=None, config=None, download_dir=None)

        # Then
        error_presenter.show_error.assert_called_with(err)
