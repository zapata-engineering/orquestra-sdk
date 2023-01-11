################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base.cli._dorq import _arg_resolvers


class TestConfigResolver:
    """
    Test boundaries::
        [ConfigResolver]->[repos]
                        ->[prompter]
    """

    @staticmethod
    @pytest.mark.parametrize("wf_run_id", ["<wf run ID sentinel>", None])
    def test_passing_config_directly(wf_run_id):
        """
        User passed `config` value directly as CLI arg.

        We expect the same result regardless of ``wf_run_id``.
        """
        # Given
        config = "<config sentinel>"

        resolver = _arg_resolvers.ConfigResolver(
            wf_run_repo=Mock(),
            config_repo=Mock(),
            prompter=Mock(),
        )

        # When
        resolved_config = resolver.resolve(wf_run_id=wf_run_id, config=config)

        # Then
        assert resolved_config == config

    class TestNoConfig:
        """
        User didn't pass `config`.
        """

        @staticmethod
        def test_valid_wf_run_id_passed():
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = None

            wf_run_repo = Mock()
            stored_config = "<read config sentinel>"
            wf_run_repo.get_config_name_by_run_id.return_value = stored_config

            prompter = Mock()

            resolver = _arg_resolvers.ConfigResolver(
                wf_run_repo=wf_run_repo,
                config_repo=Mock(),
                prompter=prompter,
            )

            # When
            resolved_config = resolver.resolve(wf_run_id=wf_run_id, config=config)

            # Then
            assert resolved_config == stored_config

            # We expect reading name from wf_run_repo.
            wf_run_repo.get_config_name_by_run_id.assert_called_with(wf_run_id)

            # We expect no prompts.
            prompter.choice.assert_not_called()

        @staticmethod
        def test_foreign_wf_run_id_passed():
            """
            Example use case: ``orq wf stop other-colleagues-wf-run``. We don't have
            this workflow in the local DB, but it doesn't mean the workflow doesn't
            exist on the cluster. We should ask the user for the config and proceed
            with the action.
            """
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = None

            config_repo = Mock()
            local_config_names = ["cfg1", "cfg2"]
            config_repo.list_config_names.return_value = local_config_names

            wf_run_repo = Mock()
            wf_run_repo.get_config_name_by_run_id.side_effect = (
                exceptions.WorkflowRunNotFoundError()
            )

            prompter = Mock()
            selected_config = local_config_names[1]
            prompter.choice.return_value = selected_config

            resolver = _arg_resolvers.ConfigResolver(
                wf_run_repo=wf_run_repo,
                config_repo=config_repo,
                prompter=prompter,
            )

            # When
            resolved_config = resolver.resolve(wf_run_id=wf_run_id, config=config)

            # Then
            prompter.choice.assert_called_with(
                local_config_names, message="Runtime config"
            )

            # Resolver should return the user's choice.
            assert resolved_config == selected_config

        @staticmethod
        def test_no_wf_run_id():
            # Given
            wf_run_id = None
            config = None

            config_repo = Mock()
            local_config_names = ["cfg1", "cfg2"]
            config_repo.list_config_names.return_value = local_config_names

            prompter = Mock()
            selected_config = local_config_names[1]
            prompter.choice.return_value = selected_config

            resolver = _arg_resolvers.ConfigResolver(
                wf_run_repo=Mock(),
                config_repo=config_repo,
                prompter=prompter,
            )

            # When
            resolved_config = resolver.resolve(wf_run_id=wf_run_id, config=config)

            # Then
            # We expect prompt for selecting config.
            prompter.choice.assert_called_with(
                local_config_names, message="Runtime config"
            )

            # Resolver should return the user's choice.
            assert resolved_config == selected_config


class TestWFRunIDResolver:
    """
    Test boundaries::
        [WFRunIDResolver]->[repo]
                         ->[prompter]


    ``config`` is assumed to be resolved to a valid value at this point.
    """

    @staticmethod
    def test_passing_id_directly():
        """
        User passed ``wf_run_id`` value directly as CLI arg.
        """
        # Given
        wf_run_id = "<wf run ID sentinel>"
        config = "<config sentinel>"

        resolver = _arg_resolvers.WFRunIDResolver(
            wf_run_repo=Mock(),
            prompter=Mock(),
        )

        # When
        resolved_id = resolver.resolve(wf_run_id=wf_run_id, config=config)

        # Then
        assert resolved_id == wf_run_id

    @staticmethod
    def test_no_wf_run_id():
        """
        User didn't pass ``wf_run_id``.
        """
        # Given
        wf_run_id = None
        config = "<config sentinel>"

        wf_run_repo = Mock()
        listed_run_ids = ["wf1", "wf2"]
        wf_run_repo.list_wf_run_ids.return_value = listed_run_ids

        prompter = Mock()
        selected_id = listed_run_ids[0]
        prompter.choice.return_value = selected_id

        resolver = _arg_resolvers.WFRunIDResolver(
            wf_run_repo=wf_run_repo,
            prompter=prompter,
        )

        # When
        resolved_id = resolver.resolve(wf_run_id=wf_run_id, config=config)

        # Then
        # We should pass config value to wf_run_repo.
        wf_run_repo.list_wf_run_ids.assert_called_with(config)

        # We should prompt for selecting workflow ID from the ones returned by the repo.
        prompter.choice.assert_called_with(listed_run_ids, message="Workflow run ID")

        # Resolver should return the user's choice.
        assert resolved_id == selected_id


@pytest.mark.parametrize(
    "args,expected_services",
    [
        pytest.param((None, None, None), ("Ray",), id="Default"),
        pytest.param((True, None, None), ("Ray",), id="Ray"),
        pytest.param((None, True, None), ("Fluent Bit",), id="Fluent Bit"),
        pytest.param((None, None, True), ("Ray", "Fluent Bit"), id="All"),
        pytest.param(
            (True, True, None), ("Ray", "Fluent Bit"), id="Ray and Fluent Bit"
        ),
        pytest.param(
            (True, True, True), ("Ray", "Fluent Bit"), id="Ray, Fluent Bit and All"
        ),
    ],
)
def test_service_resolver(args, expected_services):
    # Given
    service_resolver = _arg_resolvers.ServiceResolver()
    # When
    services = service_resolver.resolve(*args)
    # Then
    names = [svc.name for svc in services]
    assert all(svc_name in names for svc_name in expected_services)
