################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base.cli._dorq import _arg_resolvers
from orquestra.sdk.schema.workflow_run import State


class TestConfigResolver:
    """
    Test boundaries::
        [ConfigResolver]->[repos]
                        ->[prompter]
    """

    @staticmethod
    def test_passing_config_directly():
        """
        User passed `config` value directly as CLI arg.
        """
        # Given
        config = "<config sentinel>"

        resolver = _arg_resolvers.ConfigResolver(
            config_repo=Mock(),
            prompter=Mock(),
        )

        # When
        resolved_config = resolver.resolve(config=config)

        # Then
        assert resolved_config == config

    @staticmethod
    def test_no_config():
        # Given
        config = None

        config_repo = Mock()
        local_config_names = ["cfg1", "cfg2"]
        config_repo.list_config_names.return_value = local_config_names

        prompter = Mock()
        selected_config = local_config_names[1]
        prompter.choice.return_value = selected_config

        resolver = _arg_resolvers.ConfigResolver(
            config_repo=config_repo,
            prompter=prompter,
        )

        # When
        resolved_config = resolver.resolve(config=config)

        # Then
        # We expect prompt for selecting config.
        prompter.choice.assert_called_with(local_config_names, message="Runtime config")

        # Resolver should return the user's choice.
        assert resolved_config == selected_config

    class TestResolveMultiple:
        @staticmethod
        def test_passing_single_config_directly():
            """
            User passed one `config` value directly as CLI arg.
            """
            # Given
            config = ["<config sentinel>"]
            prompter = Mock()
            resolver = _arg_resolvers.ConfigResolver(
                config_repo=Mock(),
                prompter=prompter,
            )

            # When
            resolved_config = resolver.resolve_multiple(configs=config)

            # Then
            assert resolved_config == config
            prompter.assert_not_called()

        @staticmethod
        def test_passing_multiple_configs_directly():
            """
            User passed multiple `config` values directly as CLI arg.
            """
            # Given
            config = ["<config sentinel 1>", "<config sentinel 2>"]
            prompter = Mock()
            selected_config = config[1]
            prompter.choice.return_value = selected_config

            resolver = _arg_resolvers.ConfigResolver(
                config_repo=Mock(),
                prompter=prompter,
            )

            # When
            resolved_config = resolver.resolve_multiple(configs=config)

            # Then
            assert resolved_config == config
            prompter.assert_not_called()

        @staticmethod
        def test_launches_prompter():
            """
            User passed no config values. We should use the prompter to get them to
            choose from available configs.
            """
            # Given
            config = []

            config_repo = Mock()
            local_config_names = ["cfg1", "cfg2"]
            config_repo.list_config_names.return_value = local_config_names

            prompter = Mock()
            selected_config = [local_config_names[1]]
            prompter.checkbox.return_value = selected_config

            resolver = _arg_resolvers.ConfigResolver(
                config_repo=config_repo,
                prompter=prompter,
            )

            # When
            resolved_config = resolver.resolve_multiple(configs=config)

            # # Then
            # We expect prompt for selecting config.
            prompter.checkbox.assert_called_with(
                local_config_names, message="Runtime config(s)"
            )

            # Resolver should return the user's choice.
            assert resolved_config == selected_config


class TestWFConfigResolver:
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

        resolver = _arg_resolvers.WFConfigResolver(
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
        @pytest.mark.parametrize("wf_run_id", ["<wf run ID sentinel>", None])
        def test_passing_config_directly(wf_run_id):
            """
            User passed `config` value directly as CLI arg.

            We expect the same result regardless of ``wf_run_id``.
            """
            # Given
            config = "<config sentinel>"

            resolver = _arg_resolvers.WFConfigResolver(
                wf_run_repo=Mock(),
                config_repo=Mock(),
                prompter=Mock(),
            )

            # When
            resolved_config = resolver.resolve(wf_run_id=wf_run_id, config=config)

            # Then
            assert resolved_config == config

        @staticmethod
        def test_valid_wf_run_id_passed():
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = None

            wf_run_repo = Mock()
            stored_config = "<read config sentinel>"
            wf_run_repo.get_config_name_by_run_id.return_value = stored_config

            prompter = Mock()

            resolver = _arg_resolvers.WFConfigResolver(
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

            resolver = _arg_resolvers.WFConfigResolver(
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

            resolver = _arg_resolvers.WFConfigResolver(
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


class TestWFRunResolver:
    """
    Test boundaries::
        [WFRunResolver]->[repo]
                         ->[prompter]


    ``config`` is assumed to be resolved to a valid value at this point.
    """

    class TestResolveID:
        @staticmethod
        def test_passing_id_directly():
            """
            User passed ``wf_run_id`` value directly as CLI arg.
            """
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=Mock(),
                prompter=Mock(),
            )

            # When
            resolved_id = resolver.resolve_id(wf_run_id=wf_run_id, config=config)

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

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=wf_run_repo,
                prompter=prompter,
            )

            # When
            resolved_id = resolver.resolve_id(wf_run_id=wf_run_id, config=config)

            # Then
            # We should pass config value to wf_run_repo.
            wf_run_repo.list_wf_run_ids.assert_called_with(config)

            # We should prompt for selecting workflow ID from the ones returned
            # by the repo.
            prompter.choice.assert_called_with(
                listed_run_ids, message="Workflow run ID"
            )

            # Resolver should return the user's choice.
            assert resolved_id == selected_id

    class TestResolveRun:
        @staticmethod
        def test_passing_id_directly():
            """
            User passed ``wf_run_id`` value directly as CLI arg.
            """
            # Given
            wf_run_id = "<wf run ID sentinel>"
            wf_run = "<wf run sentinel>"
            config = "<config sentinel>"

            repo = Mock()
            repo.get_wf_by_run_id.return_value = wf_run

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=repo,
                prompter=Mock(),
            )

            # When
            resolved_run = resolver.resolve_run(wf_run_id=wf_run_id, config=config)

            # Then
            assert resolved_run == wf_run

        @staticmethod
        def test_no_wf_run_id():
            """
            User didn't pass ``wf_run_id``.
            """
            # Given
            wf_run_id = None
            config = "<config sentinel>"

            wf_run_repo = Mock()
            listed_runs = [Mock(), Mock()]
            wf_run_repo.list_wf_runs.return_value = listed_runs

            prompter = Mock()
            selected_run = listed_runs[0]
            prompter.choice.return_value = selected_run

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=wf_run_repo,
                prompter=prompter,
            )

            # When
            resolved_run = resolver.resolve_run(wf_run_id=wf_run_id, config=config)

            # Then
            # We should pass config value to wf_run_repo.
            wf_run_repo.list_wf_runs.assert_called_with(config)

            # We should prompt for selecting workflow run from the IDs returned
            # by the repo.
            prompter.choice.assert_called_with(
                [(run.id, run) for run in listed_runs], message="Workflow run ID"
            )

            # Resolver should return the user's choice.
            assert resolved_run == selected_run


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


class TestWFRunFilterResolver:
    """
    Test boundaries::
        [WFRunFilterResolver]->[repos]
                             ->[prompter]
    """

    class TestResolveLimit:
        @staticmethod
        def test_passing_limit_directly():
            # Given
            limit = 1701
            prompter = Mock()

            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)

            # When
            resolved_limit = resolver.resolve_limit(limit=limit)

            # Then
            assert resolved_limit == limit
            prompter.assert_not_called()

        @staticmethod
        def test_no_limit_default():
            # Given
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)

            # When
            resolved_limit = resolver.resolve_limit()

            # Then
            assert resolved_limit is None
            prompter.assert_not_called()

        @staticmethod
        def test_no_limit_interactive():
            # Given
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)
            selected_limit = 1649
            prompter.ask_for_int.return_value = selected_limit

            # When
            resolved_limit = resolver.resolve_limit(interactive=True)

            # Then
            assert resolved_limit == selected_limit
            prompter.ask_for_int.assert_called_with(
                message=(
                    "Enter maximum number of results to display. "
                    "If 'None', all results will be displayed."
                ),
                default="None",
                allow_none=True,
            )

    class TestResolveMaxAge:
        @staticmethod
        def test_passing_max_age_directly():
            # Given
            max_age = "<max_age sentinel>"
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)

            # When
            resolved_max_age = resolver.resolve_max_age(max_age=max_age)

            # Then
            assert resolved_max_age == max_age
            prompter.assert_not_called()

        @staticmethod
        def test_no_age_argument_default():
            # Given
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)

            # When
            resolved_max_age = resolver.resolve_max_age()

            # Then
            assert resolved_max_age is None
            prompter.assert_not_called()

        @staticmethod
        def test_no_age_argument_interactive():
            # Given
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)
            selected_max_age = "<max_age_sentinel>"
            prompter.ask_for_str.return_value = selected_max_age

            # When
            resolved_max_age = resolver.resolve_max_age(interactive=True)

            # Then
            assert resolved_max_age == selected_max_age
            prompter.ask_for_str.assert_called_once_with(
                message=(
                    "Maximum age of run to display. "
                    "If 'None', all results will be displayed."
                ),
                default="None",
                allow_none=True,
            )

    class TestResolveState:
        @staticmethod
        @pytest.mark.parametrize(
            "state, expected_state", [([e.value], [e]) for e in State]
        )
        def test_passing_single_valid_state(state, expected_state):
            # Given
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)

            # When
            resolved_states = resolver.resolve_state(states=state)

            # Then
            assert resolved_states == expected_state
            prompter.assert_not_called()

        @staticmethod
        def test_passing_multiple_valid_states():
            # Given
            states = ["WAITING", "SUCCEEDED"]
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)

            # When
            resolved_states = resolver.resolve_state(states=states)

            # Then
            assert resolved_states == [State("WAITING"), State("SUCCEEDED")]
            prompter.assert_not_called()

        @staticmethod
        def test_passing_single_invalid_state():
            # Given
            states = ["JUGGLING"]
            return_states = ["WAITING"]
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)
            prompter.checkbox.return_value = return_states

            # When
            resolved_states = resolver.resolve_state(states=states)

            # Then
            assert resolved_states == [State(s) for s in return_states]
            prompter.checkbox.assert_called_with(
                choices=[e.value for e in State],
                default=[e.value for e in State],
                message="Workflow Run State(s)",
            )

        @staticmethod
        def test_passing_mixed_valid_and_invalid_states():
            # Given
            states = ["JUGGLING", "WAITING"]
            return_states = ["RUNNING", "WAITING"]
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)
            prompter.checkbox.return_value = return_states

            # When
            resolved_states = resolver.resolve_state(states=states)

            # Then
            assert resolved_states == [State(s) for s in return_states]
            prompter.checkbox.assert_called_with(
                choices=[e.value for e in State],
                default=["WAITING"],
                message="Workflow Run State(s)",
            )

        @staticmethod
        def test_passing_no_state_default():
            # Given
            states = []
            return_states = ["WAITING"]
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)
            prompter.checkbox.return_value = return_states

            # When
            resolved_states = resolver.resolve_state(states=states)

            # Then
            assert resolved_states is None
            prompter.assert_not_called()

        @staticmethod
        def test_passing_no_state_interactive():
            # Given
            states = []
            return_states = ["WAITING"]
            prompter = Mock()
            resolver = _arg_resolvers.WFRunFilterResolver(prompter=prompter)
            prompter.checkbox.return_value = return_states

            # When
            resolved_states = resolver.resolve_state(states=states, interactive=True)

            # Then
            assert resolved_states == [State(s) for s in return_states]
            prompter.checkbox.assert_called_with(
                choices=[e.value for e in State],
                default=[e.value for e in State],
                message="Workflow Run State(s)",
            )
