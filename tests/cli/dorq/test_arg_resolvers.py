################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t
from unittest.mock import Mock

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base.cli._dorq import _arg_resolvers, _repos
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
            config: t.List = []

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


class TestTaskInvIDResolver:
    """
    Test boundaries::
        [TaskInvIDResolver]->[repo]
                           ->[prompters]


    ``config`` and ``wf-run_id`` are assumed to be resolved to a valid value at this
    point.
    """

    @staticmethod
    @pytest.mark.parametrize("fn_name", [None, "foo"])
    def test_passing_inv_id_directly(fn_name):
        # Given
        wf_run_id = "<wf run ID sentinel>"
        config = "<config sentinel>"
        task_inv_id = "<wf run ID sentinel>"

        resolver = _arg_resolvers.TaskInvIDResolver(
            wf_run_repo=Mock(),
            fn_name_prompter=Mock(),
            task_inv_prompter=Mock(),
        )

        # When
        resolved = resolver.resolve(
            task_inv_id=task_inv_id,
            fn_name=fn_name,
            wf_run_id=wf_run_id,
            config=config,
        )

        # Then
        assert resolved == task_inv_id

    class TestResolvingFNName:
        @staticmethod
        def test_passing_fn_name_directly():
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            fn_name = "my_fn"
            task_inv_id = None

            # Mocks
            wf_run_repo = Mock()

            # Needed to let the whole decision tree pass.
            wf_run_repo.get_task_inv_ids.return_value = ["inv1"]

            fn_name_prompter = Mock()

            resolver = _arg_resolvers.TaskInvIDResolver(
                wf_run_repo=wf_run_repo,
                fn_name_prompter=fn_name_prompter,
                task_inv_prompter=Mock(),
            )

            # When
            _ = resolver.resolve(
                task_inv_id=task_inv_id,
                fn_name=fn_name,
                wf_run_id=wf_run_id,
                config=config,
            )

            # Then
            # We should make use of the passed fn name
            wf_run_repo.get_task_inv_ids.assert_called_with(
                wf_run_id=wf_run_id,
                config_name=config,
                task_fn_name=fn_name,
            )
            # We shouldn't ask for the fn name
            assert fn_name_prompter.call_count == 0

        @staticmethod
        def test_one_fn_name():
            """
            User didn't pass in fn name directly, and the workflow has just one
            function name.
            """
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            fn_name = None
            task_inv_id = None

            # Mocks
            wf_run_repo = Mock()
            available_fn_name = "fn1"
            # Just 1 fn name.
            wf_run_repo.get_task_fn_names.return_value = [available_fn_name]

            # Needed to let the whole decision tree pass.
            wf_run_repo.get_task_inv_ids.return_value = ["inv1"]

            fn_name_prompter = Mock()

            resolver = _arg_resolvers.TaskInvIDResolver(
                wf_run_repo=wf_run_repo,
                fn_name_prompter=fn_name_prompter,
                task_inv_prompter=Mock(),
            )

            # When
            _ = resolver.resolve(
                task_inv_id=task_inv_id,
                fn_name=fn_name,
                wf_run_id=wf_run_id,
                config=config,
            )

            # Then
            # We should make use of the single available fn name
            wf_run_repo.get_task_inv_ids.assert_called_with(
                wf_run_id=wf_run_id,
                config_name=config,
                task_fn_name=available_fn_name,
            )
            # We shouldn't ask for the fn name
            assert fn_name_prompter.call_count == 0

        @staticmethod
        def test_many_fns():
            """
            This workflow has just a couple of function names.
            """
            # Given
            wf_run_id = "<wf run ID sentinel>"
            config = "<config sentinel>"
            fn_name = None
            task_inv_id = None

            # Mocks
            wf_run_repo = Mock()
            available_fn_names = ["fn1", "fn2", "fn3"]
            wf_run_repo.get_task_fn_names.return_value = available_fn_names

            # Needed to let the whole decision tree pass.
            wf_run_repo.get_task_inv_ids.return_value = ["inv1"]

            fn_name_prompter = Mock()
            selected_fn_name = available_fn_names[2]
            fn_name_prompter.choice.return_value = selected_fn_name

            resolver = _arg_resolvers.TaskInvIDResolver(
                wf_run_repo=wf_run_repo,
                fn_name_prompter=fn_name_prompter,
                task_inv_prompter=Mock(),
            )

            # When
            _ = resolver.resolve(
                task_inv_id=task_inv_id,
                fn_name=fn_name,
                wf_run_id=wf_run_id,
                config=config,
            )

            # Then
            # We should ask for the fn name
            fn_name_prompter.choice.assert_called_with(
                available_fn_names, message="Task function name"
            )

            # We should make use of the selected fn name
            wf_run_repo.get_task_inv_ids.assert_called_with(
                wf_run_id=wf_run_id,
                config_name=config,
                task_fn_name=selected_fn_name,
            )

    @staticmethod
    def test_selecting_inv_id():
        """
        We're assuming the ``fn_name`` was passed explicitly. Branches where we need to
        prompt for it are tested separately.
        """
        # Given
        wf_run_id = "<wf run ID sentinel>"
        config = "<config sentinel>"
        fn_name = "foo"
        task_inv_id = None

        # Mocks
        wf_run_repo = Mock()
        inv_ids = ["inv1", "inv2", "inv3"]
        wf_run_repo.get_task_inv_ids.return_value = inv_ids

        task_inv_prompter = Mock()
        selected_inv_id = inv_ids[2]
        task_inv_prompter.choice.return_value = selected_inv_id

        resolver = _arg_resolvers.TaskInvIDResolver(
            wf_run_repo=wf_run_repo,
            fn_name_prompter=Mock(),
            task_inv_prompter=task_inv_prompter,
        )

        # When
        resolved = resolver.resolve(
            task_inv_id=task_inv_id,
            fn_name=fn_name,
            wf_run_id=wf_run_id,
            config=config,
        )

        # Then
        assert resolved == selected_inv_id

        # We should show a prompt with invocation IDs.
        task_inv_prompter.choice.assert_called_with(
            inv_ids, message="Task invocation ID"
        )


class TestTaskRunIDResolver:
    """
    Test boundaries::
        [TaskRunIDResolver]->[repo]
                           ->[nested resolvers]
                           ->[prompters]


    ``config`` is assumed to be resolved to a valid value at this point.
    """

    @staticmethod
    @pytest.mark.parametrize("wf_run_id", [None, "wf.1"])
    @pytest.mark.parametrize("fn_name", [None, "foo"])
    @pytest.mark.parametrize("task_inv_id", [None, "inv1"])
    def test_passing_task_run_id_directly(wf_run_id, fn_name, task_inv_id):
        """
        It should return the passed task_run_id regardless of wf_run_id, fn_name, and
        task_inv_id.
        """
        # Given
        config = "<config sentinel>"
        task_run_id = "wf.1"

        resolver = _arg_resolvers.TaskRunIDResolver(
            wf_run_repo=Mock(),
            wf_run_resolver=Mock(),
            task_inv_id_resolver=Mock(),
        )

        # When
        resolved = resolver.resolve(
            task_run_id=task_run_id,
            wf_run_id=wf_run_id,
            fn_name=fn_name,
            task_inv_id=task_inv_id,
            config=config,
        )

        # Then
        assert resolved == task_run_id

    @staticmethod
    def test_passing_data():
        # Given
        config = "<config sentinel>"
        task_run_id = None
        wf_run_id = "wf.1"
        fn_name = "my_fn"
        task_inv_id = "inv1"

        # Mocks
        wf_run_resolver = Mock(_arg_resolvers.WFRunResolver)
        resolved_wf_run_id = "resolved wf run id"
        wf_run_resolver.resolve_id.return_value = resolved_wf_run_id

        task_inv_id_resolver = Mock(_arg_resolvers.TaskInvIDResolver)
        resolved_inv_id = "resolved inv id"
        task_inv_id_resolver.resolve.return_value = resolved_inv_id

        wf_run_repo = Mock(_repos.WorkflowRunRepo)
        resolved_task_run_id = "task run 1"
        wf_run_repo.get_task_run_id.return_value = resolved_task_run_id

        resolver = _arg_resolvers.TaskRunIDResolver(
            wf_run_repo=wf_run_repo,
            wf_run_resolver=wf_run_resolver,
            task_inv_id_resolver=task_inv_id_resolver,
        )

        # When
        resolved = resolver.resolve(
            task_run_id=task_run_id,
            wf_run_id=wf_run_id,
            fn_name=fn_name,
            task_inv_id=task_inv_id,
            config=config,
        )

        # Then
        # Should delegate wf_run_id resolution to the nested resolver.
        wf_run_resolver.resolve_id.assert_called_with(wf_run_id, config)

        # Should delegate task_inv_id resolution to the nested resolver.
        task_inv_id_resolver.resolve.assert_called_with(
            task_inv_id=task_inv_id,
            fn_name=fn_name,
            wf_run_id=resolved_wf_run_id,
            config=config,
        )

        # Should pass resolved IDs to the repo
        wf_run_repo.get_task_run_id.assert_called_with(
            wf_run_id=resolved_wf_run_id,
            task_inv_id=resolved_inv_id,
            config_name=config,
        )

        assert resolved == resolved_task_run_id


@pytest.mark.parametrize(
    "args,expected_services",
    [
        pytest.param((None, None), ("Ray",), id="Default"),
        pytest.param((True, None), ("Ray",), id="Ray"),
        pytest.param((None, True), ("Ray",), id="All"),
        pytest.param((True, True), ("Ray",), id="Ray and All"),
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
            states: t.List[str] = []
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
            states: t.List[str] = []
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
