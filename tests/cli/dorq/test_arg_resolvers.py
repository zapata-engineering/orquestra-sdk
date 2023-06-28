################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t
from datetime import datetime, timedelta
from unittest.mock import Mock, create_autospec

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base._logs._interfaces import WorkflowLogs, WorkflowLogTypeName
from orquestra.sdk._base._spaces._structs import Project, ProjectRef, Workspace
from orquestra.sdk._base.cli._dorq import _arg_resolvers, _repos
from orquestra.sdk._base.cli._dorq._ui import _presenters, _prompts
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.workflow_run import RunStatus, State


class TestConfigResolver:
    """
    Test boundaries::
        [ConfigResolver]->[repos]
                        ->[prompter]
    """

    class TestResolveSingular:
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
            prompter.choice.assert_called_with(
                local_config_names, message="Runtime config"
            )

            # Resolver should return the user's choice.
            assert resolved_config == selected_config

        @staticmethod
        def test_with_in_process():
            # Given
            config = "in_process"
            resolver = _arg_resolvers.ConfigResolver(
                config_repo=Mock(), prompter=Mock()
            )

            # When/Then
            with pytest.raises(exceptions.InProcessFromCLIError):
                resolver.resolve(config)

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

    class TestResolveStoredForLogin:
        @staticmethod
        def test_passing_valid_config_name():
            # GIVEN
            # cli inputs
            config = "<config sentinel>"

            # Stored configs
            stored_config = create_autospec(RuntimeConfiguration)
            stored_config.config_name = config
            stored_config.runtime_options = {"uri": "<stored uri sentinel>"}
            stored_configs = [stored_config]

            # test boundaries
            config_repo = create_autospec(_repos.ConfigRepo)
            config_repo.list_config_names.return_value = [
                config.config_name for config in stored_configs
            ]
            config_repo.read_config.side_effect = stored_configs
            prompter = create_autospec(_prompts.Prompter)

            resolver = _arg_resolvers.ConfigResolver(
                config_repo=config_repo,
                prompter=prompter,
            )

            # WHEN
            resolved_config = resolver.resolve_stored_config_for_login(config)

            # THEN
            assert resolved_config == config
            prompter.choice.assert_not_called()

        @staticmethod
        def test_passing_non_existant_config_name():
            # GIVEN
            # cli inputs
            config = "<config sentinel>"

            # Stored configs
            stored_config = create_autospec(RuntimeConfiguration)
            stored_config.config_name = "<stored config name sentinel>"
            stored_config.runtime_options = {"uri": "<stored uri sentinel>"}
            stored_configs = [stored_config]

            # test boundaries
            config_repo = create_autospec(_repos.ConfigRepo)
            config_repo.list_config_names.return_value = [
                config.config_name for config in stored_configs
            ]
            config_repo.read_config.side_effect = stored_configs
            prompter = create_autospec(_prompts.Prompter)
            prompter.choice.return_value = "<chosen config sentinel>"

            resolver = _arg_resolvers.ConfigResolver(
                config_repo=config_repo,
                prompter=prompter,
            )

            # WHEN
            resolved_config = resolver.resolve_stored_config_for_login(config)

            # THEN
            assert resolved_config == "<chosen config sentinel>"
            prompter.choice.assert_called_once_with(
                ["<stored config name sentinel>"],
                message=(
                    "No config '<config sentinel>' found in file. "
                    "Please select a valid config"
                ),
            )

        @staticmethod
        def test_passing_local_config_name():
            # GIVEN
            # cli inputs
            config = "<config sentinel>"

            # Stored configs
            stored_config = create_autospec(RuntimeConfiguration)
            stored_config.config_name = "<stored config name sentinel>"
            stored_config.runtime_options = {"uri": "<stored uri sentinel>"}
            stored_local_config = create_autospec(RuntimeConfiguration)
            stored_local_config.config_name = config
            stored_local_config.runtime_options = {}
            stored_configs = [stored_config, stored_local_config]

            # test boundaries
            config_repo = create_autospec(_repos.ConfigRepo)
            config_repo.list_config_names.return_value = [
                config.config_name for config in stored_configs
            ]
            config_repo.read_config.side_effect = stored_configs
            prompter = create_autospec(_prompts.Prompter)
            prompter.choice.return_value = "<chosen config sentinel>"

            resolver = _arg_resolvers.ConfigResolver(
                config_repo=config_repo,
                prompter=prompter,
            )

            # WHEN
            resolved_config = resolver.resolve_stored_config_for_login(config)

            # THEN
            assert resolved_config == "<chosen config sentinel>"
            prompter.choice.assert_called_once_with(
                ["<stored config name sentinel>"],
                message=(
                    "Cannot log in with '<config sentinel>' as it relates to local "
                    "runs. Please select a valid config"
                ),
            )


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
        @pytest.mark.parametrize("runtime_supports_workspaces", [True, False])
        def test_no_wf_run_id(runtime_supports_workspaces):
            """
            User didn't pass ``wf_run_id``.
            """
            # Given
            current_time = datetime.now().astimezone()

            def return_wf(id, time_delay_in_sec: int):
                run = Mock()
                run.id = id
                run.status = RunStatus(
                    state=State.RUNNING,
                    start_time=current_time + timedelta(seconds=time_delay_in_sec),
                    end_time=current_time + timedelta(seconds=time_delay_in_sec),
                )
                return run

            wf_run_id = None
            config = "<config sentinel>"

            wf_run_repo = Mock()
            time_delta = 1000
            listed_runs = [return_wf("1", 0), return_wf("2", time_delta)]
            wf_run_repo.list_wf_runs.return_value = listed_runs

            prompter = create_autospec(_prompts.Prompter)

            selected_id = listed_runs[0].id
            prompter.choice.return_value = selected_id
            spaces_resolver = create_autospec(_arg_resolvers.SpacesResolver)
            fake_ws = "wake ws"
            fake_project = "fake project"
            if runtime_supports_workspaces:
                spaces_resolver.resolve_workspace_id.return_value = fake_ws
                spaces_resolver.resolve_project_id.return_value = fake_project
            else:
                spaces_resolver.resolve_workspace_id.side_effect = (
                    exceptions.WorkspacesNotSupportedError()
                )

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=wf_run_repo,
                prompter=prompter,
                spaces_resolver=spaces_resolver,
            )

            # When
            resolved_id = resolver.resolve_id(wf_run_id=wf_run_id, config=config)

            # Then
            # We should pass config value to wf_run_repo.
            if runtime_supports_workspaces:
                wf_run_repo.list_wf_runs.assert_called_with(
                    config, workspace=fake_ws, project=fake_project
                )
            else:
                wf_run_repo.list_wf_runs.assert_called_with(
                    config, workspace=None, project=None
                )

            # We should prompt for selecting workflow ID from the ones returned
            # by the repo. Those choices should be sorted from newest at the top
            prompter.choice.assert_called_with(
                [
                    (
                        "2  " + (current_time + timedelta(seconds=time_delta)).ctime(),
                        "2",
                    ),
                    ("1  " + current_time.ctime(), "1"),
                ],
                message="Workflow run ID",
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

            repo = create_autospec(_repos.WorkflowRunRepo)
            repo.get_wf_by_run_id.return_value = wf_run

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=repo,
                prompter=create_autospec(_prompts.Prompter),
            )

            # When
            resolved_run = resolver.resolve_run(wf_run_id=wf_run_id, config=config)

            # Then
            assert resolved_run == wf_run

        @staticmethod
        @pytest.mark.parametrize("runtime_supports_workspaces", [True, False])
        def test_no_wf_run_id(runtime_supports_workspaces):
            """
            User didn't pass ``wf_run_id``.
            """
            # Given
            current_time = datetime.now()

            def return_wf(id, time_delay_in_sec: int):
                run = Mock()
                run.id = id
                run.status = RunStatus(
                    state=State.RUNNING,
                    start_time=current_time + timedelta(seconds=time_delay_in_sec),
                    end_time=current_time + timedelta(seconds=time_delay_in_sec),
                )
                return run

            wf_run_id = None
            config = "<config sentinel>"
            spaces_resolver = create_autospec(_arg_resolvers.SpacesResolver)
            fake_ws = "wake ws"
            fake_project = "fake project"
            if runtime_supports_workspaces:
                spaces_resolver.resolve_workspace_id.return_value = fake_ws
                spaces_resolver.resolve_project_id.return_value = fake_project
            else:
                spaces_resolver.resolve_workspace_id.side_effect = (
                    exceptions.WorkspacesNotSupportedError()
                )

            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            time_delta = 1000
            listed_runs = [return_wf("1", 0), return_wf("2", time_delta)]
            wf_run_repo.list_wf_runs.return_value = listed_runs

            prompter = create_autospec(_prompts.Prompter)
            selected_run = listed_runs[0]
            prompter.choice.return_value = selected_run

            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=wf_run_repo,
                prompter=prompter,
                spaces_resolver=spaces_resolver,
            )

            # When
            resolved_run = resolver.resolve_run(wf_run_id=wf_run_id, config=config)

            # Then
            # We should pass config value to wf_run_repo.
            if runtime_supports_workspaces:
                wf_run_repo.list_wf_runs.assert_called_with(
                    config, workspace="wake ws", project="fake project"
                )
            else:
                wf_run_repo.list_wf_runs.assert_called_with(
                    config,
                    workspace=None,
                    project=None,
                )

            # We should prompt for selecting workflow run from the IDs returned
            # by the repo.
            prompter.choice.assert_called_with(
                [
                    (
                        "2  " + (current_time + timedelta(seconds=time_delta)).ctime(),
                        listed_runs[1],  # this has later start_time than [0]
                    ),
                    ("1  " + current_time.ctime(), listed_runs[0]),
                ],
                message="Workflow run ID",
            )

            # Resolver should return the user's choice.
            assert resolved_run == selected_run

    class TestResolveLogSwitches:
        @staticmethod
        @pytest.mark.parametrize(
            "switches",
            [
                (False, False, True),
                (False, True, False),
                (False, True, True),
                (True, False, False),
                (True, False, True),
                (True, True, False),
                (True, True, True),
            ],
        )
        def test_returns_unchanged_if_all_switches_are_set_and_all_logs_available(
            switches,
        ):
            """
            The most trivial case - all of the log types are available, and the user
            has set every switch. Under these circumstances the resolver should do
            nothing.
            """
            # Given
            logs = WorkflowLogs(
                per_task={"foo": ["fop"]}, system=["bar"], env_setup=["baz"], other=[]
            )
            prompter = create_autospec(_prompts.Prompter)
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )

            # When
            resolved_switches = resolver.resolve_log_switches(
                *switches, logs
            )  # type: ignore

            # Then
            assert resolved_switches == {
                WorkflowLogTypeName.PER_TASK: switches[0],
                WorkflowLogTypeName.SYSTEM: switches[1],
                WorkflowLogTypeName.ENV_SETUP: switches[2],
            }
            prompter.choice.assert_not_called()

        @staticmethod
        @pytest.mark.parametrize(
            "switches, expected_switches",
            [
                ((None, None, True), (False, False, True)),
                ((None, True, None), (False, True, False)),
                ((None, True, True), (False, True, True)),
                ((True, None, None), (True, False, False)),
                ((True, None, True), (True, False, True)),
                ((True, True, None), (True, True, False)),
            ],
        )
        def test_only_positive_switches_set(
            switches: t.Tuple[bool, bool, bool],
            expected_switches: t.Tuple[bool, bool, bool],
        ):
            """
            The user has set some, but not all, switches to True, and the log types
            they want are available. The resolver should return the set switches
            unchanged and set the remaining switches to False.
            """
            # Given
            logs = WorkflowLogs(
                per_task={"foo": ["fop"]}, system=["bar"], env_setup=["baz"], other=[]
            )
            prompter = create_autospec(_prompts.Prompter)
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )

            # When
            resolved_switches = resolver.resolve_log_switches(*switches, logs)

            # Then
            assert resolved_switches == {
                WorkflowLogTypeName.PER_TASK: expected_switches[0],
                WorkflowLogTypeName.SYSTEM: expected_switches[1],
                WorkflowLogTypeName.ENV_SETUP: expected_switches[2],
            }
            prompter.choice.assert_not_called()

        @staticmethod
        @pytest.mark.parametrize(
            "switches",
            [
                (None, None, None),
                (None, None, False),
                (None, False, None),
                (None, False, False),
                (False, None, None),
                (False, None, False),
                (False, False, None),
            ],
        )
        def test_none_or_only_negative_switches_set(
            switches: t.Tuple[bool, bool, bool]
        ):
            """
            The user has set some, but not all, switches to False, and all the log
            types are available. The resolver should prompt the user to choose between
            the remaining log types they haven't ruled out.
            """
            # Given
            logs = WorkflowLogs(
                per_task={"foo": ["fop"]}, system=["bar"], env_setup=["baz"], other=[]
            )
            prompter = create_autospec(_prompts.Prompter)
            prompter.choice.return_value = WorkflowLogTypeName.PER_TASK
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )
            valid_choices = [
                (v.value, v)
                for i, v in enumerate(
                    [
                        WorkflowLogTypeName.PER_TASK,
                        WorkflowLogTypeName.SYSTEM,
                        WorkflowLogTypeName.ENV_SETUP,
                    ]
                )
                if switches[i] is None
            ]

            # When
            _ = resolver.resolve_log_switches(*switches, logs)

            # Then
            prompter.choice.assert_called_once_with(
                valid_choices, message="available logs", default="all", allow_all=True
            )

        @staticmethod
        @pytest.mark.parametrize(
            "switches, expected_switches",
            [
                ((True, False, None), (True, False, False)),
                ((False, True, None), (False, True, False)),
                ((True, None, False), (True, False, False)),
                ((False, None, True), (False, False, True)),
                ((None, True, False), (False, True, False)),
                ((None, False, True), (False, False, True)),
            ],
        )
        def test_mixed_switches(
            switches: t.Tuple[bool, bool, bool],
            expected_switches: t.Tuple[bool, bool, bool],
        ):
            """
            The user has set some, but not all, switches, and all the log types are
            available. The resolved should set any unchanged switches to false
            """
            # Given
            logs = WorkflowLogs(
                per_task={"foo": ["fop"]}, system=["bar"], env_setup=["baz"], other=[]
            )
            prompter = create_autospec(_prompts.Prompter)
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )

            # When
            resolved_switches = resolver.resolve_log_switches(*switches, logs)

            # Then
            assert resolved_switches == {
                WorkflowLogTypeName.PER_TASK: expected_switches[0],
                WorkflowLogTypeName.SYSTEM: expected_switches[1],
                WorkflowLogTypeName.ENV_SETUP: expected_switches[2],
            }
            prompter.choice.assert_not_called()

        @staticmethod
        @pytest.mark.parametrize(
            "user_choice, expected_switches",
            [
                (WorkflowLogTypeName.PER_TASK, (True, False, False)),
                (WorkflowLogTypeName.SYSTEM, (False, True, False)),
                (WorkflowLogTypeName.ENV_SETUP, (False, False, True)),
                ("all", (True, True, True)),
            ],
        )
        def test_user_choices(
            user_choice: str, expected_switches: t.Tuple[bool, bool, bool]
        ):
            """
            The user chooses the logs type when prompted. The resolver should set the
            corresponding switch(es) to true and the rest to false.
            """
            # Given
            prompter = create_autospec(_prompts.Prompter)
            logs = WorkflowLogs(
                per_task={"foo": ["fop"]}, system=["bar"], env_setup=["baz"], other=[]
            )
            prompter.choice.return_value = user_choice
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )

            # When
            resolved_switches = resolver.resolve_log_switches(None, None, None, logs)

            # Then
            assert resolved_switches == {
                WorkflowLogTypeName.PER_TASK: expected_switches[0],
                WorkflowLogTypeName.SYSTEM: expected_switches[1],
                WorkflowLogTypeName.ENV_SETUP: expected_switches[2],
            }, resolved_switches
            prompter.choice.assert_called_once_with(
                [
                    (WorkflowLogTypeName.PER_TASK.value, WorkflowLogTypeName.PER_TASK),
                    (WorkflowLogTypeName.SYSTEM.value, WorkflowLogTypeName.SYSTEM),
                    (
                        WorkflowLogTypeName.ENV_SETUP.value,
                        WorkflowLogTypeName.ENV_SETUP,
                    ),
                ],
                message="available logs",
                default="all",
                allow_all=True,
            )

        @staticmethod
        @pytest.mark.parametrize(
            "logs, expected_choices",
            [
                (
                    WorkflowLogs(
                        per_task={"foo": ["fop"]},
                        system=["bar"],
                        env_setup=["baz"],
                        other=[],
                    ),
                    [
                        WorkflowLogTypeName.PER_TASK,
                        WorkflowLogTypeName.SYSTEM,
                        WorkflowLogTypeName.ENV_SETUP,
                    ],
                ),
                (
                    WorkflowLogs(
                        per_task={"foo": ["fop"]},
                        system=["bar"],
                        env_setup=[],
                        other=[],
                    ),
                    [
                        WorkflowLogTypeName.PER_TASK,
                        WorkflowLogTypeName.SYSTEM,
                    ],
                ),
                (
                    WorkflowLogs(
                        per_task={"foo": ["fop"]},
                        system=[],
                        env_setup=["baz"],
                        other=[],
                    ),
                    [WorkflowLogTypeName.PER_TASK, WorkflowLogTypeName.ENV_SETUP],
                ),
                (
                    WorkflowLogs(
                        per_task={"foo": ["fop"]}, system=[], env_setup=[], other=[]
                    ),
                    [WorkflowLogTypeName.PER_TASK],
                ),
                (
                    WorkflowLogs(
                        per_task={}, system=["bar"], env_setup=["baz"], other=[]
                    ),
                    [WorkflowLogTypeName.SYSTEM, WorkflowLogTypeName.ENV_SETUP],
                ),
                (
                    WorkflowLogs(per_task={}, system=["bar"], env_setup=[], other=[]),
                    [WorkflowLogTypeName.SYSTEM],
                ),
                (
                    WorkflowLogs(per_task={}, system=[], env_setup=["baz"], other=[]),
                    [WorkflowLogTypeName.ENV_SETUP],
                ),
                (WorkflowLogs(per_task={}, system=[], env_setup=[], other=[]), []),
            ],
        )
        def test_choices_limited_by_availibility(
            logs, expected_choices: t.List[WorkflowLogTypeName]
        ):
            """
            The user should not be prompted with options that don't have available logs
            to show.
            """
            # Given
            prompter = create_autospec(_prompts.Prompter)
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )
            prompter.choice.return_value = WorkflowLogTypeName.PER_TASK

            # When
            _ = resolver.resolve_log_switches(None, None, None, logs)

            # Then
            prompter.choice.assert_called_once_with(
                [(v.value, v) for v in expected_choices],
                message="available logs",
                default="all",
                allow_all=True,
            )

        @staticmethod
        @pytest.mark.parametrize(
            "logs, switches, expected_choices",
            [
                (
                    WorkflowLogs(
                        per_task={"foo": ["fop"]},
                        system=["bar"],
                        env_setup=[],
                        other=[],
                    ),
                    (False, None, None),
                    [WorkflowLogTypeName.SYSTEM],
                ),
                (
                    WorkflowLogs(
                        per_task={"foo": ["fop"]},
                        system=[],
                        env_setup=["baz"],
                        other=[],
                    ),
                    (None, None, False),
                    [WorkflowLogTypeName.PER_TASK],
                ),
            ],
        )
        def test_choices_limited_by_availibilty_and_negative_switches(
            logs,
            switches: t.Tuple[bool, bool, bool],
            expected_choices: t.List[WorkflowLogTypeName],
        ):
            # Given
            prompter = create_autospec(_prompts.Prompter)
            prompter.choice.return_value = WorkflowLogTypeName.OTHER
            # choice needs to return a valid WorkflowLogTypeName since we enforced this
            # in resolve_log_switches, but we don't actually care _what_ is returned
            # for this test.
            resolver = _arg_resolvers.WFRunResolver(
                wf_run_repo=create_autospec(_repos.WorkflowRunRepo), prompter=prompter
            )

            # When
            _ = resolver.resolve_log_switches(*switches, logs)

            # Then
            prompter.choice.assert_called_once_with(
                [(v.value, v) for v in expected_choices],
                message="available logs",
                default="all",
                allow_all=True,
            )


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


class TestSpacesResolver:
    """
    Test boundaries::
        [SpacesResolver]->[repos]
                        ->[prompter]
    """

    class TestWorkspaceResolver:
        @staticmethod
        def test_passing_workspace_directly():
            """
            User passed `workspace` value directly as CLI arg.
            """
            # Given
            workspace = "<sentinel>"
            config = "<config>"

            resolver = _arg_resolvers.SpacesResolver(
                spaces=create_autospec(_arg_resolvers.SpacesResolver),
                prompter=create_autospec(_prompts.Prompter),
                presenter=Mock(),
            )

            # When
            resolved_workspace = resolver.resolve_workspace_id(
                config=config, workspace_id=workspace
            )

            # Then
            assert resolved_workspace == workspace

        @staticmethod
        def test_no_workspace():
            # Given
            config = "config"
            ws1 = Workspace(workspace_id="id1", name="name1")
            ws2 = Workspace(workspace_id="id2", name="name2")
            workspaces = [ws1, ws2]

            spaces_repo = create_autospec(_repos.SpacesRepo)
            spaces_repo.list_workspaces.return_value = workspaces

            prompter = create_autospec(_prompts.Prompter)
            selected_workspace = workspaces[1]
            prompter.choice.return_value = selected_workspace

            presenter = create_autospec(_presenters.PromptPresenter)
            labels = ["label1", "label2"]
            presenter.workspaces_list_to_prompt.return_value = labels, workspaces
            resolver = _arg_resolvers.SpacesResolver(
                spaces=spaces_repo,
                prompter=prompter,
                presenter=presenter,
            )

            # When
            resolved_workspace = resolver.resolve_workspace_id(
                config=config, workspace_id=None
            )

            # Then
            # We expect prompt for selecting config.
            presenter.workspaces_list_to_prompt.assert_called_with(workspaces, config)
            prompter.choice.assert_called_with(
                [(labels[0], ws1), (labels[1], ws2)], message="Workspace"
            )

            # Resolver should return the user's choice.
            assert resolved_workspace == selected_workspace.workspace_id

    class TestProjectResolver:
        @staticmethod
        def test_passing_project_directly():
            """
            User passed `project` value directly as CLI arg.
            """
            # Given
            workspace = "<sentinel>"
            project = "<project_sentinel>"
            config = "<config>"

            resolver = _arg_resolvers.SpacesResolver(
                spaces=create_autospec(_repos.SpacesRepo),
                prompter=create_autospec(_prompts.Prompter),
                presenter=create_autospec(_presenters.PromptPresenter),
            )

            # When
            resolved_project = resolver.resolve_project_id(
                config=config, workspace_id=workspace, project_id=project
            )

            # Then
            assert resolved_project == project

        @staticmethod
        def test_no_project():
            # Given
            config = "config"
            ws = "workspace"
            p1 = Project(workspace_id="id1", name="name1", project_id="p1")
            p2 = Project(workspace_id="id2", name="name2", project_id="p2")
            projects = [p1, p2]

            spaces_repo = create_autospec(_repos.SpacesRepo)
            spaces_repo.list_projects.return_value = projects

            prompter = create_autospec(_prompts.Prompter)
            selected_project = projects[1]
            prompter.choice.return_value = selected_project

            presenter = create_autospec(_presenters.PromptPresenter)
            labels = ["label1", "label2"]
            presenter.project_list_to_prompt.return_value = (labels, projects)
            resolver = _arg_resolvers.SpacesResolver(
                spaces=spaces_repo,
                prompter=prompter,
                presenter=presenter,
            )

            # When
            resolved_project = resolver.resolve_project_id(
                config=config, workspace_id=ws, project_id=None
            )

            # Then
            # We expect prompt for selecting config.
            presenter.project_list_to_prompt.assert_called_with(projects, config)
            prompter.choice.assert_called_with(
                [(labels[0], p1), (labels[1], p2)], message="Projects"
            )

            # Resolver should return the user's choice.
            assert resolved_project == selected_project.project_id

        @staticmethod
        def test_optional():
            # Given
            config = "config"
            ws = "workspace"
            p1 = Project(workspace_id="id1", name="name1", project_id="p1")
            p2 = Project(workspace_id="id2", name="name2", project_id="p2")
            projects = [p1, p2]

            spaces_repo = create_autospec(_repos.SpacesRepo)
            spaces_repo.list_projects.return_value = projects

            prompter = create_autospec(_prompts.Prompter)
            selected_project = projects[1]
            prompter.choice.return_value = selected_project

            presenter = create_autospec(_presenters.PromptPresenter)
            labels = ["label1", "label2"]
            presenter.project_list_to_prompt.return_value = (labels, projects)
            resolver = _arg_resolvers.SpacesResolver(
                spaces=spaces_repo,
                prompter=prompter,
                presenter=presenter,
            )

            # When
            resolved_project = resolver.resolve_project_id(
                config=config, workspace_id=ws, project_id=None, optional=True
            )

            # Then
            # We expect prompt for selecting config.
            presenter.project_list_to_prompt.assert_called_with(projects, config)
            prompter.choice.assert_called_with(
                [(labels[0], p1), (labels[1], p2), ("All", None)], message="Projects"
            )

            # Resolver should return the user's choice.
            assert resolved_project == selected_project.project_id
