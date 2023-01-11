################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for repos. Isolated unit tests unless explicitly named as integration.
"""

import json
import sys
import warnings
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra import sdk
from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _db, _factory
from orquestra.sdk._base._testing import _example_wfs
from orquestra.sdk._base.cli._dorq import _repos
from orquestra.sdk._ray import _dag

from ... import reloaders
from ...sdk.v2.data.configs import TEST_CONFIG_JSON


class TestWorkflowRunRepo:
    class TestIsolation:
        """
        Isolated unit tests for WorkflowRunRepo.

        Test boundary::

            [WorkflowRunRepo]->[WorkflowDB]
                             ->[sdk.WorkflowRun]
                             ->[sdk.WorkflowDef]
        """

        @staticmethod
        @pytest.fixture
        def db_mock(monkeypatch):
            """
            Mock object suitable for stubbing 'with WorkflowDB.open_db() as db'
            """
            db = Mock()

            ctx_manager = Mock()
            ctx_manager().__enter__ = Mock(return_value=db)
            ctx_manager().__exit__ = Mock()

            monkeypatch.setattr(_db.WorkflowDB, "open_db", ctx_manager)

            return db

        @staticmethod
        def test_get_config_name_by_run_id(db_mock):
            # Given
            config = "test_cfg"
            db_mock.get_workflow_run().config_name = config

            repo = _repos.WorkflowRunRepo()
            wf_run_id = "wf.1"

            # When
            result_config = repo.get_config_name_by_run_id(wf_run_id)

            # Then
            assert result_config == config
            db_mock.get_workflow_run.assert_called_with(workflow_run_id=wf_run_id)

        @staticmethod
        def test_get_wf_by_run_id(monkeypatch):
            # Given
            run_id = "wf.1"
            config_name = "<config sentinel>"

            wf_run = Mock()
            wf_run.get_status_model().id = run_id

            by_id = Mock(return_value=wf_run)
            monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

            repo = _repos.WorkflowRunRepo()

            # When
            wf_run = repo.get_wf_by_run_id(run_id, config_name)

            # Then
            assert wf_run.id == run_id
            by_id.assert_called_with(run_id, config_name)

        class TestListWFRunIDs:
            """
            Boundaries::

                [WorkflowRunRepo]->[RayRuntime]
                                 ->[_config]
                                 ->[_factory]
            """

            @staticmethod
            @pytest.mark.parametrize(
                "exc", [ConnectionError(), exceptions.UnauthorizedError()]
            )
            def test_passing_errors(monkeypatch, exc):
                # Given
                config = "<config sentinel>"

                # Prevent FS access
                monkeypatch.setattr(_config, "read_config", Mock())

                runtime = Mock()
                runtime.get_all_workflow_runs_status.side_effect = exc
                monkeypatch.setattr(
                    _factory, "build_runtime_from_config", Mock(return_value=runtime)
                )

                repo = _repos.WorkflowRunRepo()

                # Then
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.list_wf_run_ids(config)

        class TestSubmit:
            @staticmethod
            def test_passes_config_and_id():
                # Given
                repo = _repos.WorkflowRunRepo()

                config = "test_cfg"

                run_id = "wf.2"
                wf_def = Mock()
                wf_def.run().run_id = run_id

                # When
                result_id = repo.submit(wf_def, config, ignore_dirty_repo=True)

                # Then
                assert result_id == run_id
                wf_def.run.assert_called_with(config)

            class TestWithDirtyRepo:
                @staticmethod
                @pytest.fixture
                def wf_def():
                    run_id = "wf.2"
                    wf_def = Mock()

                    def _fake_run_method(*args, **kwargs):
                        warnings.warn(
                            "You have uncommitted changes", exceptions.DirtyGitRepo
                        )

                        wf_run = Mock()
                        wf_run.run_id = run_id
                        return wf_run

                    wf_def.run = _fake_run_method

                    return wf_def

                @staticmethod
                def test_raises_exception(wf_def):
                    # Given
                    repo = _repos.WorkflowRunRepo()
                    config = "test_cfg"

                    # When + Then
                    with pytest.raises(exceptions.DirtyGitRepo):
                        _ = repo.submit(wf_def, config, ignore_dirty_repo=False)

                @staticmethod
                def test_warns(wf_def):
                    # Given
                    repo = _repos.WorkflowRunRepo()
                    config = "test_cfg"

                    # When + Then
                    with pytest.warns(exceptions.DirtyGitRepo):
                        _ = repo.submit(wf_def, config, ignore_dirty_repo=True)

        class TestStop:
            @staticmethod
            @pytest.mark.parametrize(
                "exc",
                [
                    exceptions.UnauthorizedError(),
                    exceptions.WorkflowRunCanNotBeTerminated(),
                ],
            )
            def test_passing_data(monkeypatch, exc):
                # Given
                run_id = "wf.1"
                config_name = "<config sentinel>"

                wf_run = Mock()
                wf_run.stop.side_effect = exc

                by_id = Mock(return_value=wf_run)
                monkeypatch.setattr(sdk.WorkflowRun, "by_id", by_id)

                repo = _repos.WorkflowRunRepo()

                # Then
                # Validate passing exception
                with pytest.raises(type(exc)):
                    # When
                    _ = repo.stop(run_id, config_name)

                # Then
                # Validate passing args
                by_id.assert_called_with(run_id, config_name)

    class TestIntegration:
        @staticmethod
        def test_list_wf_run_ids(monkeypatch):
            """
            Test boundary::

                [WorkflowRunRepo]->[RayRuntime]

            Validates that we're using runtimes factory correctly.
            """

            # Given
            config = "ray"

            stub_run_ids = ["wf.1", "wf.2"]

            # Make RayRuntime return the IDs we want. We don't want to submit real
            # workflows and wait for their completion because it takes forever. It's
            # tested already by RayRuntime-specific tests.
            mock_wf_runs = []
            for stub_id in stub_run_ids:
                wf_run = Mock()
                wf_run.id = stub_id
                mock_wf_runs.append(wf_run)

            monkeypatch.setattr(
                _dag.RayRuntime,
                "get_all_workflow_runs_status",
                Mock(return_value=mock_wf_runs),
            )

            # Prevent RayRuntime from connecting to a real cluster.
            monkeypatch.setattr(_dag.RayRuntime, "startup", Mock())

            repo = _repos.WorkflowRunRepo()

            # When
            run_ids = repo.list_wf_run_ids(config)

            # Then
            assert run_ids == stub_run_ids


class TestConfigRepo:
    class TestIntegration:
        """
        We test ConfigRepo by integration because as I'm writing this there are a lot
        of moving parts related to config rework.

        Test boundary::
            [ConfigRepo]->File system

        Mocks config file location.
        """

        # TODO: switch this to unit tests with a boundary at
        # `sdk.RuntimeConfig.list_configs()` after the config reword is done.
        # See ticket: https://zapatacomputing.atlassian.net/browse/ORQSDK-674

        @staticmethod
        @pytest.fixture
        def config_content():
            return TEST_CONFIG_JSON

        @staticmethod
        def test_returns_usable_configs(tmp_path: Path, monkeypatch, config_content):
            """
            Verifies that the output is a list that makes sense for the user to select
            the config value from.
            """
            # Given
            monkeypatch.setattr(Path, "home", Mock(return_value=tmp_path))

            config_path = tmp_path / ".orquestra" / "config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(config_content))

            repo = _repos.ConfigRepo()

            # When
            names = repo.list_config_names()

            # Then
            assert set(names) == {
                # built-ins
                "ray",
                "in_process",
                "local",
                # config entries
                "test_config_default",
                "test_config_no_runtime_options",
                "test_config_qe",
            }


class TestResolveDottedName:
    """
    Unit tests for the heuristic for module name resolution.
    """

    @staticmethod
    @pytest.mark.parametrize(
        "spec,dotted_name",
        [
            ("foo", "foo"),
            ("foo.bar", "foo.bar"),
            ("foo.py", "foo"),
            (str(Path("foo") / "bar.py"), "foo.bar"),
            (str(Path("src") / "foo" / "bar.py"), "foo.bar"),
        ],
    )
    def test_examples(spec: str, dotted_name: str):
        assert _repos.resolve_dotted_name(spec) == dotted_name


class TestWorkflowDefRepoIntegration:
    """
    Integration tests for WorkflowDefRepo.

    Test boundary::

        [real testing module]->[WorkflowDefRepo]
    """

    @staticmethod
    @pytest.fixture
    def tmp_packages_site(tmp_path):
        """
        Prepares a directory for importing Python modules and cleans up the
        module cache afterwards.
        """
        with reloaders.restore_loaded_modules():
            sys.path.insert(0, str(tmp_path))

            yield tmp_path

    class TestGetModuleFromSpec:
        class TestDottedName:
            """
            Validates that we can pass 'dotted.module.name' and it loads the file
            appropriately.
            """

            @staticmethod
            def test_loads_top_level_module(tmp_packages_site: Path):
                # Given
                module_path = tmp_packages_site / "my_module.py"
                module_path.write_text("foo = 'abc'")

                repo = _repos.WorkflowDefRepo()

                # When
                mod = repo.get_module_from_spec("my_module")

                # Then
                assert mod.foo == "abc"

            @staticmethod
            def test_loads_submodules(tmp_packages_site: Path):
                # Given
                module_path = tmp_packages_site / "my_pkg" / "my_module.py"
                module_path.parent.mkdir(parents=True)
                module_path.write_text("foo = 'abc'")

                repo = _repos.WorkflowDefRepo()

                # When
                mod = repo.get_module_from_spec("my_pkg.my_module")

                # Then
                assert mod.foo == "abc"

        class TestNonExistingModules:
            @staticmethod
            def test_invalid_path():
                repo = _repos.WorkflowDefRepo()

                with pytest.raises(exceptions.WorkflowDefinitionModuleNotFound):
                    _ = repo.get_module_from_spec("doesnt_exist.py")

            @staticmethod
            def test_invalid_module():
                repo = _repos.WorkflowDefRepo()

                with pytest.raises(exceptions.WorkflowDefinitionModuleNotFound):
                    _ = repo.get_module_from_spec("doesnt_exist")

    class TestGetWorkflowNames:
        @staticmethod
        def test_examples_module():
            # Given
            repo = _repos.WorkflowDefRepo()

            # When
            names = repo.get_worklow_names(_example_wfs)

            # Then
            assert names == [
                "greet_wf",
                "greet_wf_kw",
                "complicated_wf",
                "multioutput_wf",
                "multioutput_task_wf",
                "my_workflow",
                "exception_wf",
                "wf_using_inline_imports",
                "wf_using_python_imports",
                "serial_wf_with_slow_middle_task",
                "serial_wf_with_file_triggers",
                "exception_wf_with_multiple_values",
                "wf_with_log",
                "wf_with_exec_ctx",
                "parametrized_wf",
            ]

        @staticmethod
        def test_empty_module(tmp_packages_site):
            # Given
            module_path = tmp_packages_site / "my_module.py"
            module_path.write_text("foo = 'abc'")
            repo = _repos.WorkflowDefRepo()
            module = repo.get_module_from_spec("my_module")

            # Then
            with pytest.raises(exceptions.NoWorkflowDefinitionsFound):
                # When
                _ = repo.get_worklow_names(module)

    class TestGetWorkflowDef:
        @staticmethod
        def test_standard_workflow():
            # Given
            repo = _repos.WorkflowDefRepo()
            wf_name = "greet_wf"

            # When
            wf_def = repo.get_workflow_def(_example_wfs, wf_name)

            # Then
            assert isinstance(wf_def, sdk.WorkflowDef)
            assert wf_def._name == wf_name

        @staticmethod
        def test_parametrized_workflow():
            # Given
            repo = _repos.WorkflowDefRepo()
            wf_name = "parametrized_wf"

            # Then
            with pytest.raises(exceptions.WorkflowSyntaxError):
                # When
                _ = repo.get_workflow_def(_example_wfs, wf_name)
