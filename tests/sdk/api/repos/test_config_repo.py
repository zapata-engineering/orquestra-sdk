################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t
from unittest.mock import create_autospec, sentinel

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base import _api
from orquestra.sdk._base._api import repos
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.schema.configs import RuntimeName


def _side_effect_map(args_vals: t.Mapping[t.Tuple, t.Any]):
    """Utility for mocking functions by defining a dictionary spec."""

    def _side_effect(*args):
        try:
            vals = args_vals[args]
        except KeyError as e:
            raise ValueError(f"Unmocked args: {args}") from e
        return vals

    return _side_effect


class TestConfigByNameRepo:
    # TODO
    pass


class TestConfigByIDRepo:
    @staticmethod
    def test_passing_config():
        # Given
        wf_run_id = sentinel.wf_run_id

        config = create_autospec(_api.RuntimeConfig)
        config.name = sentinel.config_name

        # Note: there are two config repos, related by dependency injection.
        config_by_name_repo = create_autospec(repos.ConfigByNameRepo)
        config_by_name_repo.normalize_config.return_value = config

        config_by_id_repo = repos.ConfigByIDRepo(
            config_by_name_repo=config_by_name_repo
        )

        # When
        resolved_config = config_by_id_repo.get_config(wf_run_id, config)

        # Then
        assert resolved_config == config

    class TestNotPassingConfig:
        @staticmethod
        @pytest.fixture
        def config_by_name_repo_mock():
            return create_autospec(repos.ConfigByNameRepo)

        @staticmethod
        @pytest.fixture
        def runtime_repo_mock():
            return create_autospec(repos.RuntimeRepo)

        @staticmethod
        @pytest.fixture
        def config_by_id_repo(config_by_name_repo_mock, runtime_repo_mock):
            return repos.ConfigByIDRepo(
                config_by_name_repo=config_by_name_repo_mock,
                runtime_repo=runtime_repo_mock,
            )

        @staticmethod
        def _make_config_mock(name_suffix: str = ""):
            config_name = getattr(sentinel, f"config_name{name_suffix}")
            config_obj = create_autospec(
                _api.RuntimeConfig, name=f"config_obj{name_suffix}"
            )
            config_obj.name = config_name
            return config_obj

        @classmethod
        def _set_up_config_mocks(
            cls,
            monkeypatch,
            configs: t.Sequence[_api.RuntimeConfig],
        ):
            monkeypatch.setattr(
                _api.RuntimeConfig,
                "list_configs",
                create_autospec(
                    _api.RuntimeConfig.list_configs,
                    return_value=[config.name for config in configs],
                ),
            )

            monkeypatch.setattr(
                _api.RuntimeConfig,
                "load",
                create_autospec(
                    _api.RuntimeConfig.load,
                    side_effect=_side_effect_map(
                        {(config.name,): config for config in configs}
                    ),
                ),
            )

        def test_run_exists_in_first(
            self,
            monkeypatch,
            config_by_id_repo: repos.ConfigByIDRepo,
            runtime_repo_mock,
        ):
            """We ask 1 runtime. Workflow run exists in it."""
            # Given
            wf_run_id = sentinel.wf_run_id
            resolved_config = self._make_config_mock()
            self._set_up_config_mocks(monkeypatch, [resolved_config])

            runtime = create_autospec(RuntimeInterface)
            runtime.get_workflow_run_status.return_value = sentinel.wf_run_model
            runtime_repo_mock.get_runtime.return_value = runtime

            # When
            result = config_by_id_repo.get_config(wf_run_id=wf_run_id, config=None)

            # Then
            assert result == resolved_config

        def test_not_found_and_found(
            self,
            monkeypatch,
            config_by_id_repo: repos.ConfigByIDRepo,
            runtime_repo_mock,
        ):
            """Runtime 1: not found, runtime 2: workflow run exists."""
            wf_run_id = sentinel.wf_run_id
            config1 = self._make_config_mock("1")
            config2 = self._make_config_mock("2")
            self._set_up_config_mocks(monkeypatch, [config1, config2])

            runtime1 = create_autospec(RuntimeInterface)
            runtime2 = create_autospec(RuntimeInterface)
            runtime_repo_mock.get_runtime.side_effect = _side_effect_map(
                {
                    (config1,): runtime1,
                    (config2,): runtime2,
                }
            )

            runtime1.get_workflow_run_status.side_effect = (
                exceptions.WorkflowRunNotFoundError
            )
            runtime2.get_workflow_run_status.return_value = sentinel.wf_run_model

            # When
            result = config_by_id_repo.get_config(wf_run_id=wf_run_id, config=None)

            # Then
            assert result == config2

        def test_no_runtimes_know_this_id(
            self,
            monkeypatch,
            config_by_id_repo: repos.ConfigByIDRepo,
            runtime_repo_mock,
        ):
            """We ask 1 runtime. It doesn't know about about the workflow run."""
            wf_run_id = sentinel.wf_run_id
            config = self._make_config_mock(name_suffix="1")
            config.runtime_name = RuntimeName.RAY_LOCAL
            self._set_up_config_mocks(monkeypatch, [config])

            runtime = create_autospec(RuntimeInterface)
            runtime.get_workflow_run_status.side_effect = (
                exceptions.WorkflowRunNotFoundError
            )
            runtime_repo_mock.get_runtime.return_value = runtime

            with pytest.raises(exceptions.RuntimeQuerySummaryError) as exc_info:
                # When
                _ = config_by_id_repo.get_config(wf_run_id=wf_run_id, config=None)

            # Then
            assert exc_info.value.wf_run_id == wf_run_id
            assert exc_info.value.not_found_runtimes == [
                exceptions.RuntimeQuerySummaryError.RuntimeInfo(
                    runtime_name=config.runtime_name,
                    config_name=config.name,
                    server_uri=None,
                )
            ]
            assert exc_info.value.unauthorized_runtimes == []

        def test_unauthorized_and_found(
            self,
            monkeypatch,
            config_by_id_repo: repos.ConfigByIDRepo,
            runtime_repo_mock,
        ):
            """Runtime 1: unauthorized, runtime 2: workflow run exists."""
            wf_run_id = sentinel.wf_run_id
            config1 = self._make_config_mock(name_suffix="1")
            config2 = self._make_config_mock(name_suffix="2")

            self._set_up_config_mocks(monkeypatch, [config1, config2])

            runtime1 = create_autospec(RuntimeInterface)
            runtime2 = create_autospec(RuntimeInterface)
            runtime_repo_mock.get_runtime.side_effect = _side_effect_map(
                {
                    (config1,): runtime1,
                    (config2,): runtime2,
                }
            )
            runtime1.get_workflow_run_status.side_effect = exceptions.UnauthorizedError
            runtime2.get_workflow_run_status.return_value = sentinel.wf_run_model

            # When
            result = config_by_id_repo.get_config(wf_run_id=wf_run_id, config=None)

            # Then
            assert result == config2

        def test_all_remotes_unauthorized(
            self,
            monkeypatch,
            config_by_id_repo: repos.ConfigByIDRepo,
            runtime_repo_mock,
        ):
            wf_run_id = sentinel.wf_run_id

            config_obj = self._make_config_mock(name_suffix="1")
            config_obj.uri = sentinel.uri1
            config_obj.runtime_name = RuntimeName.CE_REMOTE

            self._set_up_config_mocks(monkeypatch, [config_obj])

            runtime = create_autospec(RuntimeInterface)
            runtime.get_workflow_run_status.side_effect = exceptions.UnauthorizedError
            runtime_repo_mock.get_runtime.return_value = runtime

            with pytest.raises(exceptions.RuntimeQuerySummaryError) as exc_info:
                # When
                _ = config_by_id_repo.get_config(wf_run_id=wf_run_id, config=None)

            # Then
            assert exc_info.value.wf_run_id == wf_run_id
            assert exc_info.value.not_found_runtimes == []
            assert exc_info.value.unauthorized_runtimes == [
                exceptions.RuntimeQuerySummaryError.RuntimeInfo(
                    runtime_name=config_obj.runtime_name,
                    config_name=config_obj.name,
                    server_uri=config_obj.uri,
                )
            ]

        def test_ray_cluster_not_running(
            self,
            monkeypatch,
            config_by_id_repo: repos.ConfigByIDRepo,
            runtime_repo_mock,
        ):
            """Runtime 1: Ray not running, runtime 2: workflow run exists."""
            wf_run_id = sentinel.wf_run_id

            config1 = self._make_config_mock(name_suffix="1")
            config2 = self._make_config_mock(name_suffix="2")
            self._set_up_config_mocks(monkeypatch, [config1, config2])

            runtime2 = create_autospec(RuntimeInterface)

            def mock_get_runtime(config):
                if config == config1:
                    # Ray connection is established when the runtime object is created.
                    # If the cluster isn't running, we'll get the error here.
                    raise exceptions.RayNotRunningError()
                elif config == config2:
                    return runtime2

            runtime_repo_mock.get_runtime.side_effect = mock_get_runtime

            runtime2.get_workflow_run_status.return_value = sentinel.wf_run_model

            # When
            result = config_by_id_repo.get_config(wf_run_id=wf_run_id, config=None)

            # Then
            assert result == config2
