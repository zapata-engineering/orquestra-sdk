################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base.api_cfg._config.
"""

import json
import typing as t
import warnings
from pathlib import Path

import pytest
from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.exceptions import ConfigNameNotFoundError
from orquestra.workflow_shared.schema.configs import RuntimeName

from orquestra.sdk._client._base import _config as api_cfg
from orquestra.sdk._client._base._config import _settings

from ..data.configs import TEST_CONFIG_JSON, TEST_CONFIGS_DICT

VALID_RUNTIME_NAMES: list = ["RAY_LOCAL", "IN_PROCESS", "CE_REMOTE"]
VALID_CONFIG_NAMES: list = ["name_with_underscores", "name with spaces"]


@pytest.fixture()
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


@pytest.fixture
def tmp_default_config_json(patch_config_location):
    json_file = patch_config_location / "config.json"

    with json_file.open("w") as f:
        json.dump(TEST_CONFIG_JSON, f)

    return json_file


@pytest.fixture(autouse=True)
def set_config_location(patch_config_location):
    pass


class TestRuntimeConfiguration:
    class TestInit:
        @staticmethod
        def test_raises_value_error_if_called_directly():
            with pytest.raises(ValueError) as exc_info:
                api_cfg.RuntimeConfig("test_runtime_name", "name")
            assert (
                "Please use the appropriate factory method for your desired runtime."
                in str(exc_info.value)
            )
            assert "`RuntimeConfig.in_process()`" in str(exc_info.value)
            assert "`RuntimeConfig.ray()`" in str(exc_info.value)
            assert "`RuntimeConfig.ce()`" in str(exc_info.value)

        @staticmethod
        def test_raises_exception_for_invalid_config_name():
            with pytest.raises(ValueError) as exc_info:
                api_cfg.RuntimeConfig(
                    "bad_runtime_name", "name", bypass_factory_methods=True
                )
            for valid_name in RuntimeName:
                assert valid_name.value in str(exc_info)

        @staticmethod
        @pytest.mark.parametrize("runtime_name", VALID_RUNTIME_NAMES)
        @pytest.mark.parametrize("config_name", VALID_CONFIG_NAMES)
        def test_assign_custom_name(runtime_name, config_name):
            config = api_cfg.RuntimeConfig(
                runtime_name, name=config_name, bypass_factory_methods=True
            )

            assert config.name == config_name
            assert config._runtime_name == runtime_name

    class TestEq:
        @pytest.fixture
        def config(self):
            config = api_cfg.RuntimeConfig(
                "CE_REMOTE", name="test_config", bypass_factory_methods=True
            )
            setattr(config, "uri", "test_uri")
            setattr(config, "token", "test_token")
            return config

        def test_returns_true_for_matching_configs(self, config):
            test_config = api_cfg.RuntimeConfig(
                config._runtime_name,
                name=config.name,
                bypass_factory_methods=True,
            )
            test_config.uri = config.uri  # type: ignore
            test_config.token = config.token

            assert config == test_config

        @pytest.mark.parametrize(
            "runtime_name, config_name, runtime_options",
            [
                (
                    "CE_REMOTE",
                    "name_mismatch",
                    {"uri": "test_uri", "token": "test_token"},
                ),
                (
                    "RAY_LOCAL",
                    "test_config",
                    {"uri": "test_uri", "token": "test_token"},
                ),
                (
                    "CE_REMOTE",
                    "test_config",
                    {
                        "uri": "test_uri",
                        "token": "test_token",
                        "address": "test_address",
                    },
                ),
            ],
        )
        def test_returns_false_for_mismatched_configs(
            self, config, runtime_name, config_name, runtime_options
        ):
            test_config = api_cfg.RuntimeConfig(
                runtime_name, name=config_name, bypass_factory_methods=True
            )
            for key in runtime_options:
                setattr(test_config, key, runtime_options[key])

            assert config != test_config

        @pytest.mark.parametrize("other", [9, "test_str", {"test_dict": None}])
        def test_returns_false_for_mismatched_type(self, config, other):
            assert config != other

    class TestGetRuntimeOptions:
        @staticmethod
        def test_happy_path():
            config = api_cfg.RuntimeConfig(
                "CE_REMOTE", name="test_config", bypass_factory_methods=True
            )
            config.uri = "test_uri"  # type: ignore
            config.token = "test_token"
            assert config._get_runtime_options() == {
                "uri": "test_uri",
                "token": "test_token",
            }

    class TestFactories:
        class TestInProcessFactory:
            @staticmethod
            def test_with_minimal_args():
                config = api_cfg.RuntimeConfig.in_process()

                assert config.name == "in_process"
                assert config._runtime_name == "IN_PROCESS"

        class TestRayFactory:
            @staticmethod
            def test_with_minimal_args():
                config = api_cfg.RuntimeConfig.ray()

                assert config.name == "local"
                assert config._runtime_name == "RAY_LOCAL"

        class TestRemoteRayFactory:
            @staticmethod
            def test_with_minimal_args():
                config = api_cfg.RuntimeConfig.ce(
                    uri="https://prod-d.orquestra.io/",
                    token="test token",
                )

                name = config.name
                assert name == "prod-d"
                assert config._runtime_name == "CE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"  # type: ignore
                assert config.token == "test token"

    class TestStr:
        @staticmethod
        def test_with_essential_params_only(change_test_dir):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                config = api_cfg.RuntimeConfig(
                    "RAY_LOCAL", "NAME", bypass_factory_methods=True
                )
            assert "RuntimeConfiguration 'NAME' for runtime RAY_LOCAL" in str(config)

        @staticmethod
        def test_with_optional_params():
            config = api_cfg.RuntimeConfig(
                "RAY_LOCAL",
                name="test_name",
                bypass_factory_methods=True,
            )
            config.address = "test_address"  # type: ignore
            config.uri = "test_url"  # type: ignore
            config.token = "blah"

            outstr = str(config)

            for test_str in [
                "RuntimeConfiguration 'test_name'",
                "runtime RAY_LOCAL",
                "with parameters:",
                "- uri: test_url",
                "- token: blah",
                "- address: test_address",
            ]:
                assert test_str in outstr

    class TestListConfigs:
        @staticmethod
        def test_default_file_location(tmp_default_config_json):
            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == [name for name in TEST_CONFIGS_DICT] + list(
                _settings.UNIQUE_CONFIGS
            )
            # this config name should appear only when proper env var is set
            assert _settings.AUTO_CONFIG_NAME not in config_names

        @staticmethod
        def test_custom_file_location(
            tmp_config_json: Path, monkeypatch: pytest.MonkeyPatch
        ):
            monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_config_json))

            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == [name for name in TEST_CONFIGS_DICT] + list(
                _settings.UNIQUE_CONFIGS
            )

        @staticmethod
        def test_empty_configs_key(patch_config_location):
            with open(patch_config_location / "config.json", "w") as f:
                json.dump({"configs": {}}, f)

            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == list(_settings.UNIQUE_CONFIGS)

        @staticmethod
        def test_no_configs_key(patch_config_location):
            with open(patch_config_location / "config.json", "w") as f:
                json.dump({}, f)

            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == list(_settings.UNIQUE_CONFIGS)

        @staticmethod
        def test_auto_config_name(monkeypatch, tmp_config_json):
            monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", "some_file_path")
            config_names = api_cfg.RuntimeConfig.list_configs()

            assert _settings.AUTO_CONFIG_NAME in config_names

    class TestLoad:
        @pytest.mark.parametrize("config_name", [name for name in TEST_CONFIGS_DICT])
        class TestLoadSuccess:
            @staticmethod
            def test_with_default_file_path(tmp_default_config_json, config_name):
                config = api_cfg.RuntimeConfig.load(config_name)

                config_params = TEST_CONFIGS_DICT[config_name]
                assert config.name == config_name
                assert config._runtime_name == config_params["runtime_name"], (
                    f"config '{config_name}' has runtime_name '{config._runtime_name}',"
                    f" but should have config name '{config_params['runtime_name']}'."
                )
                for key in config_params["runtime_options"]:
                    assert getattr(config, key) == config_params["runtime_options"][key]

            @staticmethod
            def test_with_custom_file_path(
                tmp_config_json: Path, config_name: str, monkeypatch: pytest.MonkeyPatch
            ):
                monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_config_json))
                config = api_cfg.RuntimeConfig.load(config_name)

                assert isinstance(TEST_CONFIGS_DICT, t.Mapping)
                config_params = TEST_CONFIGS_DICT[config_name]
                assert config.name == config_name
                assert config._runtime_name == config_params["runtime_name"]
                for key in config_params["runtime_options"]:
                    assert getattr(config, key) == config_params["runtime_options"][key]

        @staticmethod
        def test_invalid_name(tmp_config_json: Path, monkeypatch: pytest.MonkeyPatch):
            monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_config_json))
            with pytest.raises(ConfigNameNotFoundError):
                api_cfg.RuntimeConfig.load(
                    "non-existing",
                )

        class TestAutoConfig:
            class TestRemoteAuto:
                def test_on_cluster(self, monkeypatch, tmp_path):
                    token = "the best token you have ever seen"
                    pass_file = tmp_path / "pass.port"
                    pass_file.write_text(token)
                    monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(pass_file))
                    monkeypatch.setenv("ORQ_CURRENT_CLUSTER", "cluster.io")

                    cfg = api_cfg.RuntimeConfig.load(
                        "auto",
                    )

                    assert cfg.token == token
                    assert getattr(cfg, "uri") == "https://cluster.io"

                def test_on_cluster_with_default_config(self, monkeypatch, tmp_path):
                    # default config does not change behaviour on cluster
                    token = "the best token you have ever seen"
                    pass_file = tmp_path / "pass.port"
                    pass_file.write_text(token)
                    monkeypatch.setenv("ORQ_CURRENT_CONFIG", "actual_name")
                    monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(pass_file))
                    monkeypatch.setenv("ORQ_CURRENT_CLUSTER", "cluster.io")

                    cfg = api_cfg.RuntimeConfig.load(
                        "auto",
                    )

                    assert cfg.token == token
                    assert getattr(cfg, "uri") == "https://cluster.io"

                def test_no_cluster_uri(self, tmp_path, monkeypatch):
                    token = "the best token you have ever seen"
                    pass_file = tmp_path / "pass.port"
                    pass_file.write_text(token)
                    monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(pass_file))
                    with pytest.raises(EnvironmentError):
                        api_cfg.RuntimeConfig.load(
                            "auto",
                        )

                def test_no_file(self, monkeypatch):
                    monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", "non-existing-path")
                    with pytest.raises(FileNotFoundError):
                        api_cfg.RuntimeConfig.load(
                            "auto",
                        )

            class TestLocalAuto:
                def test_on_local_env_default_config(
                    self, monkeypatch, tmp_default_config_json
                ):
                    # given
                    existing_config = "actual_name"
                    monkeypatch.setenv("ORQ_CURRENT_CONFIG", existing_config)

                    # when
                    config = api_cfg.RuntimeConfig.load("auto")

                    # then
                    assert config.name == existing_config
                    assert config.token == "this_token_best_token"
                    assert getattr(config, "uri") == "http://actual_name.domain"

                def test_on_local_env_default_config_set_to_local(self, monkeypatch):
                    # given
                    existing_config = "local"
                    monkeypatch.setenv("ORQ_CURRENT_CONFIG", existing_config)

                    # when
                    config = api_cfg.RuntimeConfig.load("auto")

                    # then
                    assert config == api_cfg.RuntimeConfig.load("local")

                def test_on_local_env_non_existing_default_config(
                    self, monkeypatch, tmp_default_config_json
                ):
                    # given
                    existing_config = "non_existing"
                    monkeypatch.setenv("ORQ_CURRENT_CONFIG", existing_config)

                    # then
                    with pytest.raises(exceptions.RuntimeConfigError):
                        api_cfg.RuntimeConfig.load("auto")

                def test_on_local_env_auto_default_config(self, monkeypatch):
                    # given
                    existing_config = "auto"
                    monkeypatch.setenv("ORQ_CURRENT_CONFIG", existing_config)

                    # then
                    with pytest.raises(exceptions.RuntimeConfigError):
                        api_cfg.RuntimeConfig.load("auto")

                def test_on_local_env_no_default_config(self):
                    with pytest.raises(exceptions.RuntimeConfigError):
                        api_cfg.RuntimeConfig.load("auto")


class TestMigrateConfigFile:
    @staticmethod
    def test_warns():
        with pytest.warns(DeprecationWarning):
            api_cfg.migrate_config_file()


class TestUpdateSavedToken:
    @pytest.mark.parametrize(
        "runtime_factory", [api_cfg.RuntimeConfig.ray, api_cfg.RuntimeConfig.in_process]
    )
    def test_unsupported_runtimes(self, runtime_factory):
        with pytest.raises(SyntaxError):
            cfg = runtime_factory()
            cfg.update_saved_token("new token")

    @pytest.mark.parametrize("runtime_factory", [api_cfg.RuntimeConfig.ce])
    def test_happy_path(self, runtime_factory):
        new_token = "Hi, hello"
        cfg = runtime_factory(uri="https://prod-d.orquestra.io/", token="test token")

        # when
        cfg.update_saved_token(new_token)

        # then
        assert cfg.token == new_token
        assert api_cfg.RuntimeConfig.load(cfg.name).token == new_token

    @pytest.mark.parametrize("runtime_factory", [api_cfg.RuntimeConfig.ce])
    def test_same_token(self, runtime_factory):
        token = "Hi, hello"
        cfg = runtime_factory(uri="https://prod-d.orquestra.io/", token=token)

        # when
        cfg.update_saved_token(token)

        # then
        assert cfg.token == token
        assert api_cfg.RuntimeConfig.load(cfg.name).token == token
