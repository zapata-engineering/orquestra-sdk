################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base.api_cfg._config.
"""

import json
import typing as t
import warnings
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from orquestra.sdk._base import _config
from orquestra.sdk._base._api import _config as api_cfg
from orquestra.sdk.exceptions import ConfigNameNotFoundError
from orquestra.sdk.schema.configs import CONFIG_FILE_CURRENT_VERSION, RuntimeName

from ..data.configs import TEST_CONFIG_JSON, TEST_CONFIGS_DICT

VALID_RUNTIME_NAMES: list = ["RAY_LOCAL", "QE_REMOTE", "IN_PROCESS", "CE_REMOTE"]
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
                api_cfg.RuntimeConfig("test_runtime_name")
            assert (
                "Please use the appropriate factory method for your desired runtime."
                in str(exc_info.value)
            )
            assert "`RuntimeConfig.in_process()`" in str(exc_info.value)
            assert "`RuntimeConfig.qe()`" in str(exc_info.value)
            assert "`RuntimeConfig.ray()`" in str(exc_info.value)
            assert "`RuntimeConfig.ce()`" in str(exc_info.value)

        @staticmethod
        def test_raises_exception_for_invalid_config_name():
            with pytest.raises(ValueError) as exc_info:
                api_cfg.RuntimeConfig("bad_runtime_name", bypass_factory_methods=True)
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
                "QE_REMOTE", name="test_config", bypass_factory_methods=True
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
                    "QE_REMOTE",
                    "name_mismatch",
                    {"uri": "test_uri", "token": "test_token"},
                ),
                (
                    "RAY_LOCAL",
                    "test_config",
                    {"uri": "test_uri", "token": "test_token"},
                ),
                (
                    "QE_REMOTE",
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

    @pytest.mark.parametrize("runtime_name", VALID_RUNTIME_NAMES)
    class TestNameProperty:
        @staticmethod
        @pytest.mark.parametrize("config_name", VALID_CONFIG_NAMES)
        def test_happy_path(config_name, runtime_name):
            """
            This test looks nonsensical, but is intended to test the custom getter and
            setter defined for the name property.
            """
            config = api_cfg.RuntimeConfig(
                runtime_name, name=config_name, bypass_factory_methods=True
            )

            config.name = config_name
            assert config_name == config_name

        @staticmethod
        def test_getter_raises_warning_if_name_is_not_set(runtime_name):
            config = api_cfg.RuntimeConfig(runtime_name, bypass_factory_methods=True)

            telltale_string = (
                "You are trying to access the name of a RuntimeConfig instance that "
                "has not been named."
            )
            with pytest.warns(UserWarning, match=telltale_string):
                name = config.name

            assert name is None

    class TestGetRuntimeOptions:
        @staticmethod
        def test_happy_path():
            config = api_cfg.RuntimeConfig(
                "QE_REMOTE", name="test_config", bypass_factory_methods=True
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

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = api_cfg.RuntimeConfig.in_process(name="test config")

                assert str(config.name) == "in_process"
                assert config._runtime_name == "IN_PROCESS"

        class TestRayFactory:
            @staticmethod
            def test_with_minimal_args():
                config = api_cfg.RuntimeConfig.ray()

                assert config.name == "local"
                assert config._runtime_name == "RAY_LOCAL"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = api_cfg.RuntimeConfig.ray(name="test config")

                assert config.name == "local"
                assert config._runtime_name == "RAY_LOCAL"

        class TestQeFactory:
            @staticmethod
            def test_with_minimal_args():
                config = api_cfg.RuntimeConfig.qe(
                    uri="https://prod-d.orquestra.io/",
                    token="test token",
                )

                name = config.name
                assert name == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"  # type: ignore
                assert config.token == "test token"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = api_cfg.RuntimeConfig.qe(
                        name="test config",
                        uri="https://prod-d.orquestra.io/",
                        token="test token",
                    )

                assert str(config.name) == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"  # type: ignore
                assert config.token == "test token"

        class TestRemoteRayFactory:
            @staticmethod
            def test_with_minimal_args():
                config = api_cfg.RuntimeConfig.qe(
                    uri="https://prod-d.orquestra.io/",
                    token="test token",
                )

                name = config.name
                assert name == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"  # type: ignore
                assert config.token == "test token"

            @staticmethod
            def test_with_maximal_args(tmp_path):
                with pytest.warns(FutureWarning):
                    config = api_cfg.RuntimeConfig.qe(
                        name="test config",
                        uri="https://prod-d.orquestra.io/",
                        token="test token",
                    )

                assert str(config.name) == "prod-d"
                assert config._runtime_name == "QE_REMOTE"
                assert config.uri == "https://prod-d.orquestra.io/"  # type: ignore
                assert config.token == "test token"

    class TestStr:
        @staticmethod
        def test_with_essential_params_only(change_test_dir):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                config = api_cfg.RuntimeConfig("RAY_LOCAL", bypass_factory_methods=True)
            assert "RuntimeConfiguration 'None' for runtime RAY_LOCAL" in str(config)

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
                _config.UNIQUE_CONFIGS
            )

        @staticmethod
        def test_custom_file_location(
            tmp_config_json: Path, monkeypatch: pytest.MonkeyPatch
        ):
            monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_config_json))

            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == [name for name in TEST_CONFIGS_DICT] + list(
                _config.UNIQUE_CONFIGS
            )

        @staticmethod
        def test_empty_configs_key(patch_config_location):
            with open(patch_config_location / "config.json", "w") as f:
                json.dump({"configs": {}}, f)

            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == list(_config.UNIQUE_CONFIGS)

        @staticmethod
        def test_no_configs_key(patch_config_location):
            with open(patch_config_location / "config.json", "w") as f:
                json.dump({}, f)

            config_names = api_cfg.RuntimeConfig.list_configs()

            assert config_names == list(_config.UNIQUE_CONFIGS)

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

    class TestLoadDefault:
        @staticmethod
        def test_with_default_file_path(tmp_default_config_json):
            config = api_cfg.RuntimeConfig.load_default()

            default_config_params = TEST_CONFIGS_DICT[
                TEST_CONFIG_JSON["default_config_name"]
            ]
            assert config.name == default_config_params["config_name"]
            assert config._runtime_name == default_config_params["runtime_name"]

            config_uri = config.uri  # type: ignore
            assert config_uri == default_config_params["runtime_options"]["uri"]

            assert config.token == default_config_params["runtime_options"]["token"]

        @staticmethod
        def test_with_custom_file_path(tmp_config_json, monkeypatch):
            monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_config_json))
            config = api_cfg.RuntimeConfig.load_default()

            default_config_params = TEST_CONFIGS_DICT[
                TEST_CONFIG_JSON["default_config_name"]
            ]
            assert config.name == default_config_params["config_name"]
            assert config._runtime_name == default_config_params["runtime_name"]

            config_uri = config.uri  # type: ignore
            assert config_uri == default_config_params["runtime_options"]["uri"]

            assert config.token == default_config_params["runtime_options"]["token"]

    class TestIsSaved:
        @staticmethod
        def test_returns_true_if_saved(tmp_default_config_json):
            config = api_cfg.RuntimeConfig.load("test_config_default")
            assert config.is_saved()

        @staticmethod
        def test_returns_false_if_unnamed(tmp_default_config_json):
            config = api_cfg.RuntimeConfig.load("test_config_default")
            config._name = None
            assert not config.is_saved()

        @staticmethod
        def test_returns_false_if_no_previous_save_file(tmp_default_config_json):
            config = api_cfg.RuntimeConfig.load("test_config_default")
            config._config_save_file = None
            assert not config.is_saved()

        @staticmethod
        def test_returns_false_if_no_file(monkeypatch: pytest.MonkeyPatch):
            monkeypatch.setenv("ORQ_CONFIG_PATH", "not_a_valid_file")
            config = api_cfg.RuntimeConfig(
                "IN_PROCESS",
                name="test_name",
                bypass_factory_methods=True,
            )
            assert not config.is_saved()

        @staticmethod
        def test_returns_false_if_unsaved_changes(tmp_default_config_json):
            config = api_cfg.RuntimeConfig.load("test_config_default")
            config.name = "new_name"
            assert not config.is_saved()

        @staticmethod
        @pytest.mark.parametrize("config_name", ["local", "in_process"])
        def test_returns_true_if_reserved(config_name, tmp_default_config_json):
            config = api_cfg.RuntimeConfig.load(config_name)
            assert config.is_saved()

    class TestAsDict:
        @staticmethod
        def test_with_no_runtime_options():
            config = api_cfg.RuntimeConfig("IN_PROCESS", bypass_factory_methods=True)

            dict = config._as_dict()

            assert dict["config_name"] == "None"
            assert dict["runtime_name"] == "IN_PROCESS"
            assert dict["runtime_options"] == {}

        @staticmethod
        def test_with_all_runtime_options():
            config = api_cfg.RuntimeConfig("IN_PROCESS", bypass_factory_methods=True)
            config.uri = "test_uri"  # type: ignore
            config.address = "test_address"  # type: ignore
            config.token = "test_token"

            dict = config._as_dict()

            assert dict["config_name"] == "None"
            assert dict["runtime_name"] == "IN_PROCESS"
            assert dict["runtime_options"]["uri"] == config.uri  # type: ignore
            assert dict["runtime_options"]["address"] == config.address  # type: ignore
            assert dict["runtime_options"]["token"] == config.token


@pytest.mark.parametrize(
    "input_config_file, expected_output_config_file, expected_stdout",
    [
        (  # No changes required
            {
                "configs": {
                    "single_config_no_changes": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "single_config_no_changes": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            ["No changes required for file"],
        ),
        (  # 2 config files, only one of which needs changing"
            {
                "configs": {
                    "2_configs_1_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    },
                    "not_this_one": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "2_configs_1_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    },
                    "not_this_one": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - 2_configs_1_needs_changing",  # NOQA E501
            ],
        ),
        (  # 1 config that needs changing, has additional fields that shouldn't change.
            {
                "configs": {
                    "single_config_with_additional_fields": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"blah": "blah_val"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "single_config_with_additional_fields": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "blah": "blah_val",
                            "temp_dir": None,
                        },
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - single_config_with_additional_fields",  # NOQA E501
            ],
        ),
        (  # multiple configs, all need updating
            {
                "configs": {
                    "multiple_configs_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    },
                    "this_one_too": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "blah": "other_blah_val",
                        },
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "multiple_configs_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "temp_dir": None,
                        },
                    },
                    "this_one_too": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "blah": "other_blah_val",
                            "temp_dir": None,
                        },
                    },
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 2 entries:\n - multiple_configs_need_updating\n - this_one_too",  # NOQA E501
            ],
        ),
        (  # Mix of QE and Ray configs - only ray should be updated.
            {
                "configs": {
                    "mix_of_QE_and_RAY": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    },
                    "update_me": {"runtime_name": "RAY_LOCAL", "runtime_options": {}},
                    "but_not_me": {"runtime_name": "QE_REMOTE", "runtime_options": {}},
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            {
                "configs": {
                    "mix_of_QE_and_RAY": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "temp_dir": None,
                        },
                    },
                    "update_me": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {
                            "temp_dir": None,
                        },
                    },
                    "but_not_me": {"runtime_name": "QE_REMOTE", "runtime_options": {}},
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 2 entries:\n - mix_of_QE_and_RAY\n - update_me",  # NOQA E501
            ],
        ),
        (  # version alone needs updating
            {
                "configs": {
                    "version_alone_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": "0.0.0",
            },
            {
                "configs": {
                    "version_alone_needs_changing": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": "test_temp_dir"},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 0 entries.",
            ],
        ),
        (  # version and configs need updating
            {
                "configs": {
                    "version_and_config_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {},
                    }
                },
                "version": "0.0.0",
            },
            {
                "configs": {
                    "version_and_config_need_updating": {
                        "runtime_name": "RAY_LOCAL",
                        "runtime_options": {"temp_dir": None},
                    }
                },
                "version": CONFIG_FILE_CURRENT_VERSION,
            },
            [
                "Successfully migrated file ",
                f" to version {CONFIG_FILE_CURRENT_VERSION}. Updated 1 entry:\n - version_and_config_need_updating",  # NOQA E501
            ],
        ),
    ],
)
class TestMigrateConfigFile:
    @staticmethod
    def test_for_default_file(
        input_config_file, expected_output_config_file, expected_stdout, capsys
    ):
        with patch(
            "builtins.open",
            mock_open(read_data=json.dumps(input_config_file)),
        ) as m:
            api_cfg.migrate_config_file()

        if input_config_file == expected_output_config_file:
            m().write.assert_not_called()
        else:
            m().write.assert_called_once_with(
                json.dumps(expected_output_config_file, indent=2)
            )
        captured = capsys.readouterr()
        for string in expected_stdout:
            assert string in captured.out

    @staticmethod
    def test_for_single_custom_file(
        input_config_file,
        expected_output_config_file,
        expected_stdout,
        capsys,
        tmp_path,
        monkeypatch,
    ):
        config_file = tmp_path / "test_configs.json"
        monkeypatch.setenv("ORQ_CONFIG_PATH", str(config_file))
        with open(config_file, "w") as f:
            json.dump(input_config_file, f, indent=2)

        api_cfg.migrate_config_file()

        with open(config_file, "r") as f:
            data = json.load(f)

        assert data == expected_output_config_file
        captured = capsys.readouterr()
        for string in expected_stdout:
            assert string in captured.out
