################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
"""
Unit tests for config IO. Before editing this file, please consider writing the
CLI usage scenario in tests/cli/test_cli_with_qe.py first. Motivation below.

Due to complexity of handling configs we've seen that usually it's difficult to
figure out the expected behavior for unit tests. We've had multiple situations
where:
- We work on a bug fix related to configs
- The bug scenario was fixed. Some other unit tests fail. We change the other
  tests to pass.
- We're introducing another bug in the process.

The current solution for this is to focus on testing the layer that's close to
the user. It's a lot easier to figure out appropriate behavior this way.
"""
from pathlib import Path

import filelock
import orquestra.workflow_shared.exceptions as exceptions
import pytest
from orquestra.workflow_shared.schema.configs import RuntimeName

import orquestra.sdk._client._base._config._fs as _config
from orquestra.sdk._client._base._config._settings import LOCK_FILE_NAME

from ..data.configs import TEST_CONFIG_JSON


class TestSaveOrUpdate:
    @staticmethod
    @pytest.mark.parametrize("config_name", ["in_process", "ray"])
    def test_throws_when_special_config_used(config_name):
        with pytest.raises(ValueError):
            _config.save_or_update(config_name, RuntimeName.IN_PROCESS, {})


class TestResolveConfigFile:
    @staticmethod
    def test_returns_none_for_nonexistant_file_default_location(patch_config_location):
        assert _config._resolve_config_file() is None

    @staticmethod
    def test_returns_none_for_nonexistant_file_custom_location(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_path / "test.file"))
        assert _config._resolve_config_file() is None


class TestGenerateConfigName:
    @staticmethod
    def test_raises_exception_if_ce_but_no_uri():
        with pytest.raises(AttributeError) as exc_info:
            _config.generate_config_name(RuntimeName.CE_REMOTE, None)
        assert "CE runtime configurations must have a 'URI' value set." in str(exc_info)


class TestValidateRuntimeOptions:
    class TestPassesForGoodOptions:
        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {},
                {"address": "foo"},
                {"log_to_driver": "foo"},
                {"storage": "foo"},
                {"temp_dir": "foo"},
                {
                    "address": "foo",
                    "log_to_driver": "foo",
                    "storage": "foo",
                    "temp_dir": "foo",
                },
            ],
        )
        def test_good_ray_options(runtime_options):
            assert (
                _config._validate_runtime_options(
                    RuntimeName.RAY_LOCAL, runtime_options
                )
                == runtime_options
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {},
                {"uri": "foo"},
                {"token": "foo"},
                {
                    "uri": "foo",
                    "token": "foo",
                },
            ],
        )
        def test_good_ce_options(runtime_options):
            assert (
                _config._validate_runtime_options(
                    RuntimeName.CE_REMOTE, runtime_options
                )
                == runtime_options
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {},
            ],
        )
        def test_good_in_process_options(runtime_options):
            assert (
                _config._validate_runtime_options(
                    RuntimeName.IN_PROCESS, runtime_options
                )
                == runtime_options
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {},
                {"uri": "foo"},
                {"token": "foo"},
                {
                    "uri": "foo",
                    "token": "foo",
                },
            ],
        )
        def test_good_ce_remote_options(runtime_options):
            assert (
                _config._validate_runtime_options(
                    RuntimeName.CE_REMOTE, runtime_options
                )
                == runtime_options
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime",
            [
                RuntimeName.RAY_LOCAL,
                RuntimeName.IN_PROCESS,
                RuntimeName.CE_REMOTE,
            ],
        )
        def test_returns_empty_dict_for_missing_input(runtime):
            assert (_config._validate_runtime_options(runtime, {})) == {}

    class TestFailsForBadOptions:
        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {"bar": "foo"},
                {
                    "address": "foo",
                    "log_to_driver": "foo",
                    "storage": "foo",
                    "temp_dir": "foo",
                    "bar": "foo",
                },
            ],
        )
        def test_bad_ray_options(runtime_options):
            with pytest.raises(exceptions.RuntimeConfigError) as exc_info:
                _config._validate_runtime_options(
                    RuntimeName.RAY_LOCAL, runtime_options
                )
            assert (
                "'bar' is not a valid option for the RAY_LOCAL runtime."
                == exc_info.value.args[0]
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {"bar": "foo"},
                {
                    "bar": "foo",
                    "uri": "foo",
                    "token": "foo",
                },
            ],
        )
        def test_bad_ce_options(runtime_options):
            with pytest.raises(exceptions.RuntimeConfigError) as exc_info:
                _config._validate_runtime_options(
                    RuntimeName.CE_REMOTE, runtime_options
                )
            assert (
                "'bar' is not a valid option for the CE_REMOTE runtime."
                == exc_info.value.args[0]
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {"bar": "foo"},
                {
                    "bar": "foo",
                    "uri": "foo",
                    "token": "foo",
                },
            ],
        )
        def test_bad_ce_remote_options(runtime_options):
            with pytest.raises(exceptions.RuntimeConfigError) as exc_info:
                _config._validate_runtime_options(
                    RuntimeName.CE_REMOTE, runtime_options
                )
            assert (
                "'bar' is not a valid option for the CE_REMOTE runtime."
                == exc_info.value.args[0]
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_options",
            [
                {"bar": "foo"},
            ],
        )
        def test_bad_in_process_options(runtime_options):
            with pytest.raises(exceptions.RuntimeConfigError) as exc_info:
                _config._validate_runtime_options(
                    RuntimeName.IN_PROCESS, runtime_options
                )
            assert (
                "'bar' is not a valid option for the IN_PROCESS runtime."
                == exc_info.value.args[0]
            )

        @staticmethod
        @pytest.mark.parametrize(
            "runtime_name",
            [
                RuntimeName.IN_PROCESS,
                RuntimeName.RAY_LOCAL,
                RuntimeName.CE_REMOTE,
            ],
        )
        def test_mixed_options(runtime_name):
            runtime_options = {
                "address": "foo",
                "log_to_driver": "foo",
                "storage": "foo",
                "temp_dir": "foo",
                "uri": "foo",
                "token": "foo",
            }
            with pytest.raises(exceptions.RuntimeConfigError) as exc_info:
                _config._validate_runtime_options(runtime_name, runtime_options)
            assert (
                f"' is not a valid option for the {runtime_name} runtime."
                in exc_info.value.args[0]
            )


class TestProperties:
    """
    Tests focused on testing properties of the config, not specific output values.
    """

    class TestWriteAfterRead:
        """
        If I pass in an arg to `update_config()` it should be retained
        for future reads.
        """

        @pytest.mark.parametrize("config_name", ["custom_cfg"])
        @pytest.mark.parametrize(
            "runtime_name", [RuntimeName.CE_REMOTE, RuntimeName.RAY_LOCAL]
        )
        @pytest.mark.parametrize("new_runtime_options", [None, {}, {"foo": "bar"}])
        def test_no_file(
            self,
            patch_config_location,
            patch_runtime_option_validation,
            config_name,
            runtime_name,
            new_runtime_options,
        ):
            """
            When there's no config file at the start.
            """

            _config.update_config(
                config_name=config_name,
                runtime_name=runtime_name,
                new_runtime_options=new_runtime_options,
            )
            entry = _config.read_config(config_name)

            if config_name is not None:
                assert entry.config_name == config_name

            if runtime_name is not None:
                assert entry.runtime_name == runtime_name

            if new_runtime_options is not None:
                assert set(entry.runtime_options).issuperset(new_runtime_options.keys())


class TestReadConfig:
    @staticmethod
    def test_ray_alias_of_local():
        assert _config.read_config("ray") == _config.read_config("local")


class TestReadConfigNames:
    @staticmethod
    def test_lists_all(tmp_default_config_json):
        assert _config.read_config_names() == [
            name for name in TEST_CONFIG_JSON["configs"]
        ]

    @staticmethod
    @pytest.mark.slow
    def test_filelock_timeout(tmp_default_config_json):
        with pytest.raises(IOError):
            with filelock.FileLock(_config._get_config_directory() / LOCK_FILE_NAME):
                _config.read_config_names()

    @staticmethod
    def test_no_file(patch_config_location):
        assert _config.read_config_names() == []


class TestNameGeneration:
    class TestRuntimeDatetimeNaming:
        @staticmethod
        @pytest.mark.parametrize(
            "runtime_name, expected_name",
            [("IN_PROCESS", "in_process"), ("RAY_LOCAL", "local")],
        )
        def test_generate_name(runtime_name, expected_name):
            assert _config.generate_config_name(runtime_name, "") == expected_name
