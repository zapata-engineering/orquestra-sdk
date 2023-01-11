################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
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
import datetime
import json
from unittest.mock import Mock

import filelock
import pytest

import orquestra.sdk.exceptions as exceptions
from orquestra.sdk._base import _config
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName

from .data.configs import TEST_CONFIG_JSON


class TestSavePartialConfig:
    """
    Tests for _config.save_partial_config()
    """

    @staticmethod
    @pytest.mark.parametrize("resolved_config_name", ["local", "in_process", "ray"])
    def test_saving_local(resolved_config_name, monkeypatch, patch_config_location):
        # We want to ensure the 'local' and 'in_process' configs are resolved
        monkeypatch.setattr(_config, "_resolve_config_name", Mock(resolved_config_name))

        with pytest.raises(ValueError):
            _config.update_config(
                config_name=None,
                runtime_name=None,
                new_runtime_options=None,
            )


class TestResolveRuntimeOptionsForWriting:
    @staticmethod
    def test_returns_builtins_if_name_is_builtin():
        new_runtime_options = {}
        config_name = _config.BUILT_IN_CONFIG_NAME
        stub_prev_config = RuntimeConfiguration(
            config_name="test_config", runtime_name="RAY_LOCAL", runtime_options={}
        )

        runtime_options = _config._resolve_runtime_options_for_writing(
            new_runtime_options,
            config_name,
            stub_prev_config,
        )

        assert runtime_options == _config.LOCAL_RUNTIME_CONFIGURATION.runtime_options


class TestResolveRuntimeNameForWriting:
    @staticmethod
    def test_previously_stored():
        stub_prev_config = RuntimeConfiguration(
            config_name="test_config", runtime_name="RAY_LOCAL", runtime_options={}
        )

        resolved_runtime_name = _config._resolve_runtime_name_for_writing(
            None, stub_prev_config, "test_config"
        )

        assert resolved_runtime_name == stub_prev_config.runtime_name


class TestResolveConfigNameForWriting:
    @staticmethod
    def test_raises_exception_if_name_is_builtin():
        builtin_name = _config.BUILT_IN_CONFIG_NAME

        with pytest.raises(ValueError) as exc_info:
            _config._resolve_config_name_for_writing(builtin_name, None)
        assert f"Can't write {builtin_name}, it's a reserved name" in str(exc_info)


class TestResolveConfigFileForReading:
    @staticmethod
    def test_returns_none_for_nonexistant_file_default_location(patch_config_location):
        assert _config._resolve_config_file_for_reading() is None

    @staticmethod
    def test_returns_none_for_nonexistant_file_custom_location(tmp_path):
        assert (
            _config._resolve_config_file_for_reading(
                config_file_path=tmp_path / "test.file"
            )
            is None
        )


class TestResolveConfigName:
    @staticmethod
    def test_raises_exception_if_cannot_resolve_name():
        with pytest.raises(ValueError) as exc_info:
            _config._resolve_config_name(None, None)
        assert (
            "Couldn't resolve an appropriate config name to read the "
            "configuration from. Please pass it explicitly."
        ) in str(exc_info)


class TestResolveConfigEntryForReading:
    @staticmethod
    def test_returns_builtins():
        assert (
            _config._resolve_config_entry_for_reading(
                _config.BUILT_IN_CONFIG_NAME, None
            )
            == _config.LOCAL_RUNTIME_CONFIGURATION
        )

    @staticmethod
    def test_raises_exception_if_config_file_not_found(patch_config_location):
        with pytest.raises(exceptions.ConfigFileNotFoundError) as exc_info:
            _config._resolve_config_entry_for_reading("test_config", None)
        assert "Could not locate config file." in str(exc_info)


class TestGenerateConfigName:
    @staticmethod
    def test_raises_exception_if_qe_but_no_uri():
        with pytest.raises(AttributeError) as exc_info:
            _config.generate_config_name(RuntimeName.QE_REMOTE, {})
        assert "QE and CE runtime configurations must have a 'URI' value set." in str(
            exc_info
        )


class TestReadDefaultConfigName:
    @staticmethod
    def test_happy_path(patch_config_location):
        test_config_name = "test_name"
        with open(patch_config_location / "config.json", "w") as f:
            json.dump(
                {
                    "version": "0.0.0",
                    "configs": {},
                    "default_config_name": test_config_name,
                },
                f,
            )

        assert _config.read_default_config_name() == test_config_name

    @staticmethod
    def test_returns_builtins_if_no_file_found(patch_config_location):
        assert _config.read_default_config_name() == _config.BUILT_IN_CONFIG_NAME


class TestUpdateDefaultConfigName:
    @staticmethod
    def test_no_previous_config_file(patch_config_location):
        _config.update_default_config_name("hello there")

        with open(patch_config_location / "config.json", "r") as f:
            data = json.load(f)
        assert data["default_config_name"] == "hello there"

    @staticmethod
    def test_with_previous_config_file(patch_config_location):
        with open(patch_config_location / "config.json", "w") as f:
            json.dump(
                {
                    "version": "0.0.0",
                    "configs": {},
                    "default_config_name": "hello there",
                },
                f,
            )

        _config.update_default_config_name("general kenobi")

        with open(patch_config_location / "config.json", "r") as f:
            data = json.load(f)
        assert data["default_config_name"] == "general kenobi"


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
        def test_good_qe_options(runtime_options):
            assert (
                _config._validate_runtime_options(
                    RuntimeName.QE_REMOTE, runtime_options
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
                RuntimeName.QE_REMOTE,
                RuntimeName.CE_REMOTE,
            ],
        )
        def test_returns_empty_dict_for_missing_input(runtime):
            assert (_config._validate_runtime_options(runtime, None)) == {}

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
        def test_bad_qe_options(runtime_options):
            with pytest.raises(exceptions.RuntimeConfigError) as exc_info:
                _config._validate_runtime_options(
                    RuntimeName.QE_REMOTE, runtime_options
                )
            assert (
                "'bar' is not a valid option for the QE_REMOTE runtime."
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
                RuntimeName.QE_REMOTE,
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
            "runtime_name", [RuntimeName.QE_REMOTE, RuntimeName.RAY_LOCAL]
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
        with pytest.raises(IOError) as exc_info:
            with filelock.FileLock(
                _config._get_config_directory() / _config.LOCK_FILE_NAME
            ):
                _config.read_config_names()
        assert (
            f"Could not acquire the lock for the config file at '{tmp_default_config_json / 'config.json'}' - does another function or process currently hold it? If you're calling `read_config_names` from a context that has already acquired the lock, you may want to look into using `_read_config_names` instead."  # noqa 501
            == exc_info.value.args[0]
        )

    @staticmethod
    def test_no_file(patch_config_location):
        assert _config.read_config_names() == []


FAKE_TIME = datetime.datetime(1605, 11, 5, 0, 0, 0)


@pytest.fixture
def patch_datetime_now(monkeypatch):
    class mydatetime:
        @classmethod
        def now(cls):
            return FAKE_TIME

        @classmethod
        def today(cls):
            return FAKE_TIME

    monkeypatch.setattr(datetime, "datetime", mydatetime)


class TestNameGeneration:
    class TestRuntimeDatetimeNaming:
        @staticmethod
        @pytest.mark.parametrize(
            "runtime_name, expected_name",
            [("IN_PROCESS", "in_process"), ("RAY_LOCAL", "local")],
        )
        def test_generate_name(runtime_name, expected_name):

            assert _config.generate_config_name(runtime_name, "") == expected_name
