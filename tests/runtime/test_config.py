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
from unittest.mock import Mock

import pytest

from orquestra.sdk._base import _config
from orquestra.sdk.schema.configs import RuntimeName


class TestSavePartialConfig:
    """
    Tests for _config.save_partial_config()
    """

    def test_saving_local(self, monkeypatch, patch_config_location):
        # We wanna ensure the 'local' was resolved
        monkeypatch.setattr(_config, "_resolve_config_name", Mock("local"))

        with pytest.raises(ValueError):
            _config.update_config(
                config_name=None,
                runtime_name=None,
                new_runtime_options=None,
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
