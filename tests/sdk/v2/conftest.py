################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Pytest's requirement to share fixtures across test files.
"""
import json
from unittest.mock import Mock

import pytest

import orquestra.sdk._base._config

from .data.configs import TEST_CONFIG_JSON


@pytest.fixture
def patch_config_location(tmp_path, monkeypatch):
    """
    Makes the functions in orquestra.sdk._base._config read/write file from a
    temporary directory.
    """
    config_location = Mock(return_value=tmp_path / "config.json")
    monkeypatch.setattr(
        orquestra.sdk._base._config, "_get_config_file_path", config_location
    )
    return tmp_path


@pytest.fixture
def patch_runtime_option_validation(monkeypatch):
    def assume_valid(_, input, **kwargs):
        if input is None:
            return {}
        else:
            return input

    monkeypatch.setattr(
        orquestra.sdk._base._config, "_validate_runtime_options", assume_valid
    )


@pytest.fixture
def tmp_config_json(tmp_path):
    json_file = tmp_path / "config.json"

    with open(json_file, "w") as f:
        json.dump(TEST_CONFIG_JSON, f)

    return json_file


@pytest.fixture
def tmp_default_config_json(patch_config_location, tmp_config_json):
    return patch_config_location
