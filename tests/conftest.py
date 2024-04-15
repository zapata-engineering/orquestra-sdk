################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Pytest's requirement to share fixtures across test files.
"""
from unittest.mock import Mock

import pytest

import orquestra.sdk._client._base._config


@pytest.fixture
def patch_config_location(tmp_path, monkeypatch):
    """
    Makes the functions in orquestra.sdk._base._config read/write file from a
    temporary directory.
    """
    config_location = Mock(return_value=tmp_path / "config.json")
    monkeypatch.setattr(
        orquestra.sdk._client._base._config, "get_config_file_path", config_location
    )
    return tmp_path


@pytest.fixture
def patch_config_name_generation(monkeypatch):
    patched_name = "patched_config_name"
    monkeypatch.setattr(
        orquestra.sdk._client._base._config,
        "generate_config_name",
        Mock(return_value=patched_name),
    )
    return patched_name


@pytest.fixture
def patch_runtime_option_validation(monkeypatch):
    def assume_valid(_, input, **kwargs):
        if input is None:
            return {}
        else:
            return input

    monkeypatch.setattr(
        orquestra.sdk._client._base._config, "_validate_runtime_options", assume_valid
    )
