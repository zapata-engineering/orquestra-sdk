################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Pytest's requirement to share fixtures across test files.
"""
import json

import pytest

import orquestra.sdk._base._config


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
