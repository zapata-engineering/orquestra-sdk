################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################
import os

import pytest

from orquestra.sdk._client.dremio import _env_var_reader
from orquestra.workflow_shared.exceptions import EnvVarNotFoundError


class TestEnvReader:
    @staticmethod
    def test_basic_usage(monkeypatch):
        # Given
        name = "TEST_VAR"
        value = "test-val"
        monkeypatch.setenv(name, value)
        reader = _env_var_reader.EnvVarReader(name)

        # When
        read_val = reader.read()

        # Then
        assert read_val == value

    @staticmethod
    def test_setting_vars_from_python():
        # Given
        name = "TEST_VAR"
        value = "test-val"
        os.environ[name] = value

        try:
            reader = _env_var_reader.EnvVarReader(name)

            # When
            read_val = reader.read()

            # Then
            assert read_val == value
        finally:
            del os.environ[name]

    @staticmethod
    def test_variable_not_set():
        # Given
        name = "TEST_VAR"
        reader = _env_var_reader.EnvVarReader(name)

        # Then
        with pytest.raises(EnvVarNotFoundError) as exc_info:
            # When
            _ = reader.read()

        assert exc_info.value.var_name == name

    @staticmethod
    def test_empty_var_value(monkeypatch):
        # Given
        name = "TEST_VAR"
        value = ""
        monkeypatch.setenv(name, value)
        reader = _env_var_reader.EnvVarReader(name)

        # Then
        with pytest.raises(EnvVarNotFoundError) as exc_info:
            # When
            _ = reader.read()

        assert exc_info.value.var_name == name
