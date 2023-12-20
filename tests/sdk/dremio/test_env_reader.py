import os
from orquestra.sdk.dremio import _env_var_reader


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
