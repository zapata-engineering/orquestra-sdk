################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for ``orquestra.sdk._base._env``.
"""
import pytest

from orquestra.sdk._base import _env


class TestFlagSet:
    @staticmethod
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("1", True),
            ("0", False),
            ("true", True),
            ("false", False),
            ("True", True),
            ("False", False),
            ("TRUE", True),
            ("FALSE", False),
            ("gibberish", False),
            ("", False),
        ],
    )
    def test_values(monkeypatch, value, expected):
        # Given
        name = "SOME_VAR"
        monkeypatch.setenv(name, value)

        # When
        result = _env.flag_set(name)

        # Then
        assert result is expected

    @staticmethod
    def test_unset():
        # Given
        name = "SOME_UNSET_VAR"

        # When
        result = _env.flag_set(name)

        # Then
        assert result is False
