################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Glue code between the public API and concept-specific modules."""
import json
from unittest.mock import Mock

import pytest

from orquestra.sdk._base._api import RuntimeConfig
from orquestra.sdk._base._api.repos import RuntimeRepo
from orquestra.sdk._base._driver._ce_runtime import CERuntime
from orquestra.sdk._ray import _client
from orquestra.sdk._ray._dag import RayRuntime

from ...data.configs import TEST_CONFIG_JSON


@pytest.fixture
def tmp_default_config_json(patch_config_location):
    json_file = patch_config_location / "config.json"

    with json_file.open("w") as f:
        json.dump(TEST_CONFIG_JSON, f)

    return json_file


class TestRuntimeRepo:
    class TestIntegration:
        """Integration tests. Test boundaries::

        [RuntimeRepo]-[config.json]
        [RuntimeRepo]-[RayClient]
        """

    @pytest.mark.usefixtures("tmp_default_config_json")
    class TestGetRuntime:
        @staticmethod
        @pytest.fixture
        def repo():
            return RuntimeRepo()

        @staticmethod
        def test_ray(monkeypatch, repo: RuntimeRepo):
            # Given
            config = RuntimeConfig.load("ray")
            # Prevent starting Ray connection.
            monkeypatch.setattr(_client, "RayClient", Mock())

            # When
            runtime = repo.get_runtime(config=config)

            # Then
            assert isinstance(runtime, RayRuntime)

        @staticmethod
        def test_ce(repo: RuntimeRepo):
            # Given
            config = RuntimeConfig.load("test_config_ce")

            # When
            runtime = repo.get_runtime(config=config)

            # Then
            assert isinstance(runtime, CERuntime)
