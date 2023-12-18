from unittest.mock import sentinel
import pytest


from orquestra.sdk.dremio import DremioClient


class TestDremioClient:
    @staticmethod
    def test_basic_init():
        # Given
        cfg = DremioClient.Config(
            user=sentinel.user,
            password=sentinel.password,
            host=sentinel.host,
            port=sentinel.port,
        )

        # When
        client = DremioClient.from_config(cfg)

        # Then
        assert client is not None
