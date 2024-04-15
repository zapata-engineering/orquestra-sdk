################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock, create_autospec, sentinel

import pytest

from orquestra.sdk._client.dremio import DremioClient, _api
from orquestra.sdk._client.dremio._flight_facade import (
    FlightClient,
    FlightEndpoint,
    FlightStreamReader,
)


class TestDremioClient:
    class TestFlightIntegration:
        """Attempts to test as much integration with ``pyarrow.flight`` as possible
        without sending data over the wire.

        Test boundaries::

            [.read_query]┬►[FlightClient]
                         ├►[FlightEndpoint]
                         └►[FlightStreamReader]
        """

        @staticmethod
        @pytest.fixture
        def mock_flight_client(monkeypatch):
            flight_client = create_autospec(FlightClient, name="flight_client")
            flight_init = Mock(name="FlightClient.__init__", return_value=flight_client)
            monkeypatch.setattr(_api, "FlightClient", flight_init)

            flight_endpoint = create_autospec(FlightEndpoint, name="flight_endpoint")
            flight_client.get_flight_info().endpoints = [flight_endpoint]
            flight_client.authenticate_basic_token.return_value = (
                b"foo",
                b"test value",
            )
            return flight_client

        @staticmethod
        def test_standard_usage(monkeypatch, mock_flight_client):
            # Given
            monkeypatch.setenv(
                "ORQ_DREMIO_URI", "grpc+tls://test_host.orquestra.io:2037"
            )
            monkeypatch.setenv("ORQ_DREMIO_USER", "test_user")
            monkeypatch.setenv("ORQ_DREMIO_PASS", "test_pass")

            query = "shouldn't matter"

            df_sentinel = sentinel.df
            reader = create_autospec(FlightStreamReader)
            reader.read_pandas.return_value = df_sentinel
            mock_flight_client.do_get.return_value = reader

            # When
            client = DremioClient.from_env_vars()
            df = client.read_query(query)

            # Then
            assert df == df_sentinel

        def _iter_instance_var_dicts(self, root):
            """Finds all instance variables, including nested ones.

            Traverses the whole attribute graph. Yields attribute values for
            ``root`` and it's children (``root``'s attribute's attributes).

            Ignores ``Mock`` objects to avoid infinite recursion.
            """
            try:
                attr_dict = root.__dict__
            except AttributeError:
                return

            for child_obj in attr_dict.values():
                if isinstance(child_obj, Mock):
                    continue

                yield child_obj
                yield from self._iter_instance_var_dicts(child_obj)

        @pytest.mark.usefixtures("mock_flight_client")
        def test_creds_arent_stored(self, monkeypatch):
            """We should keep the user+pass as short as possible. We shouldn't retain
            them as instance variables.
            """
            # Given
            monkeypatch.setenv(
                "ORQ_DREMIO_URI", "grpc+tls://test_host.orquestra.io:2037"
            )
            user = "test_user"
            password = "test_pass"
            monkeypatch.setenv("ORQ_DREMIO_USER", user)
            monkeypatch.setenv("ORQ_DREMIO_PASS", password)

            # When
            client = DremioClient.from_env_vars()

            # Then
            for child_obj in self._iter_instance_var_dicts(client):
                assert child_obj != user
                assert child_obj != password
