from unittest.mock import Mock, create_autospec, sentinel


from orquestra.sdk.dremio import DremioClient
from pyarrow.flight import FlightClient, FlightEndpoint, FlightStreamReader


class TestDremioClient:
    @staticmethod
    def test_basic_init(monkeypatch):
        # Given
        user = "test_user"
        password = "test_password"
        host = "grpc+tls://test-host.orquestra.io"
        port = 2037
        cfg = DremioClient.Config(
            user=user,
            password=password,
            host=host,
            port=port,
        )

        cert_content = b"test cert contents"

        flight_init_spy = Mock(name="FlightClient.__init__")
        monkeypatch.setattr(FlightClient, "__init__", flight_init_spy)

        # When
        client = DremioClient.from_config(cfg)

        # Then
        assert client is not None
        assert client._flight_client is not None
        flight_init_spy.assert_called_with("grpc+tls://test-host.orquestra.io:2037", tls_root_certs=cert_content)

    @staticmethod
    def test_using_flight_client():
        # Given
        flight_client = create_autospec(FlightClient, name="flight_client")

        flight_endpoint = create_autospec(FlightEndpoint, "flight_endpoint")
        flight_client.get_flight_info().endpoints = [flight_endpoint]

        reader = create_autospec(FlightStreamReader)
        df_sentinel = sentinel.df
        reader.read_pandas.return_value = df_sentinel

        flight_client.do_get.return_value = reader

        client = DremioClient(flight_client=flight_client)

        query = "shouldn't matter"

        # When
        df = client.read_query(query)

        # Then
        assert df == df_sentinel
