from unittest.mock import Mock, create_autospec, sentinel


from orquestra.sdk.dremio import _api, DremioClient
from pyarrow.flight import FlightClient, FlightEndpoint, FlightStreamReader


class TestDremioClient:
    @staticmethod
    def test_from_config(monkeypatch):
        """Isolated unit test for the basic init"""
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

        cert_content = b"test certificate content"
        monkeypatch.setattr(
            _api,
            "read_certificate",
            create_autospec(_api.read_certificate, return_value=cert_content),
        )

        flight_init_spy = Mock(name="FlightClient.__init__")
        monkeypatch.setattr(_api, "FlightClient", flight_init_spy)

        # When
        client = DremioClient.from_config(cfg)

        # Then
        assert client is not None
        assert client._flight_client is not None
        flight_init_spy.assert_called_with(
            "grpc+tls://test-host.orquestra.io:2037", tls_root_certs=cert_content
        )

    @staticmethod
    def test_read_certificate():
        """Integration test for reading certs from the file system."""
        # When
        cert_contents = _api.read_certificate()

        # Then
        assert len(cert_contents) > 0
        assert "-----BEGIN CERTIFICATE-----" in cert_contents
        assert "-----END CERTIFICATE-----" in cert_contents

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
