################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from dataclasses import dataclass

import certifi
from ._flight_facade import FlightCallOptions, FlightClient, FlightDescriptor


class DremioClient:
    @dataclass(frozen=True)
    class Config:
        user: str
        password: str
        host: str
        port: int

    @classmethod
    def from_config(cls, cfg: Config) -> "DremioClient":
        cert_contents = read_certificate()
        flight_client = FlightClient(
            f"{cfg.host}:{cfg.port}",
            tls_root_certs=cert_contents,
        )

        return cls(flight_client=flight_client, user=cfg.user, password=cfg.password)

    def __init__(self, flight_client: FlightClient, user: str, password: str):
        self._flight_client = flight_client
        self._user = user
        self._password = password

    def read_query(self, query: str):
        flight_desc = FlightDescriptor.for_command(query)
        options = self._get_call_options()
        ticket = self._get_ticket(descriptor=flight_desc, call_options=options)

        # Retrieve the result set as a stream of Arrow record batches.
        reader = self._flight_client.do_get(ticket, options)
        return reader.read_pandas()

    def _get_ticket(
        self, descriptor: FlightDescriptor, call_options: FlightCallOptions
    ):
        flight_info = self._flight_client.get_flight_info(descriptor, call_options)
        return flight_info.endpoints[0].ticket

    def _get_call_options(self):
        token = self._flight_client.authenticate_basic_token(self._user, self._password)
        return FlightCallOptions(headers=[token])


def read_certificate() -> str:
    return certifi.contents()
