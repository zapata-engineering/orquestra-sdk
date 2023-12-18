################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from dataclasses import dataclass

import certifi
from pyarrow.flight import FlightClient


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

        return cls(flight_client=flight_client)

    def __init__(self, flight_client: FlightClient):
        self._flight_client = flight_client

    def read_query(self, query: str):
        pass


def read_certificate() -> str:
    return certifi.contents()
