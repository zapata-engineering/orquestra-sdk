################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from dataclasses import dataclass 

import pandas as pd
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
        return cls()

    def __init__(self, flight_client: FlightClient):
        self._flight_client = flight_client

    def read_query(self, query: str) -> pd.DataFrame:
        pass
