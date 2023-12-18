################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from dataclasses import dataclass 

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
