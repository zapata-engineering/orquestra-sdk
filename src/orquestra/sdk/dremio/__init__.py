################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
# THIS FILE IS A SHIM TO REEXPORT MLFLOW CLIENT AS A PUBLIC API.
# DO NOT PUT ANY LOGIC INTO THIS FILE.

from .._client.dremio import DremioClient

__all__ = [
    "DremioClient",
]
