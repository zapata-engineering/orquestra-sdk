################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################
"""Utility for reading data from Dremio managed by Orquestra."""
# Note for SDK developers
#
# This module is considered part of the "client".
# * it can import from `orquestra.sdk._client`
# * it can import from `orquestra.sdk.schema`
# * it must not import from `orquestra.sdk.ray_wrapper`

from ._api import DremioClient

__all__ = [
    "DremioClient",
]
