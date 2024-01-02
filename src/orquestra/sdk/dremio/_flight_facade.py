################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################

"""Imports symbols we need from ``pyarrow.flight``.

Having the imports here allows ignoring this file from ``mypy`` type checking, but
leaving the ``pyright`` type checker on.
"""

from pyarrow.flight import (
    FlightCallOptions,
    FlightClient,
    FlightDescriptor,
    FlightEndpoint,
    FlightStreamReader,
)

__all__ = [
    "FlightCallOptions",
    "FlightClient",
    "FlightDescriptor",
    "FlightEndpoint",
    "FlightStreamReader",
]
