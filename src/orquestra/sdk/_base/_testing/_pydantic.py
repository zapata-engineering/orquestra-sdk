################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
"""Utilities for testing with Pydantic models."""
import warnings
from unittest.mock import create_autospec


def model_autospec(cls):
    """Replacement for ``unittest.mock.create_autospec()``.

    Silences deprecation warnings caused by accessing deprecated fields during spec
    creation.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return create_autospec(cls)
