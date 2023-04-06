################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for the little bits of code we have in ``orquestra.sdk.schema.ir``.
"""
from pathlib import Path

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk.schema import ir

from .data import unpacking

DATA_PATH = Path(__file__).parent / "data"


class TestWorkflowDef:
    """
    Tests for ``ir.WorkflowDef``.
    """

    class TestUnit:
        @staticmethod
        def test_serde_roundtrip():
            # Given
            wf_def = unpacking.unpacking_wf().model

            # When
            json_str = wf_def.json()
            wf_def2 = ir.WorkflowDef.parse_raw(json_str)

            # Then
            assert wf_def2 == wf_def
