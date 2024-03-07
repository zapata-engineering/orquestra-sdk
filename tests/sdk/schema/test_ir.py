################################################################################
# © Copyright 2023 - 2024 Zapata Computing Inc.
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
            json_str = wf_def.model_dump_json()
            wf_def2 = ir.WorkflowDef.parse_raw(json_str)

            # Then
            assert wf_def2 == wf_def

    class TestRegression:
        """
        Validates we're able to deserialize workflow defs generated with old versions
        of the SDK. We should be careful about the assertions; probably we should never
        change them to test for regressions.
        """

        @staticmethod
        def test_current_ir():
            # Given
            wf = unpacking.unpacking_wf()

            # When
            _ = wf.model

            # Then
            # No warning: we're good

        @staticmethod
        @pytest.mark.parametrize("snapshot_version", ["0.44.0", "0.45.1"])
        def test_old_ir(snapshot_version: str):
            path = DATA_PATH / f"unpacking_wf_{snapshot_version}.json"
            # Then
            with pytest.warns(exceptions.VersionMismatch):
                # When
                _ = ir.WorkflowDef.parse_file(path)
