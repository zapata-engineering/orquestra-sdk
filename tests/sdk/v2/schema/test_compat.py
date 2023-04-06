################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Tests for ``orquestra.sdk.schema._compat``.
"""
import typing as t
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from orquestra.sdk.schema import _compat, ir

from .data import unpacking


def _get_task_by_name(
    wf_def: ir.WorkflowDef, task_name: str
) -> t.Sequence[t.Tuple[ir.TaskDef, ir.TaskInvocation]]:
    task_defs = [
        task_def
        for task_def in wf_def.tasks.values()
        if task_def.fn_ref.function_name == task_name
    ]
    task_def_ids = {task_def.id for task_def in task_defs}

    pairs = []
    for task_def in task_defs:
        invs = [
            inv
            for inv in wf_def.task_invocations.values()
            if inv.task_id in task_def_ids
        ]
        pairs.extend([(task_def, inv) for inv in invs])
    return pairs


DATA_PATH = Path(__file__).parent / "data"
SNAPSHOT_VERSIONS = [
    # Last version without ``ir.WorkflowDef.metadata``
    "0.44.0",
    # Last version without ``ir.TaskDef.output_metadata``
    "0.45.1",
]


class TestNOutputs:
    class TestModernIR:
        @staticmethod
        @pytest.fixture
        def wf_def():
            return unpacking.unpacking_wf().model

        @staticmethod
        def test_single_output_task(wf_def: ir.WorkflowDef):
            # Given
            pairs = _get_task_by_name(wf_def=wf_def, task_name="single_output")
            assert len(pairs) == 1
            task_def, task_inv = pairs[0]

            # When
            n_outputs = _compat.n_outputs(task_def, task_inv)

            # Then
            assert n_outputs == 1

        @staticmethod
        def test_multi_output(wf_def: ir.WorkflowDef):
            # Given
            pairs = _get_task_by_name(wf_def=wf_def, task_name="two_outputs")
            assert (
                len(pairs) == 2
            ), "There should be two invocations in the test workflow"

            for task_def, task_inv in pairs:
                # When
                n_outputs = _compat.n_outputs(task_def, task_inv)

                # Then
                assert n_outputs == 2

    @pytest.mark.parametrize("snapshot_version", SNAPSHOT_VERSIONS)
    class TestOldIR:
        @staticmethod
        @pytest.fixture
        def wf_def(snapshot_version: str):
            path = DATA_PATH / f"unpacking_wf_{snapshot_version}.json"
            return ir.WorkflowDef.parse_file(path)

        @staticmethod
        @pytest.mark.filterwarnings("ignore::orquestra.sdk.exceptions.VersionMismatch")
        def test_single_output_task(wf_def: ir.WorkflowDef):
            # Given
            pairs = _get_task_by_name(wf_def=wf_def, task_name="single_output")
            assert len(pairs) == 1
            task_def, task_inv = pairs[0]

            # When
            n_outputs = _compat.n_outputs(task_def, task_inv)

            # Then
            assert n_outputs == 1

        # No test for "multi_output" because "n_outputs" isn't reliable with old IRs.


class TestResultIsPacked:
    class TestModernIR:
        @staticmethod
        def test_single_output_task():
            # Given
            wf_def = unpacking.unpacking_wf().model
            pairs = _get_task_by_name(wf_def=wf_def, task_name="single_output")
            assert len(pairs) == 1
            task_def, _ = pairs[0]

            # When
            packed = _compat.result_is_packed(task_def)

            # Then
            assert packed is False

        @staticmethod
        def test_multi_output():
            # Given
            wf_def = unpacking.unpacking_wf().model
            pairs = _get_task_by_name(wf_def=wf_def, task_name="two_outputs")
            assert (
                len(pairs) == 2
            ), "There should be two invocations in the test workflow"

            for task_def, _ in pairs:
                # When
                packed = _compat.result_is_packed(task_def)

                # Then
                assert packed is True

    @pytest.mark.parametrize("snapshot_version", SNAPSHOT_VERSIONS)
    class TestOldIR:
        @staticmethod
        @pytest.fixture
        def wf_def(snapshot_version: str):
            path = DATA_PATH / f"unpacking_wf_{snapshot_version}.json"
            return ir.WorkflowDef.parse_file(path)

        @staticmethod
        @pytest.mark.filterwarnings("ignore::orquestra.sdk.exceptions.VersionMismatch")
        def test_single_output_task(wf_def: ir.WorkflowDef):
            # Given
            pairs = _get_task_by_name(wf_def=wf_def, task_name="single_output")
            assert len(pairs) == 1
            task_def, _ = pairs[0]

            # When
            packed = _compat.result_is_packed(task_def)

            # Then
            assert packed is False

        @staticmethod
        @pytest.mark.filterwarnings("ignore::orquestra.sdk.exceptions.VersionMismatch")
        def test_multi_output_task(wf_def: ir.WorkflowDef):
            # Given
            pairs = _get_task_by_name(wf_def=wf_def, task_name="two_outputs")
            assert (
                len(pairs) == 2
            ), "There should be two invocations in the test workflow"

            for task_def, _ in pairs:
                # When
                packed = _compat.result_is_packed(task_def)

                # Then
                assert packed is False
