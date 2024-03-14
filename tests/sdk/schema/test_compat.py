################################################################################
# © Copyright 2023 - 2024 Zapata Computing Inc.
################################################################################
"""
Tests for ``orquestra.sdk.schema._compat``.
"""
import typing as t
from pathlib import Path

import pytest

from orquestra.sdk.packaging import _versions
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


class TestResultIsPacked:
    class TestCurrentIR:
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
            data = path.read_text()
            return ir.WorkflowDef.model_validate_json(data)

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


class TestVersionsAreCompatible:
    @staticmethod
    @pytest.mark.parametrize(
        "generated_at,current,expected",
        [
            # Major version bump since the generation.
            ("0.2.3", "1.2.3", False),
            # We're at v0 -> not compatible with any other version.
            ("1.2.3", "0.2.3", False),
            # Exact version.
            ("0.2.3", "0.2.3", True),
            # We're at v0 -> not compatible with any other version.
            ("0.2.3", "0.2.4", False),
            # We're at v0 -> not compatible with any other version.
            ("0.2.3", "0.3.0", False),
            # Exact version.
            ("1.2.3", "1.2.3", True),
            # Exact version.
            ("2.2.3", "2.2.3", True),
            # Same major, bigger feature set.
            ("2.2.3", "2.3.3", True),
            # Same major, smaller feature set.
            ("2.2.3", "2.1.3", False),
            # Different majors.
            ("2.2.3", "3.1.3", False),
        ],
    )
    def test_examples(generated_at, current, expected):
        # Given
        generated_at_version = _versions.parse_version_str(generated_at)
        current_version = _versions.parse_version_str(current)

        # When
        result = _compat.versions_are_compatible(generated_at_version, current_version)

        # Then
        assert result == expected
