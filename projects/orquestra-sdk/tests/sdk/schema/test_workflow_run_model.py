################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Tests for the workflow run model.

There are small pieces of logic contained in the workflow model schema. We test these
here.
"""

import pytest

from orquestra.sdk._shared.schema.workflow_run import State


class TestState:
    class TestIsCompleted:
        @staticmethod
        @pytest.mark.parametrize(
            "state",
            [
                State.SUCCEEDED,
                State.TERMINATED,
                State.FAILED,
                State.ERROR,
                State.KILLED,
            ],
        )
        def test_returns_true_for_completed_states(state):
            assert state.is_completed()

        @staticmethod
        @pytest.mark.parametrize("state", [State.WAITING, State.RUNNING])
        def test_returns_false_for_non_completed_states(state):
            assert not state.is_completed()

        @staticmethod
        def test_handles_unknown_state():
            # GIVEN
            state = State.UNKNOWN

            # WHEN
            with pytest.warns(UserWarning) as record:
                assert not state.is_completed()

            # THEN
            assert len(record.list) == 1
            assert (
                str(record.list[0].message)
                == "Cannot determine the workflow run state."
            )
