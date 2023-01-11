################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk._base._qe import _qe_runtime


class TestParseRunId:
    @pytest.mark.parametrize(
        "run_id,expected",
        [
            (
                "orquestra-basic-demo-qmeoj-r000-1502512104",
                (
                    "orquestra-basic-demo-qmeoj-r000",
                    "1502512104",
                ),
            ),
            (
                "orquestra-basic-demo-qmeoj-r000",
                ("orquestra-basic-demo-qmeoj-r000", None),
            ),
        ],
    )
    def test_example_output(self, run_id, expected):
        assert _qe_runtime.parse_run_id(run_id) == expected

    @pytest.mark.parametrize(
        "run_id",
        [
            "",
            "something-without-retry-counter",
            "something-without-retry-counter-4201337",
        ],
    )
    def test_errors(self, run_id):
        with pytest.raises(ValueError):
            _qe_runtime.parse_run_id(run_id)
