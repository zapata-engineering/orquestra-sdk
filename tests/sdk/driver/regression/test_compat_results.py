import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra.sdk._client.serde import deserialize

DATA_PATH = Path(__file__).parent / "data"


class MockedResponse:
    def __init__(self, json_dict, status_code=200):
        self._json_dict = json_dict
        self._status_code = status_code

    def json(self):
        return self._json_dict

    @property
    def status_code(self):
        return self._status_code

    @property
    def ok(self):
        return 200 <= self._status_code < 300


class TestWorkflowResults:
    @pytest.fixture
    def mock_results(self, monkeypatch):
        def _inner(sdk_version, scenario=None):
            extra = "{}".format("" if scenario is None else f"_{scenario}")
            ids_json = (DATA_PATH / f"{sdk_version}{extra}_ids.json").read_text()
            result_json = (DATA_PATH / f"{sdk_version}{extra}.json").read_text()
            client_get = Mock()
            client_get.side_effect = [
                MockedResponse(json.loads(ids_json)),
                MockedResponse(json.loads(result_json)),
            ]
            monkeypatch.setattr(
                "orquestra.sdk._client._driver._client.DriverClient._get", client_get
            )

        return _inner

    @pytest.mark.parametrize(
        "producing_sdk_version, scenario, expected",
        [
            (
                "v0.46.0",
                "json",
                (
                    100,
                    "json_string",
                ),
            ),
            ("v0.46.0", "json-single", (100,)),
            (
                "v0.46.0",
                "pickle",
                (
                    100,
                    "pickled_string",
                ),
            ),
            ("v0.46.0", "pickle-single", ("pickled_string",)),
            ("v0.47.0", "mixed", (100, "pickled_string")),
            ("v0.47.0", "single", ("pickled_string",)),
        ],
    )
    def test_multi_workflow_outputs(
        self,
        mock_results,
        runtime,
        workflow_run_id,
        producing_sdk_version,
        scenario,
        expected,
    ):
        mock_results(producing_sdk_version, scenario)

        results = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        assert isinstance(results, tuple)
        deserialised = tuple(deserialize(r) for r in results)
        assert deserialised == expected
