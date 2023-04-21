import json
from pathlib import Path

import pydantic
import pytest

from orquestra.sdk._base.serde import deserialize
from orquestra.sdk.schema.responses import JSONResult, PickleResult, WorkflowResult

DATA_PATH = Path(__file__).parent / "data"


class AnyObject:
    def __eq__(self, _):
        return True


class TestWorkflowResults:
    @pytest.fixture
    def mock_results(self, mocked_client):
        def _inner(scenario_name, sdk_version):
            ids_json = (
                DATA_PATH / f"{scenario_name}_ids_{sdk_version}.json"
            ).read_text()
            result_path = DATA_PATH / f"{scenario_name}_{sdk_version}.json"
            mocked_client.get_workflow_run_results.return_value = json.loads(ids_json)[
                "data"
            ]
            mocked_client.get_workflow_run_result.side_effect = [
                pydantic.parse_file_as(WorkflowResult, result_path)
            ]

        return _inner

    @pytest.mark.parametrize(
        "sdk_version, expected",
        [
            ("v0.46.0", ([100],)),
        ],
    )
    def test_single_workflow_output(
        self, mock_results, runtime, workflow_run_id, sdk_version, expected
    ):
        mock_results("single_result", sdk_version)

        results = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        assert isinstance(results, tuple)
        deserialised = tuple(deserialize(r) for r in results)
        assert deserialised == expected

    @pytest.mark.parametrize(
        "sdk_version, expected",
        [
            ("v0.46.0", ((100, AnyObject(), 100),)),
        ],
    )
    def test_multi_workflow_outputs(
        self, mock_results, runtime, workflow_run_id, sdk_version, expected
    ):
        mock_results("multi_results", sdk_version)

        results = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        assert isinstance(results, tuple)
        deserialised = tuple(deserialize(r) for r in results)
        assert deserialised == expected
