################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import os
from pathlib import Path
from typing import Any, Dict, Tuple, Union

import pytest

from orquestra.sdk._base._testing import _connections
from orquestra.sdk._base.serde import deserialize
from orquestra.sdk._ray import _client, _dag
from orquestra.sdk.schema import configs

PRODUCING_SDK_VERSIONS_TO_TEST = ["0.46.0", "0.47.0"]
BASE_PATH = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def runtime(tmp_path_factory: pytest.TempPathFactory, change_db_location):
    project_dir = tmp_path_factory.mktemp("ray-regression")
    config = configs.RuntimeConfiguration(
        config_name="ray-regression-config",
        runtime_name=configs.RuntimeName.RAY_LOCAL,
    )
    client = _client.RayClient()
    rt = _dag.RayRuntime(client, config, project_dir)
    yield rt


# Ray mishandles log file handlers and we get "_io.FileIO [closed]"
# unraisable exceptions. Last tested with Ray 2.3.0.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
# We intentionally load old workflow definitions
@pytest.mark.filterwarnings("ignore::orquestra.sdk.exceptions.VersionMismatch")
class TestOutputs:
    @pytest.fixture(scope="class", autouse=True, params=PRODUCING_SDK_VERSIONS_TO_TEST)
    def shared_ray_cluster(self, request):
        # We need to setup a Ray cluster for each producing SDK version
        storage_path = str(BASE_PATH / request.param)
        with _connections.make_ray_conn(storage_path) as ray_params:
            yield ray_params

    @pytest.mark.parametrize(
        "workflow_run_id, expected_result",
        (
            ("wf.multi_json_wf.0000001", (1, "hello")),
            ("wf.multi_pickle_wf.0000002", (set(), set())),
            ("wf.single_json_wf.0000003", (1,)),
            ("wf.single_pickle_wf.0000004", (set(),)),
        ),
    )
    def test_results(
        self,
        runtime: _dag.RayRuntime,
        workflow_run_id: str,
        expected_result: Tuple[Any, ...],
    ):
        # Get the workflow results from the cluster
        result = runtime.get_workflow_run_outputs_non_blocking(workflow_run_id)

        # The should be serialized, let's deserialize to check values
        deserialized_result = tuple(deserialize(r) for r in result)
        assert deserialized_result == expected_result

    @pytest.mark.parametrize(
        "workflow_run_id, expected_result",
        (
            (
                "wf.multi_json_wf.0000001",
                {"invocation-0-task-multi-json": (1, "hello")},
            ),
            (
                "wf.multi_pickle_wf.0000002",
                {"invocation-0-task-multi-pickle": (set(), set())},
            ),
            ("wf.single_json_wf.0000003", {"invocation-0-task-single-json": 1}),
            ("wf.single_pickle_wf.0000004", {"invocation-0-task-single-pickle": set()}),
        ),
    )
    def test_task_outputs(
        self,
        runtime: _dag.RayRuntime,
        workflow_run_id: str,
        expected_result: Dict[str, Union[Any, Tuple[Any, ...]]],
    ):
        # Get the task outputs from the cluster
        task_outputs = runtime.get_available_outputs(workflow_run_id)

        # The should be serialized, let's deserialize to check values
        deserialized_outputs = {
            task: deserialize(output) for task, output in task_outputs.items()
        }
        assert deserialized_outputs == expected_result
