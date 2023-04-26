################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import codecs
import json
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from orquestra.sdk._base import _db, serde
from orquestra.sdk._base._qe import _client, _qe_runtime
from orquestra.sdk._base._testing._example_wfs import my_workflow
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun

BASE_PATH = Path(__file__).parent / "data"


@pytest.fixture
def runtime(tmp_path: Path):
    runtime = _qe_runtime.QERuntime(
        # Fake QE configuration
        config=RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.QE_REMOTE,
            runtime_options={"uri": "http://localhost", "token": "blah"},
        ),
        project_dir=tmp_path,
    )
    runtime._client = create_autospec(_client.QEClient)
    return runtime


@pytest.fixture
def mock_workflow_db(monkeypatch: pytest.MonkeyPatch):
    def _inner(wf_run_id: str, wf_def_location: Path):
        wf_def = WorkflowDef.parse_raw(wf_def_location.read_text())
        _get_workflow_run = create_autospec(_db.WorkflowDB.get_workflow_run)
        _get_workflow_run.return_value = StoredWorkflowRun(
            workflow_run_id=wf_run_id,
            config_name="hello",
            workflow_def=wf_def,
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)

    return _inner


@pytest.fixture
def mock_artifact_responses(monkeypatch: pytest.MonkeyPatch):
    def _inner(mocked_runtime, responses_path: Path):
        responses = json.loads(responses_path.read_text())
        mocked_runtime._client.get_artifact.side_effect = [
            codecs.decode(r.encode(), "base64") for r in responses
        ]

    return _inner


@pytest.fixture
def workflow_run_id():
    return "wf-xxx-xxxx"


@pytest.mark.parametrize(
    "producing_sdk_version, expected",
    [
        (
            "0.46.0",
            {
                "invocation-0-task-dual": (0, set()),
                "invocation-1-task-dual": set(),
                "invocation-2-task-task3": set(),
                "invocation-3-task-task2": 200,
                "invocation-4-task-task1": 100,
            },
        ),
        (
            "0.47.0",
            {
                "invocation-0-task-dual": (0, set()),
                "invocation-1-task-dual": (0, set()),
                "invocation-2-task-task3": set(),
                "invocation-3-task-task2": 200,
                "invocation-4-task-task1": 100,
            },
        ),
    ],
)
# We intentionally load old workflow definitions
@pytest.mark.filterwarnings("ignore::orquestra.sdk.exceptions.VersionMismatch")
def test_task_outputs(
    runtime,
    mock_workflow_db,
    mock_artifact_responses,
    workflow_run_id,
    producing_sdk_version,
    expected,
):
    # Given
    wf_def_path = BASE_PATH / producing_sdk_version / "workflow_def.json"
    mocked_responses_path = (
        BASE_PATH / producing_sdk_version / "get_artifact_responses.json"
    )
    mock_workflow_db(workflow_run_id, wf_def_path)
    mock_artifact_responses(runtime, mocked_responses_path)

    # When
    outputs = runtime.get_available_outputs(workflow_run_id)

    # Then
    deserialized_outputs = {
        inv_id: serde.deserialize(res) for inv_id, res in outputs.items()
    }
    assert deserialized_outputs == expected
