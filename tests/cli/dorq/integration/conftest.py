import pytest
from unittest.mock import create_autospec, Mock
from orquestra.sdk._base.abc import RuntimeInterface
import orquestra.sdk._base._factory as factory
from orquestra.sdk.schema.workflow_run import WorkflowRun as WorkflowRunModel
from orquestra.sdk.schema.workflow_run import RunStatus, State
from orquestra.sdk.schema import ir
import datetime

NOW = datetime.datetime.now()


def return_wf_list(num_of_runs):
    wf_runs = []
    for stub_id in range(num_of_runs):
        wf_run = Mock()
        wf_run.id = stub_id
        wf_runs.append(wf_run)
    return wf_runs


def get_status_model(wf_run_id):
    return WorkflowRunModel(
        id=wf_run_id,
        workflow_def=create_autospec(ir.WorkflowDef),
        task_runs=[],
        status=RunStatus(state=State("RUNNING"), start_time=NOW, end_time=None),
    )


@pytest.fixture
def fake_list_runtime(monkeypatch):
    runtime = create_autospec(RuntimeInterface, name="runtime")
    runtime.list_workflow_runs.return_value = return_wf_list(4)
    runtime.get_workflow_run_status = get_status_model
    monkeypatch.setattr("orquestra.sdk._base._api._config.build_runtime_from_config", create_autospec(factory.build_runtime_from_config, return_value=runtime))

    return runtime
