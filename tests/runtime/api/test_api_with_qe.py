################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import base64
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import responses

import orquestra.sdk as sdk
import orquestra.sdk._base._db as _db
from orquestra.sdk import exceptions
from orquestra.sdk._base import _api
from orquestra.sdk._base._testing._example_wfs import my_workflow
from orquestra.sdk.schema.configs import RuntimeName
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import State

# region: globals
QE_MINIMAL_CURRENT_REPRESENTATION = {
    "status": {
        "nodes": {
            "hello-there-abc123-r000": {
                "id": "workflow-id",
                "name": "hello-there-abc123-r000",
                "displayName": "hello-there-abc123-r000",
                "type": "DAG",
                "templateName": "qeDagWorkflow",
                "phase": "Succeeded",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:05:09Z",
                "children": ["hello-there-abc123-r000-2738763496"],
                "outboundNodes": ["hello-there-abc123-r000-3825957270"],
            },
            "wf-3fmte-r000": {
                "id": "wf-3fmte-r000",
                "name": "wf-3fmte-r000",
                "displayName": "wf-3fmte-r000",
                "type": "DAG",
                "templateName": "qeDagWorkflow",
                "phase": "Failed",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:05:09Z",
                "children": ["hello-there-abc123-r000-2738763496"],
                "outboundNodes": ["hello-there-abc123-r000-3825957270"],
            },
            "hello-there-abc123-r000-2738763496": {
                "id": "hello-there-abc123-r000-2738763496",
                "name": "hello-there-abc123-r000.invocation-1-task-multi-output-test-64e80bbbd8",  # noqa: E501
                "displayName": "invocation-1-task-multi-output-test-64e80bbbd8",
                "type": "Pod",
                "templateName": "invocation-1-task-multi-output-test-64e80bbbd8",
                "phase": "Succeeded",
                "boundaryID": "hello-there-abc123-r000",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:04:28Z",
                "children": ["hello-there-abc123-r000-3825957270"],
            },
            "hello-there-abc123-r000-2752640860": {
                "id": "hello-there-abc123-r000-2752640860",
                "name": "hello-there-abc123-r000.onExit",
                "displayName": "hello-there-abc123-r000.onExit",
                "type": "DAG",
                "templateName": "orquestra",
                "phase": "Succeeded",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:16Z",
                "children": ["hello-there-abc123-r000-48533308"],
                "outboundNodes": ["hello-there-abc123-r000-48533308"],
            },
            "hello-there-abc123-r000-3825957270": {
                "id": "hello-there-abc123-r000-3825957270",
                "name": "hello-there-abc123-r000.invocation-0-task-make-greeting-message-4dd5c94fb7",  # noqa: E501
                "displayName": "invocation-0-task-make-greeting-message-4dd5c94fb7",
                "type": "Pod",
                "templateName": "invocation-0-task-make-greeting-message-4dd5c94fb7",
                "phase": "Succeeded",
                "boundaryID": "hello-there-abc123-r000",
                "startedAt": "1989-12-13T09:04:29Z",
                "finishedAt": "1989-12-13T09:05:08Z",
            },
            "hello-there-abc123-r000-48533308": {
                "id": "hello-there-abc123-r000-48533308",
                "name": "hello-there-abc123-r000.onExit.Finalizing-your-data",
                "displayName": "Finalizing-your-data",
                "type": "Pod",
                "templateName": "Finalizing-your-data",
                "phase": "Succeeded",
                "boundaryID": "hello-there-abc123-r000-2752640860",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:14Z",
            },
        },
    },
}
# endregion

# region: fixtures


@pytest.fixture
def mocked_responses():
    # This is the place to tap into response mocking machinery, if needed.
    with responses.RequestsMock() as mocked_responses:
        yield mocked_responses


@pytest.fixture()
def remote_config(patch_config_location):
    resolved_runtime_options = {"uri": "http://localhost", "token": "BLAH"}
    config = sdk._base._config.write_config(
        "QE test config",
        RuntimeName.QE_REMOTE,
        resolved_runtime_options,
    )

    return config[0].config_name


# endregion


class TestStartWorkflowRun:
    @staticmethod
    def test_happy_path(
        remote_config,
        monkeypatch,
        mocked_responses,
        tmp_path,
        mock_workflow_db_location,
    ):
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", Mock())
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            body="workflow-id",
        )

        run = my_workflow().prepare(remote_config)
        run.start()

        assert run.run_id == "workflow-id"

    @staticmethod
    def test_raises_exception_if_worflow_name_does_not_match_qe_reqs(remote_config):
        workflow_name = "NameThatDoesNotMatchQEReqs"

        @sdk.workflow(custom_name=workflow_name)
        def BadNameWorkflow():
            return [None]

        run = BadNameWorkflow().prepare(remote_config)

        with pytest.raises(exceptions.InvalidWorkflowDefinitionError) as exc_info:
            run.start()
        assert f'Workflow name "{workflow_name}" is invalid' in str(exc_info)


class TestGetWorkflowRunStatus:
    @staticmethod
    @pytest.fixture
    def qe_status_response():
        return {
            "id": "workflow-id",
            "status": "Succeeded",
            "currentRepresentation": base64.standard_b64encode(
                json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION).encode()
            ).decode(),
            "completed": True,
            "retry": "",
            "lastModified": "1989-12-13T09:10:04.14422796Z",
            "created": "1989-12-13T09:03:49.39478764Z",
        }

    def test_two_step_form(
        self,
        monkeypatch,
        mocked_responses,
        remote_config,
        qe_status_response,
        tmp_path,
        mock_workflow_db_location,
    ):
        monkeypatch.setattr(
            _db.WorkflowDB,
            "get_workflow_run",
            Mock(
                return_value=StoredWorkflowRun(
                    workflow_run_id="workflow-id",
                    config_name="hello",
                    workflow_def=my_workflow().model,
                )
            ),
        )
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", Mock())
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            body="workflow-id",
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=qe_status_response,
        )

        run = my_workflow().prepare(remote_config)
        run.start()

        assert run.get_status() == State.SUCCEEDED

    def test_shorthand_form(
        self,
        monkeypatch,
        mocked_responses,
        remote_config,
        qe_status_response,
        tmp_path,
        mock_workflow_db_location,
    ):
        monkeypatch.setattr(
            _db.WorkflowDB,
            "get_workflow_run",
            Mock(
                return_value=StoredWorkflowRun(
                    workflow_run_id="workflow-id",
                    config_name="hello",
                    workflow_def=my_workflow().model,
                )
            ),
        )
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", Mock())
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            body="workflow-id",
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=qe_status_response,
        )

        run = my_workflow().run(remote_config)

        assert run.get_status() == State.SUCCEEDED

    def test_reconnect(
        self,
        mocked_responses,
        remote_config,
        qe_status_response,
        tmp_path,
        monkeypatch,
        mock_workflow_db_location,
        patch_config_location,
    ):
        # GIVEN
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            body="workflow-id",
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=qe_status_response,
        )
        monkeypatch.setattr(Path, "cwd", Mock(return_value=tmp_path))
        monkeypatch.setattr(Path, "home", Mock(return_value=tmp_path))

        # WHEN
        run = my_workflow().run(remote_config)
        id = run.run_id
        del run

        run_reconnect = sdk.WorkflowRun.by_id(
            id,
            config_save_file=tmp_path / "config.json",
        )

        # THEN
        assert run_reconnect.get_status() == State.SUCCEEDED
