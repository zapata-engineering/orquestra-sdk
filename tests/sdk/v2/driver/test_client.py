################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._driver._client.
"""
from typing import Any, Dict
from unittest.mock import create_autospec

import numpy as np
import pytest
import responses

import orquestra.sdk as sdk
from orquestra.sdk._base._driver import _exceptions
from orquestra.sdk._base._driver._client import DriverClient, Paginated
from orquestra.sdk._base._driver._models import GetWorkflowDefResponse, Resources
from orquestra.sdk._base._spaces._structs import ProjectRef
from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.responses import JSONResult, PickleResult
from orquestra.sdk.schema.workflow_run import RunStatus, State, TaskRun

from . import resp_mocks


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def endpoint_mocker_base(mocked_responses):
    """
    Returns a helper for mocking requests.
    Does some boilerplate required for all request mocks
    """

    def _inner(method: str, url: str, *, default_status_code: int = 200):
        def _mocker(**responses_kwargs):
            try:
                status = responses_kwargs.pop("status")
            except KeyError:
                status = default_status_code
            mocked_responses.add(
                method,
                url,
                status=status,
                **responses_kwargs,
            )

        return _mocker

    return _inner


class TestClient:
    @pytest.fixture
    def base_uri(self):
        return "https://should.not.matter.example.com"

    @pytest.fixture
    def token(self):
        return "shouldn't matter"

    @pytest.fixture
    def client(self, base_uri, token):
        return DriverClient.from_token(base_uri=base_uri, token=token)

    @pytest.fixture
    def workflow_def_id(self):
        return "00000000-0000-0000-0000-000000000000"

    @pytest.fixture
    def workflow_run_id(self):
        return "00000000-0000-0000-0000-000000000000"

    @pytest.fixture
    def task_run_id(self):
        return "00000000-0000-0000-0000-000000000000"

    @pytest.fixture
    def workflow_def(self):
        @sdk.task
        def task():
            return 1

        @sdk.workflow
        def workflow():
            return task()

        return workflow().model

    class TestWorkflowDefinitions:
        # ------ queries ------

        class TestGet:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_def_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-definitions/{workflow_def_id}",
                )

            @staticmethod
            def test_response_parsing(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                """
                Verifies that the response is correctly deserialized.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_def_response(
                        id_=workflow_def_id,
                        wf_def=workflow_def,
                    ),
                )

                returned_wf_def = client.get_workflow_def(workflow_def_id)

                assert returned_wf_def.workflow == workflow_def
                assert (
                    returned_wf_def.workspaceId
                    == "evil/emiliano.zapata@zapatacomputing.com"
                )
                assert returned_wf_def.project == "emiliano's project"
                assert returned_wf_def.sdkVersion == "0x859"

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                    json=resp_mocks.make_get_wf_def_response(
                        id_=workflow_def_id, wf_def=workflow_def
                    ),
                )

                client.get_workflow_def(workflow_def_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_missing_wf_def_id(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowDefNotFound):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L20
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowDefID):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L26
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient, workflow_def_id):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L28
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L34
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_def(workflow_def_id)

        class TestList:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-definitions",
                )

            @staticmethod
            def test_list_workflow_defs(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L18
                    json=resp_mocks.make_list_wf_def_response(
                        ids=[workflow_def_id] * 10,
                        wf_defs=[workflow_def] * 10,
                    )
                )

                defs = client.list_workflow_defs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0] == workflow_def
                assert defs.next_page_token is None
                assert defs.prev_page_token is None

            @staticmethod
            def test_list_workflow_defs_with_pagination(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L18
                    json=resp_mocks.make_list_wf_def_paginated_response(
                        ids=[workflow_def_id] * 10,
                        wf_defs=[workflow_def] * 10,
                    )
                )

                defs = client.list_workflow_defs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0] == workflow_def
                assert (
                    defs.next_page_token == "1989-12-13T00:00:00.000000Z,"
                    "00000000-0000-0000-0000-0000000000000"
                )

            @staticmethod
            @pytest.mark.parametrize(
                "kwargs,params",
                [
                    ({}, {}),
                    ({"page_size": 100}, {"pageSize": 100}),
                    ({"page_token": "abc"}, {"pageToken": "abc"}),
                    (
                        {"page_size": 100, "page_token": "abc"},
                        {"pageSize": 100, "pageToken": "abc"},
                    ),
                ],
            )
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, kwargs: Dict, params: Dict
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_list_wf_def_response(ids=[], wf_defs=[]),
                    match=[responses.matchers.query_param_matcher(params)],
                    # Based on:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L8
                    status=200,
                )

                client.list_workflow_defs(**kwargs)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                    json=resp_mocks.make_list_wf_def_response(ids=[], wf_defs=[]),
                )

                client.list_workflow_defs()

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L25
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.list_workflow_defs()

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L27
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.list_workflow_defs()

            @staticmethod
            def test_unknown_error(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L29
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.list_workflow_defs()

        # ------ mutations ------

        class TestCreate:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                return endpoint_mocker_base(
                    responses.POST,
                    f"{base_uri}/api/workflow-definitions",
                    default_status_code=201,
                )

            @staticmethod
            @pytest.mark.parametrize(
                "project,params",
                [
                    (None, {}),
                    (
                        ProjectRef(workspace_id="a", project_id="b"),
                        {"workspaceId": "a", "projectId": "b"},
                    ),
                ],
            )
            def test_params_encoding(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                workflow_def: WorkflowDef,
                project,
                params,
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_create_wf_def_response(id_=workflow_def_id),
                    match=[
                        responses.matchers.json_params_matcher(workflow_def.dict()),
                        responses.matchers.query_param_matcher(params),
                    ],
                    # Based on:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definitions.yaml#L42
                    status=201,
                )

                client.create_workflow_def(workflow_def, project)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                    # Based on:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definitions.yaml#L42
                    status=201,
                    json=resp_mocks.make_create_wf_def_response(id_=workflow_def_id),
                )

                client.create_workflow_def(workflow_def, None)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_definition(
                endpoint_mocker,
                client: DriverClient,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L50
                    status=400,
                    json=resp_mocks.make_error_response("Bad definition", "details"),
                )

                with pytest.raises(_exceptions.InvalidWorkflowDef):
                    client.create_workflow_def(workflow_def, None)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_def: WorkflowDef
            ):
                endpoint_mocker(
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L56
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    client.create_workflow_def(workflow_def, None)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_def: WorkflowDef
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L58
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.create_workflow_def(workflow_def, None)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_def: WorkflowDef
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definitions.yaml#L52
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.create_workflow_def(workflow_def, None)

        class TestDelete:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str, workflow_def_id):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                return endpoint_mocker_base(
                    responses.DELETE,
                    f"{base_uri}/api/workflow-definitions/{workflow_def_id}",
                    default_status_code=204,
                )

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token, workflow_def_id
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.delete_workflow_def(workflow_def_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_missing_wf_def(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L56
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowDefNotFound):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_invalid_id(endpoint_mocker, client: DriverClient, workflow_def_id):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L48
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowDefID):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L56
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L58
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L66
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.delete_workflow_def(workflow_def_id)

    class TestWorkflowRuns:
        @pytest.fixture
        def workflow_run_status(self):
            return RunStatus(state=State.WAITING, start_time=None, end_time=None)

        @pytest.fixture
        def task_run_status(self):
            return RunStatus(state=State.WAITING, start_time=None, end_time=None)

        @pytest.fixture
        def workflow_run_tasks(self, task_run_id, task_run_status):
            return [
                TaskRun(id=task_run_id, invocation_id=f"{i}", status=task_run_status)
                for i in range(3)
            ]

        @pytest.fixture
        def resources(self):
            return Resources(cpu=None, memory=None, gpu=None)

        class TestGet:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str, workflow_run_id):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-runs/{workflow_run_id}",
                )

            @staticmethod
            @pytest.fixture
            def mock_get_workflow_def(
                client: DriverClient,
                workflow_def: WorkflowDef,
                monkeypatch: pytest.MonkeyPatch,
            ):
                mock = create_autospec(GetWorkflowDefResponse)
                mock.workflow = workflow_def
                function_mock = create_autospec(client.get_workflow_def)
                function_mock.return_value = mock
                monkeypatch.setattr(client, "get_workflow_def", function_mock)

            @staticmethod
            def test_invalid_wf_run_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L20
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_missing_wf_run(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_missing_task_run_status(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
                workflow_run_status: RunStatus,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_missing_task_run_status(
                        workflow_run_id, workflow_def_id, workflow_run_status
                    ),
                )

                wf_run = client.get_workflow_run(workflow_run_id)

                assert len(wf_run.task_runs) == 1
                assert wf_run.task_runs[0].status == RunStatus(
                    state=State.WAITING, end_time=None, start_time=None
                )

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                token,
                workflow_run_id,
                workflow_def_id,
                workflow_run_status,
                workflow_run_tasks,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_response(
                        id_=workflow_run_id,
                        workflow_def_id=workflow_def_id,
                        status=workflow_run_status,
                        task_runs=workflow_run_tasks,
                    ),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.get_workflow_run(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L26
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L28
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L36
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run(workflow_run_id)

        class TestList:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-runs",
                )

            @staticmethod
            @pytest.fixture
            def mock_get_workflow_def(
                client: DriverClient,
                workflow_def: WorkflowDef,
                monkeypatch: pytest.MonkeyPatch,
            ):
                mock = create_autospec(GetWorkflowDefResponse)
                mock.workflow = workflow_def
                function_mock = create_autospec(client.get_workflow_def)
                function_mock.return_value = mock
                monkeypatch.setattr(client, "get_workflow_def", function_mock)

            @staticmethod
            def test_list_workflow_runs(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L14
                    json=resp_mocks.make_list_wf_run_response(
                        ids=[workflow_run_id] * 10,
                        workflow_def_ids=[workflow_def_id] * 10,
                    )
                )

                defs = client.list_workflow_runs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0].id == workflow_run_id
                assert defs.next_page_token is None
                assert defs.prev_page_token is None

            @staticmethod
            def test_list_workflow_runs_with_pagination(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L14
                    json=resp_mocks.make_list_wf_run_paginated_response(
                        ids=[workflow_run_id] * 10,
                        workflow_def_ids=[workflow_def_id] * 10,
                    )
                )

                defs = client.list_workflow_runs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0].id == workflow_run_id
                assert (
                    defs.next_page_token == "1989-12-13T00:00:00.000000Z,"
                    "00000000-0000-0000-0000-0000000000000"
                )

            @staticmethod
            @pytest.mark.parametrize(
                "kwargs,params",
                [
                    ({}, {}),
                    ({"workflow_def_id": "0"}, {"workflowDefinitionID": "0"}),
                    ({"page_size": 100}, {"pageSize": 100}),
                    ({"page_token": "abc"}, {"pageToken": "abc"}),
                    (
                        {"page_size": 100, "page_token": "abc"},
                        {"pageSize": 100, "pageToken": "abc"},
                    ),
                ],
            )
            def test_params_encoding(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                kwargs: Dict,
                params: Dict,
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_list_wf_def_response(ids=[], wf_defs=[]),
                    match=[responses.matchers.query_param_matcher(params)],
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L14
                    status=200,
                )

                client.list_workflow_runs(**kwargs)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                token,
                workflow_run_id,
                workflow_def_id,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_list_wf_run_response(
                        ids=[workflow_run_id] * 10,
                        workflow_def_ids=[workflow_def_id] * 10,
                    ),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.list_workflow_runs()

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L33
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.list_workflow_runs()

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L35
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.list_workflow_runs()

            @staticmethod
            def test_unknown_error(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L37
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.list_workflow_runs()

        class TestCreate:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.POST,
                    f"{base_uri}/api/workflow-runs",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L43
                    default_status_code=201,
                )

            @staticmethod
            def test_invalid_request(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                resources: Resources,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_error_response("Bad definition", "details"),
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L45
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunRequest):
                    _ = client.create_workflow_run(workflow_def_id, resources)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_submit_wf_run_response(workflow_def_id),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.create_workflow_run(workflow_def_id, resources)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L63
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.create_workflow_run(workflow_def_id, resources)

            @staticmethod
            def test_forbidden(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L65
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.create_workflow_run(workflow_def_id, resources)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L67
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.create_workflow_run(workflow_def_id, resources)

        class TestTerminate:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_run_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    method=responses.POST,
                    url=f"{base_uri}/api/workflow-runs/{workflow_run_id}/terminate",
                    # Specified in
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L11
                    default_status_code=204,
                )

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L17
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.terminate_workflow_run(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.terminate_workflow_run(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker,
                client: DriverClient,
                workflow_run_id: str,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L13
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.terminate_workflow_run(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L15
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.terminate_workflow_run(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L23
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.terminate_workflow_run(workflow_run_id)

    class TestWorkflowRunArtifacts:
        @pytest.fixture
        def workflow_run_artifact_id(self):
            return "00000000-0000-0000-0000-000000000000"

        class TestGetWorkflowRunArtifacts:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/artifacts",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L13
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifacts_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_workflow_run_artifacts(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L32
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L22
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifacts_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_artifacts(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L28
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L30
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L38
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

        class TestGetWorkflowRunArtifact:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_run_artifact_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/artifacts/{workflow_run_artifact_id}",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            @pytest.mark.parametrize("obj", [None, 100, "hello", np.eye(10)])
            def test_objects(
                endpoint_mocker,
                client: DriverClient,
                workflow_run_artifact_id: str,
                obj: Any,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifact_response(obj),
                )

                result = client.get_workflow_run_artifact(workflow_run_artifact_id)

                assert isinstance(result, (JSONResult, PickleResult))

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L29
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunArtifactNotFound):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L19
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunArtifactID):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token: str,
                workflow_run_artifact_id: str,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifact_response(None),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L25
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L27
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L35
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

    class TestWorkflowRunResults:
        @pytest.fixture
        def workflow_run_result_id(self):
            return "00000000-0000-0000-0000-000000000000"

        class TestGetWorkflowRunResults:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/run-results",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L13
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_results_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_workflow_run_results(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_wf_run_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L22
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_missing_wf_run(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L32
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_results_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_results(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L28
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L30
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L38
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_results(workflow_run_id)

        class TestGetWorkflowRunResult:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_run_result_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/run-results/{workflow_run_result_id}",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token: str,
                workflow_run_result_id: str,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_result_legacy_response(None),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_result(workflow_run_result_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L29
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunResultNotFound):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L19
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunResultID):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L25
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L27
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L35
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

    class TestWorkflowLogs:
        class TestWorkflowRunLogs:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-run-logs",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            def test_logs_decode(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                # Given
                endpoint_mocker(
                    body=resp_mocks.make_get_wf_run_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                # When
                messages = client.get_workflow_run_logs(workflow_run_id)

                # Then
                assert messages == resp_mocks.make_get_wf_run_logs_messages()

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    # json=resp_mocks.make_get_wf_run_logs_response(),
                    body=resp_mocks.make_get_wf_run_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_workflow_run_logs(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L18
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunLogsNotFound):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_zlib_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    body=b"invalid bytes",
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                with pytest.raises(_exceptions.WorkflowRunLogsNotReadable):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    body=resp_mocks.make_get_wf_run_logs_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_logs(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L24
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L26
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L34
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_logs(workflow_run_id)

        class TestSystemLogs:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-run-logs/system",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/92d9ff32189c580fd0a2ff6eec03cc977fd01502/openapi/src/resources/workflow-run-system-logs.yaml
                    default_status_code=200,
                )

            @staticmethod
            def test_logs_decode(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    body=resp_mocks.make_get_wf_run_system_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                assert client.get_system_logs(workflow_run_id) == [
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 ADDED Pending",
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Pending\n  Pod Conditions:\n  -- PodScheduled False (Unschedulable): 0/4 nodes are available: 4 pod has unbound immediate PersistentVolumeClaims. preemption: 0/4 nodes are available: 4 Preemption is not helpful for scheduling. ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Pending\n  Pod Conditions:\n  -- PodScheduled False (Unschedulable): 0/4 nodes are available: 1 Insufficient cpu, 3 node(s) didn't match Pod's node affinity/selector. preemption: 0/4 nodes are available: 1 No preemption victims found for incoming pod, 3 Preemption is not helpful for scheduling. ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Pending\n  Pod Conditions:\n  -- PodScheduled True",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Pending\n  Pod Conditions:\n  -- Initialized False (ContainersNotInitialized): containers with incomplete status: [linkerd-init] \n  -- Ready False (ContainersNotReady): containers with unready status: [linkerd-proxy workflow-run fluent-bit] \n  -- ContainersReady False (ContainersNotReady): containers with unready status: [linkerd-proxy workflow-run fluent-bit] \n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Waiting (PodInitializing)\n  -- linkerd-proxy Waiting (PodInitializing)\n  -- workflow-run Waiting (PodInitializing)",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Pending\n  Pod Conditions:\n  -- Initialized True\n  -- Ready False (ContainersNotReady): containers with unready status: [linkerd-proxy workflow-run fluent-bit] \n  -- ContainersReady False (ContainersNotReady): containers with unready status: [linkerd-proxy workflow-run fluent-bit] \n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Waiting (PodInitializing)\n  -- linkerd-proxy Waiting (PodInitializing)\n  -- workflow-run Waiting (PodInitializing)",  # noqa: E501
                    "2023-06-02 08:50:32,701\tWARNING services.py:1780 -- WARNING: The object store is using /tmp instead of /dev/shm because /dev/shm has only 67108864 bytes available. This will harm performance! You may be able to free up space by deleting files in /dev/shm. If you are inside a Docker container, you can increase /dev/shm size by passing '--shm-size=0.60gb' to 'docker run' (or add it to the run_options list in a Ray cluster config). Make sure to set this to more than 30% of available RAM.",  # noqa: E501
                    "2023-06-02 08:50:29,943\tINFO usage_lib.py:461 -- Usage stats collection is enabled by default without user confirmation because this terminal is detected to be non-interactive. To disable this, add `--disable-usage-stats` to the command that starts the cluster, or run the following command: `ray disable-usage-stats` before starting the cluster. See https://docs.ray.io/en/master/cluster/usage-stats.html for more details.",  # noqa: E501
                    "2023-06-02 08:50:29,944\tINFO scripts.py:710 -- \x1b[37mLocal node IP\x1b[39m: \x1b[1m10.200.135.120\x1b[22m",  # noqa: E501
                    "2023-06-02 08:50:32,873\tSUCC scripts.py:747 -- \x1b[32m--------------------\x1b[39m",  # noqa: E501
                    "2023-06-02 08:50:32,873\tSUCC scripts.py:748 -- \x1b[32mRay runtime started.\x1b[39m",  # noqa: E501
                    "2023-06-02 08:50:32,873\tSUCC scripts.py:749 -- \x1b[32m--------------------\x1b[39m",  # noqa: E501
                    "2023-06-02 08:50:32,873\tINFO scripts.py:751 -- \x1b[36mNext steps\x1b[39m",  # noqa: E501
                    "2023-06-02 08:50:32,873\tINFO scripts.py:752 -- To connect to this Ray runtime from another node, run",  # noqa: E501
                    "2023-06-02 08:50:32,873\tINFO scripts.py:755 -- \x1b[1m  ray start --address='10.200.135.120:6379'\x1b[22m",  # noqa: E501
                    "2023-06-02 08:50:32,873\tINFO scripts.py:771 -- Alternatively, use the following Python code:",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:773 -- \x1b[35mimport\x1b[39m\x1b[26m ray",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:777 -- ray\x1b[35m.\x1b[39m\x1b[26minit(address\x1b[35m=\x1b[39m\x1b[26m\x1b[33m'auto'\x1b[39m\x1b[26m)",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:790 -- To see the status of the cluster, use",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:791 --   \x1b[1mray status\x1b[22m\x1b[26m",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:794 -- To monitor and debug Ray, view the dashboard at ",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:795 --   \x1b[1m10.200.135.120:8265\x1b[22m\x1b[26m",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:801 -- \x1b[4mIf connection fails, check your firewall settings and network configuration.\x1b[24m",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:809 -- To terminate the Ray runtime, run",  # noqa: E501
                    "2023-06-02 08:50:32,874\tINFO scripts.py:810 -- \x1b[1m  ray stop\x1b[22m",  # noqa: E501
                    "INFO:root:Connecting to NATS server at nats://nats.nats.svc.cluster.local:4222. Will publish to subjects NatsVars(wf_status_changes_subject='workflow.states', wf_run_results_created_subject='workflow.run.result.created', wf_artifact_created_subject='workflow.artifact.created', wf_termination_subject='workflow.run.terminated.hello_orquestra_wf-ZrioL-r000', wf_termination_stream='workflow-run-terminated-stream', wf_termination_customer='wf-driver-ray@workflow-run-terminated-consumer', wf_eoe_string_subjects=['workflow.logs.ray.hello_orquestra_wf-ZrioL-r000']).",  # noqa: E501
                    "2023-06-02 08:50:34,729\tINFO worker.py:1364 -- Connecting to existing Ray cluster at address: 10.200.135.120:6379...",  # noqa: E501
                    "2023-06-02 08:50:34,736\tINFO worker.py:1544 -- Connected to Ray cluster. View the dashboard at \x1b[1m\x1b[32m10.200.135.120:8265 \x1b[39m\x1b[22m",  # noqa: E501
                    "2023-06-02 08:50:34,758\tINFO workflow_access.py:356 -- Initializing workflow manager...",  # noqa: E501
                    '2023-06-02 08:50:36,708\tINFO api.py:203 -- Workflow job created. [id="hello_orquestra_wf-ZrioL-r000"].',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m 2023-06-02 08:50:36,979\tINFO workflow_executor.py:86 -- Workflow job [id=hello_orquestra_wf-ZrioL-r000] started.",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:37.031979683     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "INFO:root:Workflow Run created with global ID hello_orquestra_wf-ZrioL-r000.",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:37.647010247     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:38.472377038     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:41.315081740     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:41.940183950     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:42.729892179     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:45.493200183     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:46.110986629     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "\x1b[2m\x1b[33m(raylet)\x1b[0m E0602 08:50:46.823247649     140 fork_posix.cc:76]           Other threads are currently calling into gRPC, skipping fork() handlers",  # noqa: E501
                    "2023-06-02 08:50:49,323\tERROR worker.py:399 -- Unhandled error (suppress with 'RAY_IGNORE_UNHANDLED_ERRORS=1'): \x1b[36mray::WorkflowManagementActor.execute_workflow()\x1b[39m (pid=215, ip=10.200.135.120, repr=<ray.workflow.workflow_access.WorkflowManagementActor object at 0x7f04148a0c40>)",  # noqa: E501
                    "ray.exceptions.RuntimeEnvSetupError: Failed to set up runtime environment.",  # noqa: E501
                    "\x1b[36mray::WorkflowManagementActor.execute_workflow()\x1b[39m (pid=215, ip=10.200.135.120, repr=<ray.workflow.workflow_access.WorkflowManagementActor object at 0x7f04148a0c40>)",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/site-packages/ray/dashboard/modules/runtime_env/runtime_env_agent.py", line 355, in _create_runtime_env_with_retry',  # noqa: E501
                    "    runtime_env_context = await asyncio.wait_for(",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/asyncio/tasks.py", line 479, in wait_for',  # noqa: E501
                    "    return fut.result()",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/site-packages/ray/dashboard/modules/runtime_env/runtime_env_agent.py", line 310, in _setup_runtime_env',  # noqa: E501
                    "    await create_for_plugin_if_needed(",  # noqa: E501
                    "ray._private.runtime_env.utils.SubprocessCalledProcessError: Run cmd[9] failed with the following details.",  # noqa: E501
                    "Command '['/tmp/ray/session_2023-06-02_08-50-29_944743_10/runtime_resources/pip/5e83a2cad736e67605dd0a40b34d623957c7f9f8/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-06-02_08-50-29_944743_10/runtime_resources/pip/5e83a2cad736e67605dd0a40b34d623957c7f9f8/requirements.txt']' returned non-zero exit status 1.",  # noqa: E501
                    "Last 50 lines of stdout:",  # noqa: E501
                    "    Collecting git+ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git@main (from -r /tmp/ray/session_2023-06-02_08-50-29_944743_10/runtime_resources/pip/5e83a2cad736e67605dd0a40b34d623957c7f9f8/requirements.txt (line 1))",  # noqa: E501
                    "      Cloning ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git (to revision main) to /tmp/pip-req-build-i_os9yla",  # noqa: E501
                    "      Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git' /tmp/pip-req-build-i_os9yla",  # noqa: E501
                    "      Host key verification failed.",  # noqa: E501
                    "      fatal: Could not read from remote repository.",  # noqa: E501
                    "      Please make sure you have the correct access rights",  # noqa: E501
                    "      and the repository exists.",  # noqa: E501
                    "      error: subprocess-exited-with-error",  # noqa: E501
                    "  ",  # noqa: E501
                    "      × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git' /tmp/pip-req-build-i_os9yla did not run successfully.",  # noqa: E501
                    "      │ exit code: 128",  # noqa: E501
                    "      ╰─> See above for output.",  # noqa: E501
                    "  ",  # noqa: E501
                    "      note: This error originates from a subprocess, and is likely not a problem with pip.",  # noqa: E501
                    "    error: subprocess-exited-with-error",  # noqa: E501
                    "    × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git' /tmp/pip-req-build-i_os9yla did not run successfully.",  # noqa: E501
                    "    │ exit code: 128",  # noqa: E501
                    "    ╰─> See above for output.",  # noqa: E501
                    "    note: This error originates from a subprocess, and is likely not a problem with pip.",  # noqa: E501
                    "The above exception was the direct cause of the following exception:",  # noqa: E501
                    "\x1b[36mray::WorkflowManagementActor.execute_workflow()\x1b[39m (pid=215, ip=10.200.135.120, repr=<ray.workflow.workflow_access.WorkflowManagementActor object at 0x7f04148a0c40>)",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/concurrent/futures/_base.py", line 439, in result',  # noqa: E501
                    "    return self.__get_result()",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/concurrent/futures/_base.py", line 391, in __get_result',  # noqa: E501
                    "    raise self._exception",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/site-packages/ray/workflow/workflow_access.py", line 209, in execute_workflow',  # noqa: E501
                    "    await executor.run_until_complete(job_id, context, wf_store)",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/site-packages/ray/workflow/workflow_executor.py", line 109, in run_until_complete',  # noqa: E501
                    "    await asyncio.gather(",  # noqa: E501
                    '  File "/usr/local/lib/python3.9/site-packages/ray/workflow/workflow_executor.py", line 356, in _handle_ready_task',  # noqa: E501
                    "    raise err",  # noqa: E501
                    "ray.workflow.exceptions.WorkflowExecutionError: Workflow[id=hello_orquestra_wf-ZrioL-r000] failed during execution.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m 2023-06-02 08:50:49,312\tERROR workflow_executor.py:306 -- Task status [FAILED] due to a system error.\t[hello_orquestra_wf-ZrioL-r000@invocation-0-task-hello-orquestra]",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m 2023-06-02 08:50:49,320\tERROR workflow_executor.py:352 -- Workflow 'hello_orquestra_wf-ZrioL-r000' failed due to Failed to set up runtime environment.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m Traceback (most recent call last):",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/dashboard/modules/runtime_env/runtime_env_agent.py", line 355, in _create_runtime_env_with_retry',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     runtime_env_context = await asyncio.wait_for(",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/asyncio/tasks.py", line 479, in wait_for',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     return fut.result()",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/dashboard/modules/runtime_env/runtime_env_agent.py", line 310, in _setup_runtime_env',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     await create_for_plugin_if_needed(",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/plugin.py", line 252, in create_for_plugin_if_needed',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     size_bytes = await plugin.create(uri, runtime_env, context, logger=logger)",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 473, in create',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     return await task",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 455, in _create_for_hash',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     await PipProcessor(",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 361, in _run',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     await self._install_pip_packages(",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 337, in _install_pip_packages',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)",  # noqa: E501
                    '\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/utils.py", line 101, in check_output_cmd',  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     raise SubprocessCalledProcessError(",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m ray._private.runtime_env.utils.SubprocessCalledProcessError: Run cmd[9] failed with the following details.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m Command '['/tmp/ray/session_2023-06-02_08-50-29_944743_10/runtime_resources/pip/5e83a2cad736e67605dd0a40b34d623957c7f9f8/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-06-02_08-50-29_944743_10/runtime_resources/pip/5e83a2cad736e67605dd0a40b34d623957c7f9f8/requirements.txt']' returned non-zero exit status 1.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m Last 50 lines of stdout:",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     Collecting git+ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git@main (from -r /tmp/ray/session_2023-06-02_08-50-29_944743_10/runtime_resources/pip/5e83a2cad736e67605dd0a40b34d623957c7f9f8/requirements.txt (line 1))",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       Cloning ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git (to revision main) to /tmp/pip-req-build-i_os9yla",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git' /tmp/pip-req-build-i_os9yla",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       Host key verification failed.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       fatal: Could not read from remote repository.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m ",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       Please make sure you have the correct access rights",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       and the repository exists.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       error: subprocess-exited-with-error",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   ",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git' /tmp/pip-req-build-i_os9yla did not run successfully.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       │ exit code: 128",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       ╰─> See above for output.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m   ",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m       note: This error originates from a subprocess, and is likely not a problem with pip.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     error: subprocess-exited-with-error",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m ",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/orquestra-workflow-sdk.git' /tmp/pip-req-build-i_os9yla did not run successfully.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     │ exit code: 128",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     ╰─> See above for output.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m ",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m     note: This error originates from a subprocess, and is likely not a problem with pip.",  # noqa: E501
                    "\x1b[2m\x1b[36m(WorkflowManagementActor pid=215)\x1b[0m ",  # noqa: E501
                    "ERROR:root:Workflow run completed unsuccessfully",  # noqa: E501
                    "NoneType: None",  # noqa: E501
                    "INFO:root:Caught a nats.errors.TimeoutError. This is expected at the end of the task.",  # noqa: E501
                    "WARNING:root:Disconnected from NATS.",  # noqa: E501
                    "WARNING:root:NATS connection is closed.",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Running\n  Pod Conditions:\n  -- Initialized True\n  -- Ready False (ContainersNotReady): containers with unready status: [linkerd-proxy workflow-run] \n  -- ContainersReady False (ContainersNotReady): containers with unready status: [linkerd-proxy workflow-run] \n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Running\n  -- linkerd-proxy Running\n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Running\n  Pod Conditions:\n  -- Initialized True\n  -- Ready False (ContainersNotReady): containers with unready status: [workflow-run] \n  -- ContainersReady False (ContainersNotReady): containers with unready status: [workflow-run] \n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Running\n  -- linkerd-proxy Running\n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 ADDED Running\n  Pod Conditions:\n  -- Initialized True\n  -- Ready False (ContainersNotReady): containers with unready status: [workflow-run] \n  -- ContainersReady False (ContainersNotReady): containers with unready status: [workflow-run] \n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Running\n  -- linkerd-proxy Running\n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Running\n  Pod Conditions:\n  -- Initialized True\n  -- Ready False (ContainersNotReady): containers with unready status: [workflow-run] \n  -- ContainersReady False (ContainersNotReady): containers with unready status: [workflow-run] \n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Running\n  -- linkerd-proxy Running\n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Succeeded\n  Pod Conditions:\n  -- Initialized True (PodCompleted)\n  -- Ready False (PodCompleted)\n  -- ContainersReady False (PodCompleted)\n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Terminated: Exit code: 0 \n  -- linkerd-proxy Terminated: Exit code: 0 \n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 UPDATED Succeeded\n  Pod Conditions:\n  -- Initialized True (PodCompleted)\n  -- Ready False (PodCompleted)\n  -- ContainersReady False (PodCompleted)\n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Terminated: Exit code: 0 \n  -- linkerd-proxy Terminated: Exit code: 0 \n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                    "wf-run-92ae97be-6f19-43dc-9c21-29153cb55f81 DELETED Succeeded\n  Pod Conditions:\n  -- Initialized True (PodCompleted)\n  -- Ready False (PodCompleted)\n  -- ContainersReady False (PodCompleted)\n  -- PodScheduled True\n  Container Information:\n  -- fluent-bit Terminated: Exit code: 0 \n  -- linkerd-proxy Terminated: Exit code: 0 \n  -- workflow-run Terminated: Exit code: 0 ",  # noqa: E501
                ]

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id
            ):
                """Veriefies the params are correctly senf to the server."""
                endpoint_mocker(
                    body=resp_mocks.make_get_wf_run_system_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_system_logs(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2ea0f3fa410bbbc9a1b7fcffbda155aa84c4e0bd/openapi/src/resources/workflow-run-system-logs.yaml#L111
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_system_logs(workflow_run_id)

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2ea0f3fa410bbbc9a1b7fcffbda155aa84c4e0bd/openapi/src/resources/workflow-run-system-logs.yaml#L121
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunLogsNotFound):
                    _ = client.get_system_logs(workflow_run_id)

            @staticmethod
            def test_zlib_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    body=b"invalid bytes",
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                with pytest.raises(_exceptions.WorkflowRunLogsNotReadable):
                    _ = client.get_system_logs(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    body=resp_mocks.make_get_wf_run_system_logs_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_system_logs(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2ea0f3fa410bbbc9a1b7fcffbda155aa84c4e0bd/openapi/src/resources/workflow-run-system-logs.yaml#L117
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_system_logs(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2ea0f3fa410bbbc9a1b7fcffbda155aa84c4e0bd/openapi/src/resources/workflow-run-system-logs.yaml#L119
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_system_logs(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2ea0f3fa410bbbc9a1b7fcffbda155aa84c4e0bd/openapi/src/resources/workflow-run-system-logs.yaml#L131
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_system_logs(workflow_run_id)

        class TestTaskRunLogs:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/task-run-logs",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, task_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_task_run_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"taskRunId": task_run_id}
                        )
                    ],
                )

                _ = client.get_task_run_logs(task_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, task_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_task_run_logs_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_task_run_logs(task_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, task_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L18
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_task_run_logs(task_run_id)

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient, task_run_id: str):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L20
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_task_run_logs(task_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, task_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L22
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_task_run_logs(task_run_id)

    class TestWorkspaces:
        class TestListWorkspaces:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/catalog/workspaces",
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(endpoint_mocker, client: DriverClient):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_list_workspaces_repsonse(),
                )

                _ = client.list_workspaces()

            @staticmethod
            @pytest.mark.parametrize(
                "status, exception",
                [
                    (401, _exceptions.InvalidTokenError),
                    (403, _exceptions.ForbiddenError),
                    (500, _exceptions.UnknownHTTPError),
                ],
            )
            def test_error_codes(
                endpoint_mocker, client: DriverClient, status, exception
            ):
                endpoint_mocker(
                    status=status,
                )

                with pytest.raises(exception):
                    _ = client.list_workspaces()

    class TestProjects:
        class TestListProjects:
            @staticmethod
            @pytest.fixture
            def workspace_id():
                return "cool"

            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, workspace_id, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                workspace_zri = f"zri:v1::0:system:resource_group:{workspace_id}"

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/catalog/workspaces/{workspace_zri}/projects",
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workspace_id
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_list_projects_repsonse(),
                )

                _ = client.list_projects(workspace_id)

            @staticmethod
            @pytest.mark.parametrize(
                "status, exception",
                [
                    (400, _exceptions.InvalidWorkspaceZRI),
                    (401, _exceptions.InvalidTokenError),
                    (403, _exceptions.ForbiddenError),
                    (500, _exceptions.UnknownHTTPError),
                ],
            )
            def test_error_codes(
                endpoint_mocker, client: DriverClient, workspace_id, status, exception
            ):
                endpoint_mocker(
                    status=status,
                )

                with pytest.raises(exception):
                    _ = client.list_projects(workspace_id)

    class TestGetLoginUrl:
        @staticmethod
        @pytest.fixture
        def login_uri():
            return "https://example.com/login/url"

        @staticmethod
        @pytest.fixture
        def port():
            return 8080

        @staticmethod
        @pytest.fixture
        def endpoint_mocker(endpoint_mocker_base, base_uri: str):
            """
            Returns a helper for mocking requests. Assumes that most of the tests
            inside this class contain a very similar set up.
            """
            return endpoint_mocker_base(
                responses.GET,
                f"{base_uri}/api/login",
                default_status_code=307,
            )

        @staticmethod
        def test_params_encoding(
            endpoint_mocker,
            client: DriverClient,
            login_uri: str,
            port: int,
        ):
            """
            Verifies that params are correctly sent to the server.
            """
            endpoint_mocker(
                headers={"location": login_uri},
            )

            url = client.get_login_url(port)

            assert url == login_uri

        @staticmethod
        @pytest.mark.parametrize(
            "status, exception",
            [
                (500, _exceptions.UnknownHTTPError),
            ],
        )
        def test_error_codes(
            endpoint_mocker, client: DriverClient, port: int, status, exception
        ):
            endpoint_mocker(
                status=status,
            )

            with pytest.raises(exception):
                _ = client.get_login_url(port)

    class TestWorkflowProject:
        # ------ queries ------

        class TestGet:
            @pytest.fixture
            def workflow_run_status(self):
                return RunStatus(state=State.WAITING, start_time=None, end_time=None)

            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str, workflow_run_id):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-runs/{workflow_run_id}",
                )

            @pytest.fixture
            def workflow_workspace(self):
                return "my workspace"

            @pytest.fixture
            def workflow_project(self):
                return "my project"

            @staticmethod
            @pytest.fixture
            def mock_get_workflow_def(
                workflow_workspace,
                workflow_project,
                client: DriverClient,
                monkeypatch: pytest.MonkeyPatch,
            ):
                mock = create_autospec(GetWorkflowDefResponse)
                mock.workspaceId = workflow_workspace
                mock.project = workflow_project
                function_mock = create_autospec(client.get_workflow_def)
                function_mock.return_value = mock
                monkeypatch.setattr(client, "get_workflow_def", function_mock)

            @staticmethod
            def test_invalid_wf_run_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L20
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_project(workflow_run_id)

            @staticmethod
            def test_missing_wf_run(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_project(workflow_run_id)

            @staticmethod
            def test_happy_path(
                endpoint_mocker,
                mock_get_workflow_def,
                workflow_workspace,
                workflow_project,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
                workflow_run_status: RunStatus,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_missing_task_run_status(
                        workflow_run_id, workflow_def_id, workflow_run_status
                    ),
                )

                project = client.get_workflow_project(workflow_run_id)

                assert project.workspace_id == workflow_workspace
                assert project.project_id == workflow_project

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                token,
                workflow_run_id,
                workflow_def_id,
                workflow_run_status,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_missing_task_run_status(
                        id_=workflow_run_id,
                        workflow_def_id=workflow_def_id,
                        status=workflow_run_status,
                    ),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.get_workflow_project(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L26
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_project(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L28
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_project(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L36
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_project(workflow_run_id)
