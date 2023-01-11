################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

import orquestra.sdk as sdk
from orquestra.sdk._base import _db
from orquestra.sdk._base.cli._corq._format import per_command
from orquestra.sdk.schema import local_database, responses, workflow_run

OK_META = responses.ResponseMetadata(
    success=True,
    code=responses.ResponseStatusCode.OK,
    message="shouldn't matter",
)


OK_STATUS = workflow_run.RunStatus(
    state=workflow_run.State.SUCCEEDED,
    start_time=datetime.fromisoformat("2022-07-19T09:59:03.144368+00:00"),
    end_time=datetime.fromisoformat("2022-07-19T09:59:03.159318+00:00"),
)


@sdk.task
def _hello_task():
    return "hello"


@sdk.workflow
def _simple_workflow():
    return [_hello_task()]


def _make_workflow_run(wf_run_id: str):
    wf_model = _simple_workflow.model
    inv_id = list(wf_model.task_invocations.keys())[0]

    return workflow_run.WorkflowRun(
        id=wf_run_id,
        workflow_def=wf_model,
        task_runs=[
            workflow_run.TaskRun(
                id="task_run_1",
                invocation_id=inv_id,
                status=OK_STATUS,
            ),
        ],
        status=OK_STATUS,
    )


def _make_mock_wf_db(wf_run_id, wf_def):
    mock_db = Mock()
    mock_db.__enter__ = Mock(return_value=mock_db)
    mock_db.__exit__ = Mock(return_value=None)
    mock_db.get_workflow_run = Mock(
        return_value=local_database.StoredWorkflowRun(
            workflow_run_id=wf_run_id,
            config_name="testing",
            workflow_def=wf_def,
        )
    )

    return mock_db


DATA_DIR = Path(__file__).parent / "data"


class TestPrettyPrintResponse:
    class TestPureResponse:
        """
        Tests formatting responses where we only use data from the response object.
        """

        @staticmethod
        @pytest.mark.parametrize(
            "resp_model, tell_tales",
            [
                (
                    responses.SubmitWorkflowDefResponse(
                        meta=OK_META,
                        workflow_runs=[
                            workflow_run.WorkflowRunMinimal(id="a_run_id"),
                        ],
                    ),
                    ["a_run_id"],
                ),
                (
                    responses.GetWorkflowRunResponse(
                        meta=OK_META,
                        workflow_runs=[],
                    ),
                    ["No workflow runs found"],
                ),
                (
                    responses.GetWorkflowRunResponse(
                        meta=OK_META,
                        workflow_runs=[_make_workflow_run("a_wf_run_id")],
                    ),
                    ["a_wf_run_id"],
                ),
                (
                    responses.GetWorkflowRunResponse(
                        meta=OK_META,
                        workflow_runs=[
                            _make_workflow_run("a_wf_run_id1"),
                            _make_workflow_run("a_wf_run_id2"),
                        ],
                    ),
                    ["a_wf_run_id1", "a_wf_run_id2"],
                ),
            ],
        )
        def test_tell_tale_in_output(capsys, resp_model, tell_tales):
            per_command.pretty_print_response(resp_model, project_dir=None)

            capture_result = capsys.readouterr()
            for tell_tale in tell_tales:
                assert tell_tale in capture_result.out

        @staticmethod
        @pytest.mark.parametrize(
            "resp_cls,resp_path,recorded_output_path",
            [
                (
                    responses.GetWorkflowRunResponse,
                    DATA_DIR / "responses" / "failed_wf_run.json",
                    DATA_DIR / "cli_outputs" / "failed_wf_run.txt",
                ),
            ],
        )
        def test_matches_recorded_output(
            capsys, resp_cls, resp_path: Path, recorded_output_path: Path
        ):
            # Given
            resp_model = resp_cls.parse_file(resp_path)

            # When
            per_command.pretty_print_response(resp_model, project_dir=None)

            # Then
            captured = capsys.readouterr()
            expected_out = recorded_output_path.read_text()
            assert captured.out == expected_out

    class TestUsingDB:
        @staticmethod
        @pytest.mark.parametrize(
            "resp_model, tell_tales",
            [
                (
                    responses.GetWorkflowRunResultsResponse(
                        meta=OK_META,
                        workflow_run_id="a_wf_run_id",
                        workflow_results=[
                            responses.JSONResult(
                                value=json.dumps("string returned from the wf"),
                            ),
                        ],
                    ),
                    ["a_wf_run_id", "string returned from the wf"],
                ),
            ],
        )
        def test_tell_tale_in_output(
            capsys, monkeypatch, tmp_path, resp_model, tell_tales
        ):
            """
            Tests formatting responses where we require loading data from the
            workflows db in addition to the response object.
            """
            monkeypatch.setattr(
                _db.WorkflowDB,
                "open_db",
                lambda: _make_mock_wf_db(
                    wf_run_id=resp_model.workflow_run_id,
                    wf_def=_simple_workflow.model,
                ),
            )

            per_command.pretty_print_response(resp_model, project_dir=tmp_path)

            capture_result = capsys.readouterr()
            for tell_tale in tell_tales:
                assert tell_tale in capture_result.out
