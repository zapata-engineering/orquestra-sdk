################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
"""Snippets and tests used in the "Remote Workflows" tutorial."""

import subprocess
import sys
from pathlib import Path

import pytest
import pytest_httpserver

from orquestra.sdk.examples.exportable_wf import my_workflow

from .parsers import get_snippet_as_str


class Snippets:
    @staticmethod
    def workflow_defs():
        import orquestra.sdk as sdk

        @sdk.task(
            source_import=sdk.InlineImport(),
            dependency_imports=[
                sdk.GitImport(
                    repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",  # noqa: E501
                    git_ref="main",
                ),
            ],
        )
        def hello_orquestra() -> str:
            return "Hello Orquestra!"

        @sdk.workflow
        def hello_orquestra_wf():
            return [hello_orquestra()]

        # </snippet>

    @staticmethod
    def execute_workflow():
        from remote_workflow import hello_orquestra_wf

        import orquestra.sdk as sdk

        # Import the workflow and configuration
        workflow = hello_orquestra_wf()
        remote_config = sdk.RuntimeConfig.load(config_name="<paste your config name>")
        # Start the workflow
        workflow_run = workflow.run(remote_config)

        # Get the workflow run ID
        print(workflow_run.run_id)
        # </snippet>

    @staticmethod
    def get_results():
        from orquestra import sdk

        # Use this to get access to a workflow run if you closed your REPL.
        wf_run = sdk.WorkflowRun.by_id("<paste workflow run ID here>")

        # This blocks the process until the workflow is completed.
        wf_run.wait_until_finished()

        # Should print "Hello, Orquestra!"
        print(wf_run.get_results())
        # </snippet>


@pytest.mark.slow
class TestSnippets:
    wf_id = "test_workflow_id"
    config_name = "test-remote-tutorial-config"

    @staticmethod
    @pytest.fixture
    def define_test_config(
        httpserver: pytest_httpserver.HTTPServer,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        from orquestra import sdk

        monkeypatch.setenv("ORQ_CONFIG_PATH", str(tmp_path / "config.json"))
        sdk.RuntimeConfig.ce(uri=f"http://127.0.0.1:{httpserver.port}", token="nice")

    @staticmethod
    @pytest.mark.dependency()
    def test_execute_workflow(
        change_test_dir,
        httpserver: pytest_httpserver.HTTPServer,
        change_db_location,
        define_test_config,
    ):
        # Given
        httpserver.expect_request(
            "/api/workflow-definitions", method="POST"
        ).respond_with_json({"data": {"id": "def_id"}})
        httpserver.expect_request(
            "/api/workflow-runs", method="POST"
        ).respond_with_json({"data": {"id": TestSnippets.wf_id}})

        # Prepare the project dir: 'workflow_defs.py' and 'script.py' files
        src_file = Path("./execute_workflow.py")
        file_content = get_snippet_as_str(fn=Snippets.execute_workflow)
        file_content = file_content.replace("<paste your config name>", "127")
        src_file.write_text(file_content)

        wf_defs_file = Path("./remote_workflow.py")
        wf_defs_file.write_text(get_snippet_as_str(fn=Snippets.workflow_defs))

        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(src_file)], capture_output=True)
        std_out = str(proc.stdout, "utf-8")

        # Then
        proc.check_returncode()
        assert TestSnippets.wf_id in std_out

    @staticmethod
    @pytest.mark.dependency(depends=["TestSnippets::test_execute_workflow"])
    def test_get_results(
        change_test_dir,
        httpserver: pytest_httpserver.HTTPServer,
        change_db_location,
        define_test_config,
    ):
        # Given
        httpserver.expect_request(
            "/api/workflow-runs/test_workflow_id", method="GET"
        ).respond_with_json(
            {
                "data": {
                    "id": "test_workflow_id",
                    "definitionId": "def_id",
                    "owner": "taylor.swift@zapatacomputing.com",
                    "status": {
                        "state": "SUCCEEDED",
                        "start_time": "1989-12-13T09:03:49Z",
                        "end_time": "1989-12-13T09:05:14Z",
                    },
                    "taskRuns": [],
                }
            }
        )
        httpserver.expect_request(
            "/api/workflow-definitions/def_id", method="GET"
        ).respond_with_json(
            {
                "data": {
                    "id": "def_id",
                    "created": "1989-12-13T09:03:49Z",
                    "owner": "taylor.swift@zapatacomputing.com",
                    "workflow": my_workflow().model.model_dump(),
                    "workspaceId": "eras_tour",
                    "project": "red",
                    "sdkVersion": "0.22.0",
                }
            }
        )
        httpserver.expect_request(
            "/api/run-results",
            method="GET",
            query_string="workflowRunId=test_workflow_id",
        ).respond_with_json({"data": ["result1"]})
        httpserver.expect_request(
            "/api/run-results/result1", method="GET"
        ).respond_with_json(
            {
                "results": [
                    {
                        "serialization_format": "JSON",
                        "value": '"Hello Orquestra!"',
                    }
                ],
                "type": "ComputeEngineWorkflowResult",
            }
        )

        file_content = get_snippet_as_str(fn=Snippets.get_results)
        # Replace expected string with actual wf_id that we got from previous test
        file_content = file_content.replace(
            "<paste workflow run ID here>", TestSnippets.wf_id
        )

        src_file = Path("./get_results.py")
        src_file.write_text(file_content)

        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(src_file)], capture_output=True)

        # Then
        print(proc.stdout.decode(), proc.stderr.decode())
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        assert "Hello Orquestra!" in std_out
