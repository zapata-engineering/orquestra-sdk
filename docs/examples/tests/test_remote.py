################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Remote Workflows" tutorial.
"""

import base64
import io
import json
import subprocess
import sys
import tarfile
import typing as t
from pathlib import Path

import pytest
import pytest_httpserver

from .parsers import get_snippet_as_str

QE_MINIMAL_CURRENT_REPRESENTATION: t.Dict[str, t.Any] = {
    "status": {
        "phase": "Succeeded",
        "startedAt": "1989-12-13T09:03:49Z",
        "finishedAt": "1989-12-13T09:05:14Z",
        "nodes": {},
    },
}

QE_STATUS_RESPONSE = {
    "id": "hello-there-abc123-r000",
    "status": "Succeeded",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION).encode()
    ).decode(),
    "completed": True,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}


QE_WORKFLOW_RESULT_JSON_DICT = {
    "hello-there-abc123-r000-2738763496": {
        "artifact-0-hello-orquestra": {
            "serialization_format": "JSON",
            "value": '"Hello Orquestra!"',
        },
        "inputs": {},
        "stepID": "hello-there-abc123-r000-2738763496",
        "stepName": "invocation-0-task-hello-orquestra",
        "workflowId": "hello-there-abc123-r000",
    },
}


def _make_result_bytes(results_dict: t.Dict[str, t.Any]) -> bytes:
    results_file_bytes = json.dumps(results_dict).encode()

    tar_buf = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=tar_buf) as tar:
        # See this for creating tars in memory:
        # https://github.com/python/cpython/issues/66404#issuecomment-1093662423
        tar_info = tarfile.TarInfo("results.json")
        tar_info.size = len(results_file_bytes)
        tar.addfile(tar_info, fileobj=io.BytesIO(results_file_bytes))

    tar_buf.seek(0)
    return tar_buf.read()


QE_WORKFLOW_RESULT_BYTES = _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT)


class Snippets:
    @staticmethod
    def workflow_defs():
        import orquestra.sdk as sdk

        @sdk.task(
            source_import=sdk.InlineImport(),
            dependency_imports=[
                sdk.GitImport(
                    repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",  # noqa: E501
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

        # Should print ("Hello, Orquestra!",)
        print(wf_run.get_results())
        # </snippet>


class TestSnippets:
    wf_id = "test_workflow_id"
    config_name = "test-remote-tutorial-config"

    @staticmethod
    @pytest.fixture
    def define_test_config(httpserver: pytest_httpserver.HTTPServer):
        from orquestra import sdk

        sdk.RuntimeConfig.qe(uri=f"http://127.0.0.1:{httpserver.port}", token="nice")

    @staticmethod
    @pytest.mark.dependency()
    def test_execute_workflow(
        change_test_dir,
        httpserver: pytest_httpserver.HTTPServer,
        change_db_location,
        define_test_config,
    ):
        # Given
        httpserver.expect_request("/v1/workflows").respond_with_data(TestSnippets.wf_id)

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
    ):
        # Given
        httpserver.expect_request("/v1/workflow").respond_with_json(QE_STATUS_RESPONSE)
        httpserver.expect_request(
            f"/v2/workflows/{TestSnippets.wf_id}/result"
        ).respond_with_data(
            QE_WORKFLOW_RESULT_BYTES, content_type="application/x-gtar-compressed"
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
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        assert "('Hello Orquestra!',)" in std_out
