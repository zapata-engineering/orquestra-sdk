################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Running locally with Ray" tutorial.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from .parsers import get_snippet_as_str


class Snippets:
    @staticmethod
    def workflow_defs():
        import orquestra.sdk as sdk

        @sdk.task(source_import=sdk.InlineImport())
        def hello_orquestra() -> str:
            return "Hello Orquestra!"

        @sdk.workflow
        def hello_orquestra_wf():
            return [hello_orquestra()]

        # </snippet>

    @staticmethod
    def execute_workflow():
        from workflow_defs import hello_orquestra_wf

        # Start the workflow
        wf_run = hello_orquestra_wf().run("local")

        # A workflow can be executed multiple times. Run ID can be used later to
        # identify a single run.
        print(wf_run.run_id)

        # Should print "RUNNING or SUCCEEDED"
        print(wf_run.get_status())
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


@pytest.mark.slow
class TestSnippets:
    wf_id = None

    @staticmethod
    @pytest.mark.dependency()
    def test_execute_workflow(change_test_dir, shared_ray_conn, change_db_location):
        # Given

        # Prepare the project dir: 'workflow_defs.py' and 'script.py' files
        src_file = Path("./execute_workflow.py")
        src_file.write_text(get_snippet_as_str(fn=Snippets.execute_workflow))

        wf_defs_file = Path("./workflow_defs.py")
        wf_defs_file.write_text(get_snippet_as_str(fn=Snippets.workflow_defs))

        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(src_file)], capture_output=True)

        # Then
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        assert "RUNNING" in std_out or "SUCCEEDED" in std_out
        TestSnippets.wf_id = std_out.split()[0]

    @staticmethod
    @pytest.mark.dependency(depends=["TestSnippets::test_execute_workflow"])
    def test_get_results(change_test_dir, shared_ray_conn, change_db_location):
        # Given

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
