################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Parametrized Workflows" tutorial.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from .parsers import get_snippet_as_str


class Snippets:
    @staticmethod
    def parametrized_workflow():
        import orquestra.sdk as sdk

        @sdk.task(source_import=sdk.InlineImport())
        def add(a, b):
            return a + b

        @sdk.workflow
        def parametrized_workflow(a, b):
            return add(a, b)

        # </snippet>

    @staticmethod
    def execute_single_workflow():
        from parametrized_workflow import parametrized_workflow

        import orquestra.sdk as sdk

        wf = parametrized_workflow(1, 2)

        run = wf.run("local")

        print(run.run_id)

        # Should print "RUNNING or SUCCEEDED"
        print(run.get_status())
        # </snippet>

    @staticmethod
    def get_results():
        from orquestra import sdk

        # Use this to get access to a workflow run if you closed your REPL.
        wf_run = sdk.WorkflowRun.by_id("<paste workflow run ID here>")

        # This blocks the process until the workflow is completed.
        wf_run.wait_until_finished()

        # Should print (3,)
        print(wf_run.get_results())
        # </snippet>

    @staticmethod
    def execute_multiple_workflows():
        from parametrized_workflow import parametrized_workflow

        import orquestra.sdk as sdk

        # creating 5 workflows
        workflows = [parametrized_workflow(a, a + 1) for a in range(5)]

        # Submit all of them
        workflow_runs = [workflow.run("local") for workflow in workflows]

        # wait for every workflow to be finished
        [workflow_run.wait_until_finished() for workflow_run in workflow_runs]

        # print the result for each of the worklfow run
        for workflow_run in workflow_runs:
            print(workflow_run.get_results())
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
        src_file.write_text(get_snippet_as_str(fn=Snippets.execute_single_workflow))

        wf_defs_file = Path("./parametrized_workflow.py")
        wf_defs_file.write_text(get_snippet_as_str(fn=Snippets.parametrized_workflow))

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
        assert "(3,)" in std_out

    @staticmethod
    def test_execute_multiple_workflows(
        change_test_dir, shared_ray_conn, change_db_location
    ):
        # Given

        # Prepare the project dir: 'workflow_defs.py' and 'script.py' files
        src_file = Path("./execute_multiple_workflows.py")
        src_file.write_text(get_snippet_as_str(fn=Snippets.execute_multiple_workflows))

        wf_defs_file = Path("./parametrized_workflow.py")
        wf_defs_file.write_text(get_snippet_as_str(fn=Snippets.parametrized_workflow))

        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(src_file)], capture_output=True)

        # Then
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        for i in range(5):
            assert f"({i*2+1},)" in std_out
