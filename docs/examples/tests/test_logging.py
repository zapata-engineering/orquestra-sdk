################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Logging Guide" tutorial.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from .parsers import get_snippet_as_str


class Snippets:
    @staticmethod
    def test_task_logging():
        import orquestra.sdk as sdk
        from orquestra.sdk import wfprint, workflow_logger

        @sdk.task
        def say_hello():
            logger = workflow_logger()
            logger.info("We're doing some quantum work here!")
            wfprint("Another good way to use raw prints from a workflow!")

        # </snippet>
        say_hello._TaskDef__sdk_task_body()

    @staticmethod
    def test_logging_execution():
        from tasks import say_hello

        import orquestra.sdk as sdk

        @sdk.workflow
        def wf():
            return say_hello()

        wf().run("in_process")


class TestSnippets:
    wf_id = None

    @staticmethod
    @pytest.mark.dependency()
    def test_task_logging(change_test_dir):
        # Given
        # Prepare the project dir with the task and workflow
        src_file = Path(".") / "tasks.py"
        src_file.write_text(get_snippet_as_str(fn=Snippets.test_task_logging))

        src_file = Path(".") / "workflow_defs.py"
        src_file.write_text(get_snippet_as_str(fn=Snippets.test_logging_execution))

        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(src_file)], capture_output=True)

        # Then
        proc.check_returncode()
        std_out = str(proc.stderr, "utf-8")
        assert (
            '{"run_id": "wf.local_run.0000000@tsk.task_local-0.00000", '
            '"logs": "We\'re doing some quantum work here!"}' in std_out
        )
        assert (
            '{"run_id": "wf.local_run.0000000@tsk.task_local-0.00000", '
            '"logs": "Another good way to use raw prints from a workflow!"}' in std_out
        )
