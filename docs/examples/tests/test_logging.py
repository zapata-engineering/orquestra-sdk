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
        import logging

        from orquestra import sdk

        @sdk.task
        def say_hello():
            print("We're doing some quantum work here!")

            logger = logging.getLogger(__name__)
            logger.warning("Python logging module also can be used!")

        # </snippet>
        say_hello._TaskDef__sdk_task_body()

    @staticmethod
    def test_logging_execution():
        from tasks import say_hello

        from orquestra import sdk

        @sdk.workflow
        def wf():
            return say_hello()

        wf().run("in_process")


@pytest.mark.slow
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
        assert "We're doing some quantum work here!" in proc.stdout.decode()
        assert "Python logging module also can be used!" in proc.stderr.decode()
