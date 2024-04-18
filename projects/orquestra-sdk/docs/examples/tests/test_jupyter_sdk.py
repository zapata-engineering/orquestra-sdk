################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Snippets and tests used in the "Using the Workflow SDK with Jupyter" tutorial."""
import sys

from orquestra.sdk._client._base._testing import _reloaders

from .parsers import get_snippet_as_str


class Snippets:
    @staticmethod
    def all_notebook():
        # <snippet cell_task_wf>
        # in: a notebook cell
        from orquestra import sdk

        @sdk.task
        def task_sum_numbers(numbers: tuple):
            return sum(numbers)

        @sdk.workflow
        def wf_sum_numbers():
            numbers = (1, 2)
            return task_sum_numbers(numbers)

        # </snippet>
        # <snippet cell_wf_run>
        # in: a notebook cell
        wf_run = wf_sum_numbers().run("in_process")
        # </snippet>

        # <snippet cell_results>
        # in: a notebook cell
        assert wf_run.get_results() == 3
        # </snippet>

    class TaskInAnotherFile:
        @staticmethod
        def another_task_file_tasks_py():
            # in: tasks.py
            from orquestra import sdk

            @sdk.task
            def make_greeting(first_name: str, last_name: str):
                return f"Hello, {first_name} {last_name}!"

            # </snippet>

        @staticmethod
        def another_task_file_notebook():
            # in: a notebook cell
            from tasks import make_greeting

            from orquestra import sdk

            @sdk.workflow
            def wf_hello():
                return make_greeting("Emiliano", "Zapata")

            wf_run = wf_hello().run("in_process")
            results = wf_run.get_results()

            assert results == "Hello, Emiliano Zapata!"
            # </snippet>

    class WFInAnotherFile:
        @staticmethod
        def another_wf_file_defs_py():
            # in: defs.py
            from orquestra import sdk

            @sdk.task
            def make_greeting(first_name: str, last_name: str):
                return f"Hello, {first_name} {last_name}!"

            @sdk.workflow
            def wf_hello():
                return make_greeting("Emiliano", "Zapata")

            # </snippet>

        @staticmethod
        def another_wf_file_notebook():
            # in: a notebook cell
            from defs import wf_hello

            wf_run = wf_hello().run("in_process")
            results = wf_run.get_results()

            assert results == "Hello, Emiliano Zapata!"
            # </snippet>


class TestSnippets:
    @staticmethod
    def test_all_notebook():
        # No exceptions = all good.
        Snippets.all_notebook()

    @staticmethod
    def test_task_in_another_file(monkeypatch, tmp_path):
        # Given
        monkeypatch.chdir(tmp_path)
        (tmp_path / "tasks.py").write_text(
            get_snippet_as_str(
                Snippets.TaskInAnotherFile.another_task_file_tasks_py, dedent=12
            )
        )

        with _reloaders.restore_loaded_modules():
            sys.path.append(".")

            # When
            Snippets.TaskInAnotherFile.another_task_file_notebook()

            # Then
            # No exceptions = all good.

    @staticmethod
    def test_wf_in_another_file(monkeypatch, tmp_path):
        # Given
        monkeypatch.chdir(tmp_path)
        (tmp_path / "defs.py").write_text(
            get_snippet_as_str(
                Snippets.WFInAnotherFile.another_wf_file_defs_py, dedent=12
            )
        )

        with _reloaders.restore_loaded_modules():
            sys.path.append(".")

            # When
            Snippets.WFInAnotherFile.another_wf_file_notebook()

            # Then
            # No exceptions = all good.
