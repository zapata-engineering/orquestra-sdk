################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Dependency Installation" guide.
"""
import typing as t

import pytest

from orquestra import sdk
from orquestra.sdk.schema import ir


class Snippets:
    # --- "Good Practice" section
    @staticmethod
    def good_practice_defaults():
        @sdk.task
        def simple_task():
            ...

        # </snippet>
        return simple_task

    @staticmethod
    def good_practice_python_imports():
        @sdk.task(dependency_imports=[sdk.PythonImports("torch~=2.0")])
        def depends_on_torch():
            ...

        # </snippet>
        return depends_on_torch

    @staticmethod
    def good_practice_github_import():
        # file: src/my_proj/tasks.py
        @sdk.task(
            source_import=sdk.GithubImport(
                "zapatacomputing/my_source_repo",
                git_ref="feat/my-feature-branch",
                username="my-github-username",
                personal_access_token=sdk.Secret("my_pat"),
            )
        )
        def my_task():
            ...

        # </snippet>
        return my_task

    @staticmethod
    def good_practice_git_import_with_auth():
        from orquestra import sdk

        @sdk.task(
            source_import=sdk.GitImportWithAuth(
                repo_url="https://example.com/some_person/some_repo.git",
                git_ref="v1.0.1",
                username="my-git-username",
                auth_secret=sdk.Secret(
                    "access-token",
                    config_name="prod-d",
                    # Orquestra Workspace where the secret is defined. You can read it
                    # on Orquestra Portal website.
                    workspace_id="ws1",
                ),
            )
        )
        def demo_task():
            pass

        # </snippet>
        return demo_task

    # --- "Imports In Detail" section ---

    @staticmethod
    def simple_task_explicit():
        @sdk.task(source_import=sdk.InlineImport(), dependency_imports=[])
        def simple_task():
            ...

        # </snippet>
        return simple_task

    @staticmethod
    def python_imports():
        from orquestra import sdk

        # Individually specified as arguments
        @sdk.task(
            dependency_imports=[
                sdk.PythonImports(
                    "typing_extensions==4.5.0", "pytest~=6.2.5", "gpustat>=1.0.0"
                )
            ]
        )
        def demo_task_1():
            ...

        # Read from file at submit time. File path is relative to current working
        # directory.
        @sdk.task(dependency_imports=[sdk.PythonImports(file="./requirements.txt")])
        def demo_task_2():
            ...

        # Both
        @sdk.task(
            dependency_imports=[
                sdk.PythonImports(
                    "typing_extensions==4.5.0",
                    "pytest~=6.2.5",
                    "gpustat>=1.0.0",
                    file="./requirements.txt",
                )
            ]
        )
        def demo_task_3():
            ...

        # </snippet>

        return demo_task_1, demo_task_2, demo_task_3

    @staticmethod
    def github_import_private_repo():
        from orquestra import sdk

        @sdk.task(
            source_import=sdk.GithubImport(
                "zapatacomputing/my_source_repo",
                git_ref="feat/my-feature-branch",
                username="my-github-username",
                personal_access_token=sdk.Secret(
                    "my_pat",
                    config_name="prod-d",
                    # Orquestra Workspace where the secret is defined. You can read it
                    # on Orquestra Portal website.
                    workspace_id="ws1",
                ),
            )
        )
        def demo_task():
            pass

        # </snippet>
        return demo_task

    @staticmethod
    def github_import_public_repo():
        from orquestra import sdk

        @sdk.task(source_import=sdk.GithubImport("zapatacomputing/my_source_repo"))
        def demo_task():
            pass

        # </snippet>
        return demo_task


def _import_models(
    dsl_task,
) -> t.Tuple[ir.Import, t.Optional[t.Sequence[ir.Import]]]:
    """
    Uses the ``dsl_task`` exactly once in a workflow, builds the workflow model, and
    extracts imports used by the corresponding task def model.
    """

    @sdk.workflow
    def wf():
        return dsl_task()

    wf_model = wf().model

    task_model = list(wf_model.tasks.values())[0]
    src_import = wf_model.imports[task_model.source_import_id]
    if (deps_ids := task_model.dependency_import_ids) is not None:
        deps_imports = [wf_model.imports[dep_id] for dep_id in deps_ids]
    else:
        deps_imports = None

    return src_import, deps_imports


@pytest.mark.slow
class TestSnippets:
    @staticmethod
    def test_good_practice_defaults():
        """
        Expecting inline source import and no dependency imports.
        """
        # Given
        task = Snippets.good_practice_defaults()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "INLINE_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_good_practice_python_imports():
        # Given
        task = Snippets.good_practice_python_imports()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "INLINE_IMPORT"
        assert deps_imports is not None
        assert len(deps_imports) == 1
        assert deps_imports[0].type == "PYTHON_IMPORT"

    @staticmethod
    def test_good_practice_git_import_with_auth():
        # Given
        task = Snippets.good_practice_git_import_with_auth()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_simple_task_explicit():
        """
        Expecting inline source import and no dependency imports.
        """
        # Given
        task = Snippets.simple_task_explicit()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "INLINE_IMPORT"
        assert deps_imports == []

    @staticmethod
    def test_python_imports(monkeypatch, tmp_path):
        # Given
        task1, task2, task3 = Snippets.python_imports()

        # `file=` is read at model build time so we need to fake it.
        (tmp_path / "requirements.txt").write_text("torch\n")
        monkeypatch.chdir(tmp_path)

        for task in [task1, task2, task3]:
            # When
            src_import, deps_imports = _import_models(task)

            # Then
            assert src_import.type == "INLINE_IMPORT"
            assert deps_imports is not None
            assert len(deps_imports) == 1
            assert deps_imports[0].type == "PYTHON_IMPORT"

    @staticmethod
    def test_github_import_private_repo():
        # Given
        task = Snippets.github_import_private_repo()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_github_import_public_repo():
        # Given
        task = Snippets.github_import_public_repo()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None
