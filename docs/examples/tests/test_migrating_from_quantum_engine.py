################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Migrating From Quantum Engine" guide.
"""
import typing as t

import pytest

from orquestra import sdk
from orquestra.sdk.schema import ir


class Snippets:
    @staticmethod
    def defaults():
        @sdk.task
        def simple_task():
            ...

        # </snippet>
        return simple_task

    @staticmethod
    def inline_import():
        @sdk.task(source_import=sdk.InlineImport())
        def simple_task():
            ...

        # </snippet>
        return simple_task

    @staticmethod
    def python_imports():
        @sdk.task(dependency_imports=[sdk.PythonImports("torch~=2.0")])
        def my_task():
            ...

        # </snippet>
        return my_task

    @staticmethod
    def github_import_public():
        from orquestra import sdk

        @sdk.task(
            source_import=sdk.GithubImport(
                "zapatacomputing/my_source_repo",
                git_ref="feat/my-feature-branch",
            )
        )
        def my_task():
            ...

        # </snippet>
        return my_task

    @staticmethod
    def github_import_private_before():
        from orquestra import sdk

        @sdk.task(
            source_import=sdk.GithubImport(
                "zapatacomputing/my_source_repo",
                git_ref="feat/my-feature-branch",
            )
        )
        def demo_task():
            pass

        # </snippet>
        return demo_task

    @staticmethod
    def github_import_private_after():
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
    def git_import_with_auth():
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


def _import_models(
    dsl_task,
) -> t.Tuple[ir.Import, t.Optional[t.Sequence[ir.Import]]]:
    """
    Uses the ``dsl_task`` exactly once in a workflow, builds the workflow model, and
    extracts imports used by the corresponding task def model.
    """
    # TODO: reuse across test files

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
    @pytest.mark.parametrize("task", [Snippets.defaults(), Snippets.inline_import()])
    def test_defaults(task):
        """
        Expecting inline source import and no dependency imports.
        """
        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "INLINE_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_python_imports():
        # Given
        task = Snippets.python_imports()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "INLINE_IMPORT"
        assert deps_imports is not None
        assert len(deps_imports) == 1
        assert deps_imports[0].type == "PYTHON_IMPORT"

    @staticmethod
    def test_github_import_private_before():
        # Given
        task = Snippets.github_import_private_before()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_github_import_private_after():
        # Given
        task = Snippets.github_import_private_after()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_github_import_public():
        # Given
        task = Snippets.github_import_public()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None

    @staticmethod
    def test_good_practice_git_import_with_auth():
        # Given
        task = Snippets.git_import_with_auth()

        # When
        src_import, deps_imports = _import_models(task)

        # Then
        assert src_import.type == "GIT_IMPORT"
        assert deps_imports is None
