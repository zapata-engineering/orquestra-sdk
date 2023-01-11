################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk._base._conversions import _ids
from orquestra.sdk.schema import ir


class TestGenerateYamlIDs:
    @staticmethod
    @pytest.mark.parametrize(
        "obj,expected_id",
        [
            (
                ir.GitImport(
                    id="git-1871174d6e_github_com_zapatacomputing_orquestra_sdk",
                    repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",  # noqa: E501
                    git_ref="main",
                ),
                "orq-wor-sdk",
            ),
            (
                ir.InlineImport(
                    id="inline-import-0",
                ),
                "inline-import-0",
            ),
            (
                ir.PythonImports(
                    id="python-import-0",
                    packages=[
                        ir.PackageSpec(
                            name="numpy==1.21.5",
                            extras=[],
                            version_constraints=[],
                            environment_markers="",
                        )
                    ],
                    pip_options=[],
                ),
                "py",
            ),
        ],
    )
    def test_supported_types(obj, expected_id):
        id_obj_map = _ids.generate_yaml_ids([obj])

        ((result_id, result_obj),) = id_obj_map.items()

        assert result_id == expected_id
        assert result_obj == obj

    @staticmethod
    def test_collisions():
        # Given
        imports = [
            ir.GitImport(
                id="git-1871174d6e_github_com_zapatacomputing_orquestra_workflow_sdk",
                repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
                git_ref="main",
            ),
            ir.GitImport(
                id="git-123456_github_com_zapatacomputing_orquestra_workflow_sdk",
                repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
                git_ref="dev",
            ),
        ]

        # When
        id_obj_map = _ids.generate_yaml_ids(imports)

        # Then
        assert id_obj_map == {
            "orq-wor-sdk-0": ir.GitImport(
                id="git-1871174d6e_github_com_zapatacomputing_orquestra_workflow_sdk",
                repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
                git_ref="main",
            ),
            "orq-wor-sdk-1": ir.GitImport(
                id="git-123456_github_com_zapatacomputing_orquestra_workflow_sdk",
                repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
                git_ref="dev",
            ),
        }
