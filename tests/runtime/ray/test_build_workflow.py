################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from typing import Dict, Optional, Union
from unittest.mock import ANY, Mock, call, create_autospec

import pytest

from orquestra.sdk._base._testing._example_wfs import (
    workflow_parametrised_with_resources,
)
from orquestra.sdk._ray import _build_workflow
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.responses import WorkflowResult


class TestPipString:
    class TestPythonImports:
        def test_empty(self):
            imp = ir.PythonImports(id="mock-import", packages=[], pip_options=[])
            pip = _build_workflow._pip_string(imp)
            assert pip == []

        def test_with_package(self, monkeypatch: pytest.MonkeyPatch):
            # We're not testing the serde package, so we're mocking it
            monkeypatch.setattr(
                _build_workflow.serde,
                "stringify_package_spec",
                Mock(return_value="mocked"),
            )
            imp = ir.PythonImports(
                id="mock-import",
                packages=[
                    ir.PackageSpec(
                        name="one",
                        extras=[],
                        version_constraints=[],
                        environment_markers="",
                    )
                ],
                pip_options=[],
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["mocked"]

        def test_with_two_packages(self, monkeypatch: pytest.MonkeyPatch):
            # We're not testing the serde package, so we're mocking it
            monkeypatch.setattr(
                _build_workflow.serde,
                "stringify_package_spec",
                Mock(return_value="mocked"),
            )
            imp = ir.PythonImports(
                id="mock-import",
                packages=[
                    ir.PackageSpec(
                        name="one",
                        extras=[],
                        version_constraints=[],
                        environment_markers="",
                    ),
                    ir.PackageSpec(
                        name="one",
                        extras=["extra"],
                        version_constraints=["version"],
                        environment_markers="env marker",
                    ),
                ],
                pip_options=[],
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["mocked", "mocked"]

    class TestGitImports:
        @pytest.fixture
        def patch_env(self, monkeypatch: pytest.MonkeyPatch):
            monkeypatch.setenv("ORQ_RAY_DOWNLOAD_GIT_IMPORTS", "1")

        def test_http(self, patch_env):
            imp = ir.GitImport(
                id="mock-import", repo_url="https://mock/mock/mock", git_ref="mock"
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["git+https://mock/mock/mock@mock"]

        def test_pip_ssh_format(self, patch_env):
            imp = ir.GitImport(
                id="mock-import", repo_url="ssh://git@mock/mock/mock", git_ref="mock"
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["git+ssh://git@mock/mock/mock@mock"]

        def test_usual_ssh_format(self, patch_env):
            imp = ir.GitImport(
                id="mock-import", repo_url="git@mock:mock/mock", git_ref="mock"
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["git+ssh://git@mock/mock/mock@mock"]

        def test_no_env_set(self):
            imp = ir.GitImport(
                id="mock-import", repo_url="git@mock:mock/mock", git_ref="mock"
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == []

    class TestOtherImports:
        def test_local_import(self):
            imp = ir.LocalImport(id="mock-import")
            pip = _build_workflow._pip_string(imp)
            assert pip == []

        def test_inline_import(self):
            imp = ir.InlineImport(id="mock-import")
            pip = _build_workflow._pip_string(imp)
            assert pip == []


class TestResourcesInMakeDag:
    @pytest.fixture
    def wf_run_id(self):
        return "mocked_wf_run_id"

    @pytest.fixture
    def client(self):
        return create_autospec(_build_workflow.RayClient)

    @pytest.mark.parametrize(
        "resources, expected, types",
        [
            ({}, {}, {}),
            ({"cpu": "1000m"}, {"num_cpus": 1.0}, {"num_cpus": int}),
            ({"memory": "1Gi"}, {"memory": 1073741824}, {"memory": int}),
            ({"gpu": "1"}, {"num_gpus": 1}, {"num_gpus": int}),
            (
                {"cpu": "2500m", "memory": "10G", "gpu": "1"},
                {"num_cpus": 2.5, "memory": 10000000000, "num_gpus": 1},
                {"num_cpus": float, "memory": int, "num_gpus": int},
            ),
        ],
    )
    def test_setting_resources(
        self,
        client: Mock,
        wf_run_id: str,
        resources: Dict[str, str],
        expected: Dict[str, Union[int, float]],
        types: Dict[str, type],
    ):
        # Given
        workflow = workflow_parametrised_with_resources(**resources).model

        # When
        _ = _build_workflow.make_ray_dag(client, workflow, wf_run_id, None)

        # Then
        calls = client.add_options.call_args_list

        # We should only have two calls: our invocation and the aggregation step
        assert len(calls) == 2
        # Checking our call did not have any resources included
        assert calls[0] == call(
            ANY,
            name=ANY,
            metadata=ANY,
            runtime_env=ANY,
            catch_exceptions=ANY,
            max_retries=ANY,
            **expected,
        )
        for kwarg_name, type_ in types.items():
            assert isinstance(calls[0].kwargs[kwarg_name], type_)

    @pytest.mark.parametrize(
        "custom_image, expected_resources",
        (
            ("a_custom_image:latest", {"image:a_custom_image:latest": 1}),
            (
                None,
                {
                    "image:hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:mocked": 1  # noqa: E501
                },
            ),
        ),
    )
    class TestSettingCustomImage:
        def test_with_env_set(
            self,
            client: Mock,
            wf_run_id: str,
            monkeypatch: pytest.MonkeyPatch,
            custom_image: Optional[str],
            expected_resources: Dict[str, int],
        ):
            # Given
            monkeypatch.setenv("ORQ_RAY_SET_CUSTOM_IMAGE_RESOURCES", "1")
            workflow = workflow_parametrised_with_resources(
                custom_image=custom_image
            ).model

            # To prevent hardcoding a version number, let's override the version for
            # this test.

            # We can be certain the workfloe def metadata is available
            assert workflow.metadata is not None
            workflow.metadata.sdk_version.original = "mocked"

            # When
            _ = _build_workflow.make_ray_dag(client, workflow, wf_run_id, None)

            # Then
            calls = client.add_options.call_args_list

            # We should only have two calls: our invocation and the aggregation step
            assert len(calls) == 2
            # Checking our call did not have any resources included

            assert calls[0] == call(
                ANY,
                name=ANY,
                metadata=ANY,
                runtime_env=ANY,
                catch_exceptions=ANY,
                max_retries=ANY,
                resources=expected_resources,
            )

        def test_with_env_not_set(
            self,
            client: Mock,
            wf_run_id: str,
            custom_image: Optional[str],
            expected_resources: Dict[str, int],
        ):
            # Given
            workflow = workflow_parametrised_with_resources(
                custom_image=custom_image
            ).model

            # When
            _ = _build_workflow.make_ray_dag(client, workflow, wf_run_id, None)

            # Then
            calls = client.add_options.call_args_list

            # We should only have two calls: our invocation and the aggregation step
            assert len(calls) == 2
            # Checking our call did not have any resources included
            assert calls[0] == call(
                ANY,
                name=ANY,
                metadata=ANY,
                runtime_env=ANY,
                catch_exceptions=ANY,
                max_retries=ANY,
            )


class TestArgumentUnwrapper:
    @pytest.fixture
    def mock_secret_get(self, monkeypatch: pytest.MonkeyPatch):
        secrets_get = create_autospec(_build_workflow.secrets.get)
        monkeypatch.setattr(_build_workflow.secrets, "get", secrets_get)
        return secrets_get

    @pytest.fixture
    def mock_deserialize(self, monkeypatch: pytest.MonkeyPatch):
        deserialize = create_autospec(_build_workflow.serde.deserialize)
        monkeypatch.setattr(_build_workflow.serde, "deserialize", deserialize)
        return deserialize

    class TestConstantNode:
        def test_deserialize(self, mock_deserialize):
            # Given
            fn = Mock()
            expected_arg = "Mocked"
            mock_deserialize.return_value = expected_arg
            constant_node = ir.ConstantNodeJSON(
                id="mocked", value="mocked", value_preview="mocked"
            )
            arg_unwrapper = _build_workflow.ArgumentUnwrapper(
                user_fn=fn,
                args_artifact_nodes={},
                kwargs_artifact_nodes={},
                deserialize=True,
            )

            # When
            _ = arg_unwrapper(constant_node)

            # Then
            mock_deserialize.assert_called_with(constant_node)
            fn.assert_called_with(expected_arg)

        def test_no_deserialize(self, mock_deserialize):
            # Given
            fn = Mock()
            constant_node = ir.ConstantNodeJSON(
                id="mocked", value="mocked", value_preview="mocked"
            )
            arg_unwrapper = _build_workflow.ArgumentUnwrapper(
                user_fn=fn,
                args_artifact_nodes={},
                kwargs_artifact_nodes={},
                deserialize=False,
            )

            # When
            _ = arg_unwrapper(constant_node)

            # Then
            mock_deserialize.assert_not_called()
            fn.assert_called_with(constant_node)

    class TestSecretNode:
        def test_deserialize(self, mock_secret_get):
            # Given
            fn = Mock()
            expected_arg = "Mocked"
            mock_secret_get.return_value = expected_arg
            secret_node = ir.SecretNode(id="mocked", secret_name="mocked")
            arg_unwrapper = _build_workflow.ArgumentUnwrapper(
                user_fn=fn,
                args_artifact_nodes={},
                kwargs_artifact_nodes={},
                deserialize=True,
            )

            # When
            _ = arg_unwrapper(secret_node)

            # Then
            mock_secret_get.assert_called_with(
                secret_node.secret_name, config_name=None, workspace_id=None
            )
            fn.assert_called_with(expected_arg)

        def test_no_deserialize(self, mock_secret_get):
            # Given
            fn = Mock()
            secret_node = ir.SecretNode(id="mocked", secret_name="mocked")
            arg_unwrapper = _build_workflow.ArgumentUnwrapper(
                user_fn=fn,
                args_artifact_nodes={},
                kwargs_artifact_nodes={},
                deserialize=False,
            )

            # When
            _ = arg_unwrapper(secret_node)

            # Then
            mock_secret_get.assert_not_called()
            fn.assert_called_with(secret_node)

    class TestUnrwapArtifact:
        @pytest.fixture
        def args_artifact_nodes(self):
            return {
                0: ir.ArtifactNode(id="mocked"),
                1: ir.ArtifactNode(id="mocked", artifact_index=0),
                2: ir.ArtifactNode(id="mocked", artifact_index=1),
            }

        @pytest.fixture
        def kwargs_artifact_nodes(self):
            return {
                "a": ir.ArtifactNode(id="mocked"),
                "b": ir.ArtifactNode(id="mocked", artifact_index=0),
                "c": ir.ArtifactNode(id="mocked", artifact_index=1),
            }

        @pytest.fixture
        def packed(self):
            return create_autospec(WorkflowResult)

        @pytest.fixture
        def unpacked(self):
            return tuple(create_autospec(WorkflowResult) for _ in range(2))

        @pytest.fixture
        def task_result(self, packed, unpacked):
            return _build_workflow.TaskResult(
                packed=packed,
                unpacked=unpacked,
            )

        def test_positional(
            self, mock_deserialize, args_artifact_nodes, task_result, packed, unpacked
        ):
            fn = Mock()
            arg_unwrapper = _build_workflow.ArgumentUnwrapper(
                fn, args_artifact_nodes, {}, True
            )
            _ = arg_unwrapper(task_result, task_result, task_result)

            calls = [call(packed), call(unpacked[0]), call(unpacked[1])]
            mock_deserialize.assert_has_calls(calls)

        def test_kwargs(
            self, mock_deserialize, kwargs_artifact_nodes, task_result, packed, unpacked
        ):
            fn = Mock()
            arg_unwrapper = _build_workflow.ArgumentUnwrapper(
                fn, {}, kwargs_artifact_nodes, True
            )
            _ = arg_unwrapper(a=task_result, b=task_result, c=task_result)

            calls = [call(packed), call(unpacked[0]), call(unpacked[1])]
            mock_deserialize.assert_has_calls(calls)
