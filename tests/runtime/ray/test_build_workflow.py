################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import re
from typing import Any, Dict, List, Optional, Union
from unittest.mock import ANY, Mock, call, create_autospec

import pytest

import orquestra.sdk as sdk
import orquestra.sdk._client.secrets
from orquestra.sdk._client._base import _git_url_utils
from orquestra.sdk._client._base._graphs import iter_invocations_topologically
from orquestra.sdk._client._base._testing._example_wfs import (
    workflow_parametrised_with_resources,
)
from orquestra.sdk.exceptions import OrquestraSDKVersionMismatchWarning
from orquestra.sdk.runtime._ray import _build_workflow, _client
from orquestra.sdk.shared import serde
from orquestra.sdk.shared.schema import ir
from orquestra.sdk.shared.schema.ir import GitURL, SecretNode
from orquestra.sdk.shared.schema.responses import WorkflowResult


@pytest.fixture
def git_url() -> GitURL:
    return GitURL(
        original_url="https://github.com/zapata-engineering/orquestra-sdk",
        protocol="https",
        user=None,
        password=None,
        host="github.com",
        port=None,
        path="zapata-engineering/orquestra-sdk",
        query=None,
    )


class TestBuildGitURL:
    @pytest.mark.parametrize(
        "protocol,expected_url",
        [
            (
                "git+ssh",
                "git+ssh://git@github.com/zapata-engineering/orquestra-sdk",
            ),
            (
                "ssh+git",
                "ssh+git://git@github.com/zapata-engineering/orquestra-sdk",
            ),
            ("ftp", "ftp://git@github.com/zapata-engineering/orquestra-sdk"),
            ("ftps", "ftps://git@github.com/zapata-engineering/orquestra-sdk"),
            ("http", "http://github.com/zapata-engineering/orquestra-sdk"),
            ("https", "https://github.com/zapata-engineering/orquestra-sdk"),
            (
                "git+http",
                "git+http://github.com/zapata-engineering/orquestra-sdk",
            ),
            (
                "git+https",
                "git+https://github.com/zapata-engineering/orquestra-sdk",
            ),
        ],
    )
    def test_different_protocols(
        self, git_url: GitURL, protocol: str, expected_url: str
    ):
        url = _build_workflow._build_git_url(git_url, protocol)
        assert url == expected_url

    def test_ssh_with_port(self, git_url: GitURL):
        git_url.port = 22
        url = _build_workflow._build_git_url(git_url, "ssh")
        assert url == "ssh://git@github.com:22/zapata-engineering/orquestra-sdk"

    @pytest.mark.parametrize(
        "protocol",
        ["http", "https", "git+http", "git+https"],
    )
    def test_http_with_user(self, git_url: GitURL, protocol: str):
        git_url.user = "amelio_robles_avila"
        url = _build_workflow._build_git_url(git_url, protocol)
        assert url == (
            f"{protocol}://amelio_robles_avila@github.com"
            "/zapata-engineering/orquestra-sdk"
        )

    def test_uses_default_protocol(self, git_url: GitURL):
        url = _build_workflow._build_git_url(git_url)
        assert url == "https://github.com/zapata-engineering/orquestra-sdk"

    def test_with_password(self, monkeypatch: pytest.MonkeyPatch, git_url: GitURL):
        secrets_get = create_autospec(orquestra.sdk.secrets.get)
        secrets_get.return_value = "<mocked secret>"
        monkeypatch.setattr(orquestra.sdk.secrets, "get", secrets_get)

        secret_name = "my_secret"
        secret_config = "secret config"
        secret_workspace = "secret workspace"
        git_url.password = SecretNode(
            id="mocked secret",
            secret_name=secret_name,
            secret_config=secret_config,
            workspace_id=secret_workspace,
        )

        url = _build_workflow._build_git_url(git_url)
        assert url == (
            "https://git:<mocked secret>@github.com/zapata-engineering/orquestra-sdk"
        )
        secrets_get.assert_called_once_with(
            secret_name, config_name=secret_config, workspace_id=secret_workspace
        )

    def test_unknown_protocol_in_original(self, git_url: GitURL):
        git_url.original_url = "custom_protocol://<blah>"
        git_url.protocol = "custom_protocol"
        url = _build_workflow._build_git_url(git_url)
        assert url == git_url.original_url

    def test_unknown_protocol_override(self, git_url: GitURL):
        with pytest.raises(ValueError) as exc_info:
            _ = _build_workflow._build_git_url(git_url, "custom_protocol")
        exc_info.match("Unknown protocol: `custom_protocol`")


def make_workflow_with_dependencies(deps, *, n_tasks=1):
    """Generate a workflow definition with the specified dependencies."""

    @sdk.task(dependency_imports=deps)
    def hello_orquestra() -> str:
        return "Hello Orquestra!"

    @sdk.workflow()
    def hello_orquestra_wf():
        return [hello_orquestra() for _ in range(n_tasks)]

    return hello_orquestra_wf()


class TestPipString:
    @pytest.fixture()
    def mock_serde(self, monkeypatch: pytest.MonkeyPatch):
        """We're not testing the serde package, so we're mocking it."""
        monkeypatch.setattr(
            serde,
            "stringify_package_spec",
            Mock(return_value="mocked"),
        )

    @pytest.mark.usefixtures("mock_serde")
    class TestPythonImports:
        def test_empty(self):
            imp = ir.PythonImports(id="mock-import", packages=[], pip_options=[])
            pip = _build_workflow._pip_string(imp)
            assert pip == []

        def test_with_package(self):
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

        def test_with_two_packages(self):
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
                id="mock-import",
                repo_url=_git_url_utils.parse_git_url("https://mock/mock/mock"),
                git_ref="mock",
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["git+https://mock/mock/mock@mock"]

        def test_pip_ssh_format(self, patch_env):
            imp = ir.GitImport(
                id="mock-import",
                repo_url=_git_url_utils.parse_git_url("ssh://git@mock/mock/mock"),
                git_ref="mock",
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["git+ssh://git@mock/mock/mock@mock"]

        def test_usual_ssh_format(self, patch_env):
            imp = ir.GitImport(
                id="mock-import",
                repo_url=_git_url_utils.parse_git_url("git@mock:mock/mock"),
                git_ref="mock",
            )
            pip = _build_workflow._pip_string(imp)
            assert pip == ["git+ssh://git@mock/mock/mock@mock"]

        def test_no_env_set(self):
            imp = ir.GitImport(
                id="mock-import",
                repo_url=_git_url_utils.parse_git_url("git@mock:mock/mock"),
                git_ref="mock",
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


class TestMakeDag:
    @pytest.fixture
    def client(self):
        return create_autospec(_client.RayClient)

    @pytest.fixture
    def wf_run_id(self):
        return "mocked_wf_run_id"

    def test_import_pip_string_resolved_once_per_import(
        self, monkeypatch: pytest.MonkeyPatch, client: Mock, wf_run_id: str
    ):
        pip_string = create_autospec(_build_workflow._pip_string)
        pip_string.return_value = ["mocked"]
        monkeypatch.setattr(_build_workflow, "_pip_string", pip_string)
        imps = [
            sdk.GithubImport(
                "zapata-engineering/orquestra-sdk",
                personal_access_token=sdk.Secret(
                    "mock-secret", config_name="mock-config", workspace_id="mock-ws"
                ),
            ),
            sdk.PythonImports("numpy", "polars"),
            sdk.InlineImport(),
        ]
        workflow_def = make_workflow_with_dependencies(imps, n_tasks=100).model
        _ = _build_workflow.make_ray_dag(client, workflow_def, wf_run_id, False)
        assert pip_string.mock_calls == [
            call(imp) for imp in workflow_def.imports.values()
        ]

    class TestResourcesInMakeDag:
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
            _ = _build_workflow.make_ray_dag(client, workflow, wf_run_id, False)

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
            "custom_image, gpu, expected_resources, expected_kwargs",
            (
                (
                    "a_custom_image:latest",
                    None,
                    {"image:a_custom_image:latest": 1},
                    {},
                ),
                (
                    None,
                    None,
                    {
                        "image:hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:mocked": 1  # noqa: E501
                    },
                    {},
                ),
                (
                    None,
                    1,
                    {
                        "image:hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:mocked-cuda": 1  # noqa: E501
                    },
                    {
                        "num_gpus": 1,
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
                gpu: Optional[int],
                expected_resources: Dict[str, int],
                expected_kwargs: Dict[str, Any],
            ):
                # Given
                monkeypatch.setenv("ORQ_RAY_SET_CUSTOM_IMAGE_RESOURCES", "1")
                workflow = workflow_parametrised_with_resources(
                    gpu=gpu, custom_image=custom_image
                ).model

                # To prevent hardcoding a version number, let's override the version for
                # this test.

                # We can be certain the workfloe def metadata is available
                assert workflow.metadata is not None
                workflow.metadata.sdk_version.original = "mocked"

                # When
                _ = _build_workflow.make_ray_dag(client, workflow, wf_run_id, False)

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
                    **expected_kwargs,
                )

            def test_with_env_not_set(
                self,
                client: Mock,
                wf_run_id: str,
                custom_image: Optional[str],
                gpu: Optional[int],
                expected_resources: Dict[str, int],
                expected_kwargs: Dict[str, Any],
            ):
                # Given
                workflow = workflow_parametrised_with_resources(
                    gpu=gpu, custom_image=custom_image
                ).model

                # When
                _ = _build_workflow.make_ray_dag(client, workflow, wf_run_id, False)

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
                    **expected_kwargs,
                )


class TestArgumentUnwrapper:
    @pytest.fixture
    def mock_secret_get(self, monkeypatch: pytest.MonkeyPatch):
        secrets_get = create_autospec(_build_workflow.secrets.get)
        monkeypatch.setattr(_build_workflow.secrets, "get", secrets_get)
        return secrets_get

    @pytest.fixture
    def mock_deserialize(self, monkeypatch: pytest.MonkeyPatch):
        deserialize = create_autospec(serde.deserialize)
        monkeypatch.setattr(serde, "deserialize", deserialize)
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
                fn,
                args_artifact_nodes,
                {},
                deserialize=True,
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


@pytest.mark.parametrize(
    "installed_sdk_version, expected_sdk_dependency",
    [
        ("0.57.1.dev3+g3ef9f57.d20231003", None),
        ("1.2.3", "1.2.3"),
        ("0.1.dev1+g25df81e", None),
    ],
)
class TestHandlingSDKVersions:
    """``_import_pip_env`` handles adding the current SDK version as a dependency.

    Note that these tests don't mock serde - we're interested in whether the correct
    package list gets constructed so we can't just return 'mocked' for imports.
    """

    @staticmethod
    def test_with_no_dependencies(
        installed_sdk_version: str,
        expected_sdk_dependency: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Given
        monkeypatch.setattr(
            _build_workflow,
            "get_installed_version",
            Mock(return_value=installed_sdk_version),
        )
        wf = make_workflow_with_dependencies([]).model
        task_inv = [inv for inv in iter_invocations_topologically(wf)][0]
        imports = {
            id: _build_workflow._pip_string(imp) for id, imp in wf.imports.items()
        }

        # When
        pip = _build_workflow._import_pip_env(task_inv, wf, imports)

        # Then
        if expected_sdk_dependency:
            assert pip == [f"orquestra-sdk=={expected_sdk_dependency}"]
        else:
            assert pip == []

    @staticmethod
    @pytest.mark.parametrize(
        "python_imports",
        [
            ["MarkupSafe==1.0.0"],
            ["MarkupSafe==1.0.0", "Jinja2==2.7.2"],
        ],
    )
    def test_with_multiple_dependencies(
        python_imports: List[str],
        installed_sdk_version: str,
        expected_sdk_dependency: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Given
        monkeypatch.setattr(
            _build_workflow,
            "get_installed_version",
            Mock(return_value=installed_sdk_version),
        )
        wf = make_workflow_with_dependencies([sdk.PythonImports(*python_imports)]).model
        task_inv = [inv for inv in iter_invocations_topologically(wf)][0]
        imports = {
            id: _build_workflow._pip_string(imp) for id, imp in wf.imports.items()
        }

        # When
        pip = _build_workflow._import_pip_env(task_inv, wf, imports)

        # Then
        if expected_sdk_dependency:
            assert sorted(pip) == sorted(
                python_imports + [f"orquestra-sdk=={expected_sdk_dependency}"]
            )
        else:
            assert sorted(pip) == sorted(python_imports)

    @pytest.mark.parametrize(
        "sdk_import",
        [
            "orquestra-sdk",
            "orquestra-sdk>=1.2.3",
            "orquestra-sdk<=1.2.3",
            "orquestra-sdk~=1.2.3",
            "orquestra-sdk==1.2.3",
            "orquestra-sdk!=1.2.3",
            "orquestra-sdk===1.2.3",
            "orquestra-sdk >= 1.2.3",
            "orquestra-sdk <= 1.2.3",
            "orquestra-sdk ~= 1.2.3",
            "orquestra-sdk == 1.2.3",
            "orquestra-sdk != 1.2.3",
            "orquestra-sdk === 1.2.3",
        ],
    )
    @pytest.mark.parametrize(
        "python_imports",
        [
            [],
            ["MarkupSafe==1.0.0"],
            ["MarkupSafe==1.0.0", "Jinja2==2.7.2"],
        ],
    )
    class TestHandlesSDKDependency:
        @staticmethod
        @pytest.mark.filterwarnings("ignore:The definition for task ")
        def test_replaces_declared_sdk_dependency(
            sdk_import,
            python_imports,
            installed_sdk_version: str,
            expected_sdk_dependency: str,
            monkeypatch: pytest.MonkeyPatch,
        ):
            # Given
            monkeypatch.setattr(
                _build_workflow,
                "get_installed_version",
                Mock(return_value=installed_sdk_version),
            )
            wf = make_workflow_with_dependencies(
                [sdk.PythonImports(*python_imports + [sdk_import])]
            ).model
            task_inv = [inv for inv in iter_invocations_topologically(wf)][0]
            imports = {
                id: _build_workflow._pip_string(imp) for id, imp in wf.imports.items()
            }

            # When
            pip = _build_workflow._import_pip_env(task_inv, wf, imports)

            # Then
            if expected_sdk_dependency:
                assert sorted(pip) == sorted(
                    python_imports + [f"orquestra-sdk=={expected_sdk_dependency}"]
                )
            else:
                assert sorted(pip) == sorted(python_imports)

        @staticmethod
        @pytest.mark.filterwarnings("error")
        def test_warns_user_that_declared_sdk_dependency_is_ignored(
            sdk_import,
            python_imports,
            installed_sdk_version: str,
            expected_sdk_dependency: str,
            monkeypatch: pytest.MonkeyPatch,
        ):
            # Given
            monkeypatch.setattr(
                _build_workflow,
                "get_installed_version",
                Mock(return_value=installed_sdk_version),
            )
            wf = make_workflow_with_dependencies(
                [sdk.PythonImports(*python_imports + [sdk_import])]
            ).model
            task_inv = [inv for inv in iter_invocations_topologically(wf)][0]
            imports = {
                id: _build_workflow._pip_string(imp) for id, imp in wf.imports.items()
            }

            # When
            with pytest.raises(OrquestraSDKVersionMismatchWarning) as e:
                _ = _build_workflow._import_pip_env(task_inv, wf, imports)

            # Then
            warning: str = e.exconly().strip()
            assert re.match(
                r"^orquestra\.sdk\.exceptions\.OrquestraSDKVersionMismatchWarning: The definition for task `task-hello-orquestra-.*` declares `orquestra-sdk(?P<dependency>.*)` as a dependency. The current SDK version (\((?P<installed>.*)\) )?is automatically installed in task environments. The specified dependency will be ignored.$",  # noqa: E501
                warning,
            ), warning
