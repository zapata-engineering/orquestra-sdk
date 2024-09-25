import json
from pathlib import Path
from unittest.mock import Mock, call, create_autospec

import orquestra.workflow_shared.secrets
import pydantic
import pytest
from orquestra.workflow_shared import parse_git_url, serde
from orquestra.workflow_shared.schema import ir
from orquestra.workflow_shared.schema.ir import GitURL, SecretNode, WorkflowDef
from orquestra.workflow_shared.schema.responses import WorkflowResult

from orquestra.workflow_runtime._ray import _build_workflow, _client


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
        secrets_get = create_autospec(orquestra.workflow_shared.secrets.get)
        secrets_get.return_value = "<mocked secret>"
        monkeypatch.setattr(orquestra.workflow_shared.secrets, "get", secrets_get)

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

        @pytest.mark.parametrize(
            "imp, expected",
            [
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("https://mock/mock/mock"),
                        git_ref="mock",
                    ),
                    ["git+https://mock/mock/mock@mock"],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("ssh://git@mock/mock/mock"),
                        git_ref="mock",
                    ),
                    ["git+ssh://git@mock/mock/mock@mock"],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("git@mock:mock/mock"),
                        git_ref="mock",
                    ),
                    ["git+ssh://git@mock/mock/mock@mock"],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("git@mock:mock/mock"),
                        git_ref="mock",
                        package_name="pack_mock",
                    ),
                    ["pack_mock @ git+ssh://git@mock/mock/mock@mock"],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("git@mock:mock/mock"),
                        git_ref="mock",
                        package_name="pack_mock",
                        extras=None,
                    ),
                    ["pack_mock @ git+ssh://git@mock/mock/mock@mock"],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("git@mock:mock/mock"),
                        git_ref="mock",
                        package_name="pack_mock",
                        extras=("extra_mock",),
                    ),
                    ["pack_mock[extra_mock] @ git+ssh://git@mock/mock/mock@mock"],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("git@mock:mock/mock"),
                        git_ref="mock",
                        package_name="pack_mock",
                        extras=("extra_mock", "e_mock"),
                    ),
                    [
                        "pack_mock[extra_mock,e_mock] @ "
                        "git+ssh://git@mock/mock/mock@mock"
                    ],
                ),
                (
                    ir.GitImport(
                        id="mock-import",
                        repo_url=parse_git_url("git@mock:mock/mock"),
                        git_ref="mock",
                        package_name=None,
                        extras=("extra_mock", "e_mock"),
                    ),
                    ["git+ssh://git@mock/mock/mock@mock"],
                ),
            ],
        )
        def test_build_pip_string(self, patch_env, imp, expected):
            assert _build_workflow._pip_string(imp) == expected

        def test_no_env_set(self):
            imp = ir.GitImport(
                id="mock-import",
                repo_url=parse_git_url("git@mock:mock/mock"),
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


class TestArgumentUnwrapper:
    @pytest.fixture
    def mock_secret_get(self, monkeypatch: pytest.MonkeyPatch):
        secrets_get = create_autospec(orquestra.workflow_shared.secrets.get)
        monkeypatch.setattr(orquestra.workflow_shared.secrets, "get", secrets_get)
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
        import orquestra.workflow_shared.packaging._versions

        import orquestra.workflow_runtime._ray._build_workflow

        monkeypatch.setattr(
            orquestra.workflow_shared.packaging._versions,
            "get_installed_version",
            lambda _: "0.64.0",
        )
        monkeypatch.setattr(
            orquestra.workflow_runtime._ray._build_workflow,
            "get_installed_version",
            lambda _: "0.64.0",
        )
        pip_string = create_autospec(_build_workflow._pip_string)
        pip_string.return_value = ["mocked"]
        monkeypatch.setattr(_build_workflow, "_pip_string", pip_string)
        path_to_json = Path(__file__).parent.joinpath("data/100_tasks_with_imps.json")
        with open(path_to_json, "r", encoding="utf-8") as f:
            wf_def = pydantic.TypeAdapter(WorkflowDef).validate_json(json.load(f))
        _ = _build_workflow.make_ray_dag(client, wf_def, wf_run_id, False)
        assert pip_string.mock_calls == [call(imp) for imp in wf_def.imports.values()]
