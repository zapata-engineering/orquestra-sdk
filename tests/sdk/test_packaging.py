################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock

import pytest

from orquestra import sdk
from orquestra.sdk.shared import packaging
from orquestra.sdk.shared.packaging import _versions

try:
    import importlib.metadata as metadata  # type: ignore
except ModuleNotFoundError:
    import importlib_metadata as metadata  # type: ignore


class TestGetInstalledVersion:
    @staticmethod
    def test_successful_call(monkeypatch):
        # Given
        monkeypatch.setattr(metadata, "version", Mock(return_value="1.2.3"))
        # When
        version = packaging.get_installed_version("some-package")
        # Then
        assert version == "1.2.3"

    @staticmethod
    def test_package_not_found(monkeypatch):
        # Given
        monkeypatch.setattr(
            metadata, "version", Mock(side_effect=metadata.PackageNotFoundError)
        )
        # When
        with pytest.raises(packaging.PackagingError) as exc_info:
            _ = packaging.get_installed_version("some-package")
        # Then
        assert exc_info.match("Package not found: some-package")


class TestInstalledImport:
    @staticmethod
    def test_package_found(monkeypatch):
        # Given
        monkeypatch.setattr(
            _versions, "get_installed_version", Mock(return_value="1.2.3")
        )
        # When
        imp = packaging.InstalledImport(package_name="some-package")
        # Then
        assert isinstance(imp, sdk.PythonImports)
        assert len(imp._packages) == 1
        assert imp._packages[0] == "some-package==1.2.3"

    @staticmethod
    def test_package_not_found(monkeypatch):
        # Given
        monkeypatch.setattr(
            _versions,
            "get_installed_version",
            Mock(side_effect=packaging.PackagingError("Package not found:")),
        )
        # When
        with pytest.raises(packaging.PackagingError) as exc_info:
            _ = packaging.InstalledImport(package_name="some-package")
        # Then
        assert exc_info.match("Package not found:")

    @staticmethod
    def test_package_not_found_fallback(monkeypatch):
        # Given
        monkeypatch.setattr(
            _versions,
            "get_installed_version",
            Mock(side_effect=packaging.PackagingError("Package not found:")),
        )
        fallback = sdk.GithubImport("zapatacomputing/orquestra-workflow-sdk")
        # When
        imp = packaging.InstalledImport(package_name="some-package", fallback=fallback)
        # Then
        assert imp == fallback

    @staticmethod
    def test_package_version_matches(monkeypatch):
        # Given
        monkeypatch.setattr(
            _versions, "get_installed_version", Mock(return_value="1.2.3")
        )
        # When
        imp = packaging.InstalledImport(
            package_name="some-package", version_match="[0-9].[0-9].[0-9]"
        )
        # Then
        assert isinstance(imp, sdk.PythonImports)
        assert len(imp._packages) == 1
        assert imp._packages[0] == "some-package==1.2.3"

    @staticmethod
    def test_package_version_does_not_match(monkeypatch):
        # Given
        monkeypatch.setattr(
            _versions, "get_installed_version", Mock(return_value="1.2.3")
        )
        # When
        with pytest.raises(packaging.PackagingError) as exc_info:
            _ = packaging.InstalledImport(
                package_name="some-package", version_match="xxx"
            )
        # Then
        assert exc_info.match(
            "Package version mismatch: some-package==1.2.3\n"
            'Expected version to match "xxx"'
        )

    @staticmethod
    def test_package_version_does_not_match_fallback(monkeypatch):
        # Given
        monkeypatch.setattr(
            _versions, "get_installed_version", Mock(return_value="1.2.3")
        )
        fallback = sdk.GithubImport("zapatacomputing/orquestra-workflow-sdk")
        # When
        imp = packaging.InstalledImport(
            package_name="some-package",
            version_match="xxx",
            fallback=fallback,
        )
        # Then
        assert imp == fallback


class TestExecuteTask:
    @staticmethod
    def test_pass_task_def():
        @sdk.task
        def hello():
            return 100

        result = packaging.execute_task(hello, (), {})

        assert result == 100

    @staticmethod
    def test_pass_task_def_args():
        @sdk.task
        def hello(a, b):
            return a * b

        result = packaging.execute_task(
            hello,
            (1, 2),
            {},
        )

        assert result == 2

    @staticmethod
    def test_pass_task_def_kwargs():
        @sdk.task
        def hello(a, b):
            return a * b

        result = packaging.execute_task(
            hello,
            (),
            {"a": 2, "b": 2},
        )

        assert result == 4
