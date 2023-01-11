################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock

import pytest

import orquestra.sdk as sdk
import orquestra.sdk.packaging as packaging


class TestGetInstalledVersion:
    @staticmethod
    def test_successful_call(monkeypatch):
        # Given
        metadata_version = Mock(return_value="1.2.3")
        monkeypatch.setattr(packaging.metadata, "version", metadata_version)
        # When
        version = packaging.get_installed_version("some-package")
        # Then
        assert version == "1.2.3"

    @staticmethod
    def test_package_not_found(monkeypatch):
        # Given
        metadata_version = Mock(side_effect=packaging.metadata.PackageNotFoundError)
        monkeypatch.setattr(packaging.metadata, "version", metadata_version)
        # When
        with pytest.raises(packaging.PackagingError) as exc_info:
            _ = packaging.get_installed_version("some-package")
        # Then
        assert exc_info.match("Package not found: some-package")


class TestInstalledImport:
    @staticmethod
    def test_package_found(monkeypatch):
        # Given
        get_version = Mock(return_value="1.2.3")
        monkeypatch.setattr(packaging, "get_installed_version", get_version)
        # When
        imp = packaging.InstalledImport(package_name="some-package")
        # Then
        assert isinstance(imp, sdk.PythonImports)
        assert len(imp.packages) == 1
        assert imp.packages[0] == "some-package==1.2.3"

    @staticmethod
    def test_package_not_found(monkeypatch):
        # Given
        get_version = Mock(side_effect=packaging.PackagingError("Package not found:"))
        monkeypatch.setattr(packaging, "get_installed_version", get_version)
        # When
        with pytest.raises(packaging.PackagingError) as exc_info:
            _ = packaging.InstalledImport(package_name="some-package")
        # Then
        assert exc_info.match("Package not found:")

    @staticmethod
    def test_package_not_found_fallback(monkeypatch):
        # Given
        get_version = Mock(side_effect=packaging.PackagingError("Package not found:"))
        monkeypatch.setattr(packaging, "get_installed_version", get_version)
        fallback = sdk.GithubImport("zapatacomputing/orquestra-workflow-sdk")
        # When
        imp = packaging.InstalledImport(package_name="some-package", fallback=fallback)
        # Then
        assert imp == fallback

    @staticmethod
    def test_package_version_matches(monkeypatch):
        # Given
        get_version = Mock(return_value="1.2.3")
        monkeypatch.setattr(packaging, "get_installed_version", get_version)
        # When
        imp = packaging.InstalledImport(
            package_name="some-package", version_match="[0-9].[0-9].[0-9]"
        )
        # Then
        assert isinstance(imp, sdk.PythonImports)
        assert len(imp.packages) == 1
        assert imp.packages[0] == "some-package==1.2.3"

    @staticmethod
    def test_package_version_does_not_match(monkeypatch):
        # Given
        get_version = Mock(return_value="1.2.3")
        monkeypatch.setattr(packaging, "get_installed_version", get_version)
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
        get_version = Mock(return_value="1.2.3")
        monkeypatch.setattr(packaging, "get_installed_version", get_version)
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
