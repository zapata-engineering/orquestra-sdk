################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from unittest.mock import Mock

import pytest

from orquestra.workflow_shared import packaging

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
