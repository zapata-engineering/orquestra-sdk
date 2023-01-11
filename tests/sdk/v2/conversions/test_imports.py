################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import importlib.metadata as metadata
from unittest.mock import Mock

import pytest

from orquestra.sdk._base._conversions import _imports


@pytest.mark.parametrize(
    "version_string",
    [
        ("0.0.4"),
        ("1.2.3"),
        ("10.20.30"),
        ("1.0.0"),
        ("2.0.0"),
        ("1.1.7"),
        ("999999999999999999999999.999999999999999999.99999999999999999"),
    ],
)
def test_allowed_package_versions(monkeypatch, version_string: str):
    version_mock = Mock(return_value=version_string)
    monkeypatch.setattr(metadata, "version", version_mock)
    version = _imports._get_package_version_tag("doesnt-matter")
    assert version == f"v{version_string}"


@pytest.mark.parametrize(
    "version_string, exception_message",
    [
        # Invalid SemVer
        ("1", "Version string doesn't match expected SemVer."),
        ("1.2", "Version string doesn't match expected SemVer."),
        ("1.2.3-0123", "Version string doesn't match expected SemVer."),
        ("+invalid", "Version string doesn't match expected SemVer."),
        ("-invalid", "Version string doesn't match expected SemVer."),
        ("-invalid+invalid", "Version string doesn't match expected SemVer."),
        ("1.0.0-alpha_beta", "Version string doesn't match expected SemVer."),
        # Revisions since release
        (
            "1.0.2.main+gabcd123",
            "There have been revisions since the last tagged version.",
        ),
        (
            "1.0.2-main+gabcd123",
            "There have been revisions since the last tagged version.",
        ),
        # Dirty working dir
        ("1.0.1+d19891213", "The workdir is not clean."),
        # Revisions since release and dirty working directory
        (
            "1.0.2.main+gabcd123.d19891213",
            "There have been revisions since the last tagged version.",
        ),
        (
            "1.0.2-main+gabcd123.d19891213",
            "There have been revisions since the last tagged version.",
        ),
    ],
)
def test_disallowed_package_versions(
    monkeypatch, version_string: str, exception_message: str
):
    version_mock = Mock(return_value=version_string)
    monkeypatch.setattr(metadata, "version", version_mock)
    with pytest.raises(_imports.PackageVersionError) as e:
        _ = _imports._get_package_version_tag("doesnt-matter")
    assert exception_message in str(e.value)
