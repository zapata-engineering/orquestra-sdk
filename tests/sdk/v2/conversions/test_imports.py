################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import importlib.metadata as metadata
from unittest.mock import Mock

import pytest

from orquestra.sdk._base._conversions import _imports
from orquestra.sdk.schema.ir import GitImport, GitURL, SecretNode


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


def test_skip_password_dereference_yaml(caplog: pytest.LogCaptureFixture):
    caplog.set_level("DEBUG")
    ir_imports = {
        "sdk": GitImport(
            id="sdk",
            repo_url=GitURL(
                original_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",  # noqa: E501
                protocol="ssh",
                user="git",
                password=None,
                host="github.com",
                port=None,
                path="zapatacomputing/orquestra-workflow-sdk.git",
                query=None,
            ),
            git_ref="main",
        ),
        "test": GitImport(
            id="test",
            repo_url=GitURL(
                original_url="https://example.com/some/path.git",
                protocol="https",
                user="example",
                password=SecretNode(
                    id="secret", secret_name="my_secret", secret_config="config_name"
                ),
                host="example.com",
                port=None,
                path="some/path.git",
                query=None,
            ),
            git_ref="main",
        ),
    }
    _ = _imports.ImportTranslator(ir_imports=ir_imports, orq_sdk_git_ref=None)
    logs = caplog.records
    assert len(logs) == 1
    assert logs[0].name == "orquestra.sdk._base._conversions._imports"
    assert logs[0].levelname == "DEBUG"
    assert (
        logs[0].msg
        == "Refusing to dereference secret for url `https://example.com/some/path.git`"
    )
