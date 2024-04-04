################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import re

import pytest

from orquestra.sdk._client._base._regex import SEMVER_REGEX, VERSION_REGEX


@pytest.mark.parametrize(
    "version",
    (
        "0.1.0",
        "0.1.1.dev1+gabc1234",
        "0.1.0+d19891213",
        "0.1.1.dev1+gabc1234.d19891213",
    ),
)
def test_version_and_semver_both_match(version: str):
    semver = re.match(SEMVER_REGEX, version)
    ver = re.match(VERSION_REGEX, version)
    assert semver is not None
    assert ver is not None
    assert semver.string == ver.string == version


def test_semver_has_named_groups():
    version = "0.1.1.dev1+gabc1234.d19891213"
    match = re.match(SEMVER_REGEX, version)
    assert match is not None
    assert match.groupdict() == {
        "buildmetadata": "gabc1234.d19891213",
        "major": "0",
        "minor": "1",
        "patch": "1",
        "prerelease": "dev1",
    }
