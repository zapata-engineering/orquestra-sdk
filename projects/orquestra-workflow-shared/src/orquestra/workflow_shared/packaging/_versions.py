################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""Utilities for handling pip packages."""

import sys

try:
    import importlib.metadata as metadata  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    import importlib_metadata as metadata  # type: ignore  # pragma: no cover

import packaging.version

from ..exceptions import BaseRuntimeError
from ..schema import ir


class PackagingError(BaseRuntimeError):
    """Base error for any packaging errors."""


def get_installed_version(package_name: str) -> str:
    """Gets the installed version of a specific package.

    A package is what you use when you `pip install` something, not the module:

     - Package: `orquestra-sdk`
     - Module: `orquestra.sdk`

    Args:
        package_name: the name of the package to get the version of

    Raises:
        PackagingError: When the requested package cannot be found

    Returns:
        the installed version of package_name

    """
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError as e:
        raise PackagingError(f"Package not found: {package_name}") from e


def parse_version_str(version_str: str) -> ir.Version:
    parsed_sdk_version = packaging.version.parse(version_str)
    return ir.Version(
        original=version_str,
        major=parsed_sdk_version.major,
        minor=parsed_sdk_version.minor,
        patch=parsed_sdk_version.micro,
        is_prerelease=parsed_sdk_version.is_prerelease,
    )


def get_installed_version_model(package_name: str) -> ir.Version:
    sdk_version_str = get_installed_version(package_name)
    return parse_version_str(sdk_version_str)


def get_current_sdk_version() -> ir.Version:
    return get_installed_version_model("orquestra-sdk")


def get_current_python_version() -> ir.Version:
    return ir.Version(
        original=sys.version,
        major=sys.version_info.major,
        minor=sys.version_info.minor,
        patch=sys.version_info.micro,
        is_prerelease=sys.version_info.releaselevel != "final",
    )
