################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""Utilities for handling pip packages."""

import re
import sys
from typing import Any, Optional

try:
    import importlib.metadata as metadata  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    import importlib_metadata as metadata  # type: ignore  # pragma: no cover

import packaging.version

from orquestra.sdk import Import, PythonImports, TaskDef, exceptions
from orquestra.sdk.schema import ir


class PackagingError(exceptions.BaseRuntimeError):
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


def InstalledImport(
    *,
    package_name: str,
    version_match: Optional[str] = None,
    fallback: Optional[Import] = None,
) -> Import:
    """Returns PythonImports for a task for the installed version of a package.

    On an error, if a fallback is provided, the fallback is returned instead.

    Args:
        package_name: the package to use as the import.
        version_match: a regex string to match the installed version.
        fallback: a fallback import to return if there are any issues
            in finding the package version.

    Raises:
        PackagingError: If there are any issues finding an installed package and no
            fallback is provided.

    Returns:
        Either a PythonImports for package_name at the installed version or `fallback`.

    """
    try:
        version = get_installed_version(package_name)
        if version_match is not None:
            if re.match(version_match, version) is None:
                raise PackagingError(
                    f"Package version mismatch: "
                    f"{package_name}=={version}\n"
                    f'Expected version to match "{version_match}"'
                )
    except PackagingError as e:
        if fallback is not None:
            return fallback
        raise e

    return PythonImports(f"{package_name}=={version}")


def execute_task(task: TaskDef, args: tuple, kwargs: dict) -> Any:
    """A helper method to testing tasks by executing them outside of the workflow graph.

    Use only for unittesting code.

    Args:
        task: the task you want to execute.
        args: the positional arguments to pass to the task.
        kwargs: the keyword arguments to pass to the task.

    Returns:
        the result of executing a task.
    """
    return task._TaskDef__sdk_task_body(*args, **kwargs)
