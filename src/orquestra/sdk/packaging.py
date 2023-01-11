################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import re
from typing import Any, Optional

try:
    import importlib.metadata as metadata  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    import importlib_metadata as metadata  # type: ignore  # pragma: no cover

from orquestra.sdk import Import, PythonImports, TaskDef, exceptions


class PackagingError(exceptions.BaseRuntimeError):
    """Base error for any packaging errors"""


def get_installed_version(package_name: str) -> str:
    """
    Gets the installed version of a specific package

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


def InstalledImport(
    *,
    package_name: str,
    version_match: Optional[str] = None,
    fallback: Optional[Import] = None,
) -> Import:
    """
    Returns PythonImports for a task for the installed version of a package

    On an error, if a fallback is provided, the fallback is returned instead

    Args:
        package_name: the package to use as the import
        version_match: a regex string to match the installed version
        fallback: a fallback import to return if there are any issues
            in finding the package version

    Raises:
        PackagingError: If there are any issues finding an installed package and no
            fallback is provided.

    Returns:
        Either a PythonImports for package_name at the installed version or `fallback`

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


def execute_task(task: TaskDef, args, kwargs) -> Any:
    """
    A helper method for testing tasks by executing them outside of the workflow graph

    Use only for unittesting code.

    Args:
        task: the task you want to execute
        args: the positional arguments to pass to the task
        kwargs: the keyword arguments to pass to the task

    Returns:
        the result of executing a task
    """
    return task._TaskDef__sdk_task_body(*args, **kwargs)
