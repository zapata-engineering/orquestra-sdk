################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""
Class to manage local Orquestra services.
"""

import os
import subprocess
from pathlib import Path
from typing import List, Protocol, Type, Union

from . import _services_conf

ORQUESTRA_BASE_PATH = Path.home() / ".orquestra"


class Service(Protocol):
    @property
    def name(self) -> str:
        ...

    def up(self):
        ...

    def down(self):
        ...

    def is_running(self) -> bool:
        ...


def ray_temp_path():
    """
    ``ray_temp_path`` is used by RayRuntime to know where to read the logs from,
    in the scenario where we directly read the logs produced by Ray, bypassing the
    fluentbit docker container output.

    To ensure that we have compatibility with the Docker approach, it has to be
    consistent with:
    - orquestra.sdk._base._services_conf/.env
    - orquestra.sdk._base._services_conf/fluent-bit.conf

    However, Studio/Portal may need to override this in a special case.
    We will use an environment variable to override the location.
    This is unsupported outside of Studio/Portal
    """
    try:
        return Path(os.environ["ORQ_RAY_TEMP"])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray"


def ray_storage_path():
    """
    See ``ray_temp_path``
    """
    try:
        return Path(os.environ["ORQ_RAY_STORAGE"])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray_storage"


# It's used to identify fluentbit container when getting status.
# It has to be consistent with:
# - orquestra.sdk._base._services_conf/fluent-bit.conf
FLUENTBIT_CONTAINER_NAME = "orquestra_runtime_fluentbit"


def fluentbit_output_path():
    """
    It's used by RayRuntime to know where to read the logs from, in the scenario where
    we let FluentBit docker container to parse the Ray logs.

    It has to be consistent with:
    - orquestra.sdk._base._services_conf/fluent-bit.conf
    """
    return ORQUESTRA_BASE_PATH / "log"


def _services_conf_dir():
    return Path(_services_conf.__file__).resolve().parent


def _compose_path():
    return _services_conf_dir() / "docker-compose.yaml"


# Timeout for inter-process commands (seconds).
IPC_TIMEOUT = 20


class DockerException(Exception):
    pass


class _DockerClient:
    """
    Wraps ``python_on_whales`` docker client.

    Defers the import until `__init__` time. If ``python_on_whales`` is not installed,
    prompts the user to install the appropriate extra.
    """

    DOCKER_MISSING_EXCEPTION = ModuleNotFoundError(
        "In order to use Docker based logging, please make sure you install the "
        "optional dependencies with:\n`pip install orquestra-sdk[all]`",
        name="python_on_whales",
    )

    def __init__(self, compose_files: List[Union[str, Path]]):
        self._docker_exception: Type[Exception]
        try:
            import python_on_whales
        except ModuleNotFoundError:
            self._client = None
            # We need to keep track of *some* exception for ServiceManager
            self._docker_exception = RuntimeError
        else:
            self._client = python_on_whales.DockerClient(compose_files=compose_files)
            self._docker_exception = python_on_whales.exceptions.DockerException

    def build(self, *args, **kwargs):
        if self._client is None:
            raise self.DOCKER_MISSING_EXCEPTION
        try:
            return self._client.compose.build(*args, **kwargs)
        except self._docker_exception as e:
            raise DockerException(*e.args) from e

    def up(self, *args, **kwargs):
        if self._client is None:
            raise self.DOCKER_MISSING_EXCEPTION
        try:
            return self._client.compose.up(*args, **kwargs)
        except self._docker_exception as e:
            raise DockerException(*e.args) from e

    def down(self, *args, **kwargs):
        if self._client is None:
            raise self.DOCKER_MISSING_EXCEPTION
        try:
            return self._client.compose.down(*args, **kwargs)
        except self._docker_exception as e:
            raise DockerException(*e.args) from e

    def ps(self, *args, **kwargs):
        if self._client is None:
            raise self.DOCKER_MISSING_EXCEPTION
        try:
            return self._client.compose.ps(*args, **kwargs)
        except self._docker_exception as e:
            raise DockerException(*e.args) from e


class FluentManager:
    def __init__(self):
        self._docker_client = _DockerClient(compose_files=[_compose_path()])

    @property
    def name(self) -> str:
        """The human readable name for this service"""
        return "Fluent Bit"

    def up(self):
        """
        Starts a managed fluentbit container. If the container is already running, this
        does nothing.

        Raises:
            RuntimeError: if the fluentbit container couldn't se started
        """
        try:
            self._docker_client.build(quiet=True)
            self._docker_client.up(detach=True, quiet=True)
        except DockerException as e:
            raise RuntimeError(
                "Unable to start fluentbit. Do you have docker installed?"
            ) from e

    def down(self):
        """
        Shuts down the managed fluentbit container. If the container isn't running, this
        does nothing.
        """
        try:
            self._docker_client.down(timeout=IPC_TIMEOUT, volumes=True)
        except DockerException:
            # Raised when docker daemon isn't running.
            pass

    def is_running(self) -> bool:
        """
        Checks if the fluentbit container is running

        Returns:
            True if the container is running, False otherwise
        """
        try:
            containers = self._docker_client.ps()
        except (DockerException, ModuleNotFoundError):
            # Raised when docker daemon isn't running.
            return False

        fluentbit_containers = [
            c
            for c in containers
            if c.state.running and c.name == FLUENTBIT_CONTAINER_NAME
        ]
        return len(fluentbit_containers) > 0


class RayManager:
    @property
    def name(self) -> str:
        """The human readable name for this service"""
        return "Ray"

    def up(self):
        """
        Starts a Ray cluster. If a Ray is already running, this does nothing.

        Raises:
            RuntimeError: if we ask Ray to start and it fails
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        # 'ray start' fails if the cluster can't be started, or another problem
        # occurred. I don't know a good way to differentiate between these
        # scenarios, so the strategy is:
        # 1. Attempt to start the Ray cluster. Ignore errors.
        # 2. Check if the cluster is running to confirm that either the cluster was
        #    started, or it had been already running prior to this command.
        _ = subprocess.run(
            [
                "ray",
                "start",
                "--head",
                f"--temp-dir={ray_temp_path()}",
                f"--storage={ray_storage_path()}",
            ],
            check=False,
            timeout=IPC_TIMEOUT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if not self.is_running():
            raise RuntimeError("Couldn't start Ray cluster")

    def down(self):
        """
        Shuts down the managed Ray cluster. If Ray isn't running, this does nothing.

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        # 'ray stop' can be ran multiple times. It doesn't fail if no cluster is
        # running.
        _ = subprocess.run(
            ["ray", "stop"],
            check=True,
            timeout=IPC_TIMEOUT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def is_running(self):
        """
        Checks if a Ray cluster is running

        Returns:
            True if the cluster is running, False otherwise

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        proc = subprocess.run(
            ["ray", "status"],
            check=False,
            timeout=IPC_TIMEOUT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return proc.returncode == 0


class ServiceManager:
    """
    This is only used in the corq CLI and is a thin wrapper above the underlying service
    managers.
    """

    def __init__(
        self,
        ray_manager=RayManager(),
        fluent_manager=FluentManager(),
    ):
        self._fluent = fluent_manager
        self._ray = ray_manager

    def up(self):
        """
        Starts the managed background services. If a given service is already running,
        and this method is called, this does nothing.

        Raises:
            RuntimeError: if any of the underlying services couldn't be started
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        self._fluent.up()
        self._ray.up()

    def down(self):
        """
        Shuts down the managed background services. If a given service isn't running,
        and this method is called, it does nothing.

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        self._fluent.down()
        self._ray.down()

    def is_ray_running(self) -> bool:
        """
        Checks if a local Ray cluster is running.

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        return self._ray.is_running()

    def is_fluentbit_running(self) -> bool:
        """
        Checks if the fluentbit docker container is running.
        """
        return self._fluent.is_running()
