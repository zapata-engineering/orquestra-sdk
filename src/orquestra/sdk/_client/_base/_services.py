################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""Class to manage local Orquestra services."""

import os
import subprocess
from pathlib import Path
from typing import Protocol

from ._env import (
    ORQ_TASK_RUN_LOGS_DIR,
    RAY_PLASMA_PATH_ENV,
    RAY_STORAGE_PATH_ENV,
    RAY_TEMP_PATH_ENV,
)

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
    """Used by RayRuntime to know where to look for the log files to read.

    Studio/Portal may need to override this in a special case.
    We will use an environment variable to override the location.
    This is unsupported outside of Studio/Portal.
    """
    try:
        return Path(os.environ[RAY_TEMP_PATH_ENV])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray"


def ray_storage_path():
    """See ``ray_temp_path``."""
    try:
        return Path(os.environ[RAY_STORAGE_PATH_ENV])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray_storage"


def ray_plasma_path():
    """See ``ray_temp_path``."""
    try:
        return Path(os.environ[RAY_PLASMA_PATH_ENV])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "ray_plasma"


def redirected_logs_dir():
    """Used by log redirection to store workflow logs.

    By default, this is `~/.orquestra/logs` and each workflow will have log files like:
     * `~/.orquestra/logs/wf/<wf run ID>/task/<task invocation ID>.err`
     * `~/.orquestra/logs/wf/<wf run ID>/task/<task invocation ID>.out`
    """
    try:
        return Path(os.environ[ORQ_TASK_RUN_LOGS_DIR])
    except KeyError:
        return ORQUESTRA_BASE_PATH / "logs"


# Timeout for inter-process commands (seconds).
IPC_TIMEOUT = 20


class RayManager:
    @property
    def name(self) -> str:
        """The human readable name for this service."""
        return "Ray"

    def up(self):
        """Starts a Ray cluster.

        If Ray is already running, this does nothing.

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed.
        """
        ray_temp = ray_temp_path()
        ray_storage = ray_storage_path()
        ray_plasma = ray_plasma_path()

        for path in (ray_temp, ray_storage, ray_plasma):
            path.mkdir(exist_ok=True, parents=True)

        # 'ray start' fails if the cluster can't be started, or another problem
        # occurred. I don't know a good way to differentiate between these
        # scenarios, so the strategy is:
        # 1. Attempt to start the Ray cluster. Ignore errors.
        # 2. Check if the cluster is running to confirm that either the cluster was
        #    started, or it had been already running prior to this command.
        proc = subprocess.run(
            [
                "ray",
                "start",
                "--head",
                f"--temp-dir={ray_temp}",
                f"--storage={ray_storage}",
                f"--plasma-directory={ray_plasma}",
            ],
            check=False,
            timeout=IPC_TIMEOUT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if not self.is_running():
            try:
                proc.check_returncode()
            except subprocess.CalledProcessError:
                raise

    def down(self):
        """Shuts down the managed Ray cluster.

        If Ray isn't running, this does nothing.

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        # 'ray stop' can be ran multiple times. It doesn't fail if no cluster is
        # running.
        try:
            _ = subprocess.run(
                ["ray", "stop"],
                check=True,
                timeout=IPC_TIMEOUT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            raise

    def is_running(self):
        """Checks if a Ray cluster is running.

        Returns:
            True if the cluster is running, False otherwise

        Raises:
            subprocess.CalledProcessError: if calling the `ray` CLI failed. This
                shouldn't happen in regular conditions.
        """
        try:
            proc = subprocess.run(
                ["ray", "status"],
                check=False,
                timeout=IPC_TIMEOUT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            raise
        return proc.returncode == 0
