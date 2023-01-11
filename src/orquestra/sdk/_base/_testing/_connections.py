################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code related to setting up Ray connection in tests. Functions instead of pytest
fixtures for better reusability across scopes.
"""

import os
import platform
import random
import shutil
import typing as t
from contextlib import contextmanager
from pathlib import Path

from orquestra.sdk._ray import _dag


@contextmanager
def ray_suitable_temp_dir():
    """
    Context manager that generates a temporary directory and removes the content at
    exit. Generates shorter paths than `tempfile` would on a macOS. Workaround for Ray
    complaining about too long socket file path.

    Ray source code:
    https://github.com/ray-project/ray/blob/223ce3988e0a424d33a557c616eff6dd140b812c/python/ray/_private/node.py#L772-L776
    """
    if platform.system() == "Windows":
        # ray uses very long paths sometimes. We have to make the base short,
        # so it won't exceed Windows path limits
        temp_path = Path("C:/") / "tmp"
    else:
        random_int = random.randint(0, 1_000)
        temp_path = Path(f"/tmp/orq_rt/{random_int}")
    try:
        yield temp_path
    finally:
        try:
            shutil.rmtree(temp_path)
        except PermissionError:
            # Ray retains log file handlers even after calling `shutdown`. Minimal
            # reproducing example:
            #   print(psutil.open_files())
            #   ray.init()
            #   print(psutil.open_files())
            #   ray.shutdown()
            #   print(psutil.open_files())
            #
            # We won't try to fix Ray just to make our tests pass. Instead, we expect
            # that a PermissionError might happen at the time of removal of ray temp
            # dir. It should be fine to ignore it, as temporary directories should be
            # pruned by the OS anyway.
            pass


@contextmanager
def make_ray_conn() -> t.Iterator[_dag.RayParams]:
    """
    Initializes ray connection. By default, starts a linked cluster on its
    own.
    Can be changed to run against a background cluster that needs to
    be started separately ('ray start --head'). To do that, set the
    'RAY_CLUSTER_URL' env variable. `RAY_CLUSTER_URL="auto"` tells Ray to
    discover a running cluster.

    Yields the params used to initialize the connection.
    """

    cluster_url_env = os.getenv("RAY_CLUSTER_URL")

    with ray_suitable_temp_dir() as tmp_path:
        if cluster_url_env is not None:
            # Connecting to a background cluster?
            # - Use the address from the env var.
            # - Can't pass storage path.
            # - We shouldn't pass temp dir path either.
            address = cluster_url_env
            storage_path = None
            ray_temp_dir = None
        else:
            # No env var value? => Starting a linked cluster.
            # - Use the Ray's magic constant value for starting the cluster.
            # - Need to pass storage path (workflow data)
            # - Need to pass temp dir (Ray log files location).
            address = "local"
            storage_path = tmp_path / "ray" / "storage"
            ray_temp_dir = tmp_path / "ray" / "tmp"
        params = _dag.RayParams(
            address=address,
            log_to_driver=False,
            storage=str(storage_path),
            _temp_dir=str(ray_temp_dir),
        )

        # Ray plugs in its own breakpoint entrypoint during 'ray.init()', but
        # unfortunately it wrecks the PDB in pytest. To workaround this we can
        # bring back the old handler after initializing Ray connection.
        breakpoint_handler = os.environ.get("PYTHONBREAKPOINT", "")
        _dag.RayRuntime.startup(params)
        os.environ["PYTHONBREAKPOINT"] = breakpoint_handler

        yield params

        _dag.RayRuntime.shutdown()
