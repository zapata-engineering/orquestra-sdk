################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import typing as t
from pathlib import Path

from orquestra.sdk import exceptions
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName


def build_runtime_from_config(
    project_dir: Path, config: RuntimeConfiguration, verbose: bool = False
) -> RuntimeInterface:
    """
    Centralized place to get runtime object based on config.

    There are a couple of runtime runtime integrations implemented as separate
    classes. This factory function solves the problem of figuring out which
    class to use.
    """
    # Imports are deferred to cut down on the import graph for CLI latency. The
    # subgraphs for Ray and for QE are distinct, and both take a lot of time to
    # import.
    selected_runtime: t.Type[RuntimeInterface]
    if config.runtime_name == RuntimeName.RAY_LOCAL:
        import orquestra.sdk._ray._dag

        selected_runtime = orquestra.sdk._ray._dag.RayRuntime
    elif config.runtime_name == RuntimeName.QE_REMOTE:
        import orquestra.sdk._base._qe._qe_runtime

        selected_runtime = orquestra.sdk._base._qe._qe_runtime.QERuntime
    elif config.runtime_name == RuntimeName.CE_REMOTE:
        import orquestra.sdk._base._driver._ce_runtime

        selected_runtime = orquestra.sdk._base._driver._ce_runtime.CERuntime
    else:
        raise exceptions.NotFoundError(f"Unknown runtime: {config.runtime_name}")

    return selected_runtime.from_runtime_configuration(
        project_dir=project_dir,
        config=config,
        verbose=verbose,
    )
