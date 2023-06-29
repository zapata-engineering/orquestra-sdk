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
    if config.runtime_name == RuntimeName.RAY_LOCAL:
        import orquestra.sdk._ray._dag

        return orquestra.sdk._ray._dag.RayRuntime(
            project_dir=project_dir,
            config=config,
        )
    elif config.runtime_name == RuntimeName.QE_REMOTE:
        import orquestra.sdk._base._qe._qe_runtime

        return orquestra.sdk._base._qe._qe_runtime.QERuntime(
            project_dir=project_dir, config=config, verbose=verbose
        )

    elif config.runtime_name == RuntimeName.CE_REMOTE:
        return _build_ce_runtime(config, verbose)
    else:
        raise exceptions.NotFoundError(f"Unknown runtime: {config.runtime_name}")


def _build_ce_runtime(config: RuntimeConfiguration, verbose: bool):
    import orquestra.sdk._base._driver._ce_runtime
    import orquestra.sdk._base._driver._client

    # We're using a reusable session to allow shared headers
    # In the future we can store cookies, etc too.

    try:
        base_uri = config.runtime_options["uri"]
        token = config.runtime_options["token"]
    except KeyError as e:
        raise exceptions.RuntimeConfigError(
            "Invalid CE configuration. Did you login first?"
        ) from e

    uri_provider = orquestra.sdk._base._driver._client.ExternalUriProvider(base_uri)

    client = orquestra.sdk._base._driver._client.DriverClient.from_token(
        token=token, uri_provider=uri_provider
    )

    return orquestra.sdk._base._driver._ce_runtime.CERuntime(
        config=config,
        client=client,
        verbose=verbose,
    )
