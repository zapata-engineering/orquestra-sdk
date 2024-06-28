################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################

from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.abc import RuntimeInterface
from orquestra.workflow_shared.schema.configs import RuntimeConfiguration, RuntimeName


def build_runtime_from_config(
    config: RuntimeConfiguration, verbose: bool = False
) -> RuntimeInterface:
    """Centralized place to get runtime object based on config.

    There are a couple of runtime integrations implemented as separate
    classes. This factory function solves the problem of figuring out which
    class to use.
    """
    # Imports are deferred to cut down on the import graph for CLI latency. The
    # subgraphs for Ray and for CE are distinct, and both take a lot of time to
    # import.
    if config.runtime_name == RuntimeName.RAY_LOCAL:
        from orquestra.workflow_runtime import RayRuntime

        return RayRuntime(
            config=config,
        )
    elif config.runtime_name == RuntimeName.IN_PROCESS:
        import orquestra.sdk._client._base._in_process_runtime

        return orquestra.sdk._client._base._in_process_runtime.InProcessRuntime()
    elif config.runtime_name == RuntimeName.CE_REMOTE:
        return _build_ce_runtime(config, verbose)
    elif config.runtime_name == RuntimeName.QE_REMOTE:
        raise exceptions.QERemoved(
            "QE support has been removed. "
            f"Use CE by logging in again with `orq login -c {config.config_name}`"
        )
    else:
        raise exceptions.NotFoundError(f"Unknown runtime: {config.runtime_name}")


def _build_ce_runtime(config: RuntimeConfiguration, verbose: bool):
    import orquestra.sdk._client._base._driver._ce_runtime
    import orquestra.sdk._client._base._driver._client

    # We're using a reusable session to allow shared headers
    # In the future we can store cookies, etc too.

    try:
        base_uri = config.runtime_options["uri"]
        token = config.runtime_options["token"]
    except KeyError as e:
        raise exceptions.RuntimeConfigError(
            "Invalid CE configuration. Did you login first?"
        ) from e

    uri_provider = orquestra.sdk._client._base._driver._client.ExternalUriProvider(
        base_uri
    )

    client = orquestra.sdk._client._base._driver._client.DriverClient.from_token(
        token=token, uri_provider=uri_provider
    )

    return orquestra.sdk._client._base._driver._ce_runtime.CERuntime(
        config=config,
        client=client,
        token=token,
        verbose=verbose,
    )
