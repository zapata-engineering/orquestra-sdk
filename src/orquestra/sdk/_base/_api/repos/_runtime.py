################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from pathlib import Path

from ....exceptions import NotFoundError, RuntimeConfigError
from ....schema.configs import RuntimeConfiguration, RuntimeName
from ...abc import RuntimeInterface
from .._config import RuntimeConfig


class RuntimeRepo:
    def get_runtime(self, config: RuntimeConfig) -> RuntimeInterface:
        # Imports are deferred to cut down on the import graph for CLI latency. The
        # subgraphs for Ray and for CE are distinct, and both take a lot of time to
        # import.
        if config.runtime_name == RuntimeName.RAY_LOCAL:
            return _build_ray_runtime()
        elif config.runtime_name == RuntimeName.IN_PROCESS:
            return _build_in_process_runtime()
        elif config.runtime_name == RuntimeName.CE_REMOTE:
            return _build_ce_runtime(config)
        else:
            raise NotFoundError(f"Unknown runtime: {config.runtime_name}")


def _build_ray_runtime():
    project_dir = Path(".")
    config_model = _config_model_from_config(RuntimeConfig.ray())

    import orquestra.sdk._ray._dag

    return orquestra.sdk._ray._dag.RayRuntime(
        project_dir=project_dir,
        config=config_model,
    )


def _config_model_from_config(config: RuntimeConfig) -> RuntimeConfiguration:
    # TODO: figure out cases where name is none
    assert config.name is not None, "Config should have a name innit"

    return RuntimeConfiguration(
        config_name=config.name,
        runtime_name=config.runtime_name,
        runtime_options=config._get_runtime_options(),
    )


def _build_in_process_runtime():
    import orquestra.sdk._base._in_process_runtime

    return orquestra.sdk._base._in_process_runtime.InProcessRuntime()


def _build_ce_runtime(config: RuntimeConfig):
    import orquestra.sdk._base._driver._ce_runtime
    import orquestra.sdk._base._driver._client

    # We're using a reusable session to allow shared headers
    # In the future we can store cookies, etc too.

    try:
        base_uri: str = getattr(config, "uri")
        token: str = getattr(config, "token")
    except AttributeError as e:
        raise RuntimeConfigError(
            "Invalid CE configuration. Did you login first?"
        ) from e

    uri_provider = orquestra.sdk._base._driver._client.ExternalUriProvider(base_uri)

    client = orquestra.sdk._base._driver._client.DriverClient.from_token(
        token=token, uri_provider=uri_provider
    )

    # TODO (ORQSDK-971): remove `config_model`. The connection params are already
    # handled by `client`.
    config_model = _config_model_from_config(config)

    return orquestra.sdk._base._driver._ce_runtime.CERuntime(
        config=config_model,
        client=client,
        verbose=False,
    )
