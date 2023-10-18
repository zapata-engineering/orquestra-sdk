################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Glue code between the public API and concept-specific modules."""
import typing as t

from orquestra.sdk import exceptions

from ....schema.configs import ConfigName
from ....schema.workflow_run import WorkflowRunId
from .._config import RuntimeConfig
from ._runtime import RuntimeRepo


class ConfigByNameRepo:
    """Figures out what config the API user had in mind based on the config object."""

    def normalize_config(
        self,
        config: t.Union[ConfigName, RuntimeConfig],
    ) -> RuntimeConfig:
        """Returns config object or reads the entry from the config file.

        Raises:
            orquestra.sdk.exceptions.ConfigFileNotFoundError: when there's no config
                file at the expected location.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the passed
                ``config`` name doesn't match the config file's content.
            TypeError: when invalid config object is passed
        """
        if isinstance(config, RuntimeConfig):
            # EZ. Passed-in explicitly.
            resolved_config = config
        elif isinstance(config, str):
            # Shorthand: just the config name.
            resolved_config = RuntimeConfig.load(config)
        else:
            raise TypeError(f"'config' is of unsupported type {type(config)}.")

        return resolved_config


class ConfigByIDRepo:
    """Figures out what config the API user had in mind based on the workflow run."""

    def __init__(
        self,
        config_by_name_repo: ConfigByNameRepo = ConfigByNameRepo(),
        runtime_repo: RuntimeRepo = RuntimeRepo(),
    ):
        self._config_by_name_repo = config_by_name_repo
        self._runtime_repo = runtime_repo

    def get_config(
        self,
        wf_run_id: WorkflowRunId,
        config: t.Union[None, ConfigName, RuntimeConfig],
    ) -> RuntimeConfig:
        """Find config that matches the workflow run.

        If the user has passed ``config`` explicitly, it will be returned. Otherwise,
        query all the known runtimes until a matching workflow run is found.

        Args:
            wf_run_id: ID of the workflow run the user is interested in.
            config: user-provided config if passed in explicitly

        Raises:
            orquestra.sdk.exceptions.ConfigFileNotFoundError: when there's no config
                file at the expected location.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when the passed
                ``config`` name doesn't match the config file's content.
            orquestra.sdk.exceptions.RuntimeQuerySummaryError: when all known runtimes
                have been queried, but none knows about the workflow run.
            TypeError: when invalid config object is passed
        """
        if config is not None:
            try:
                return self._config_by_name_repo.normalize_config(config)
            except (
                exceptions.ConfigFileNotFoundError,
                exceptions.ConfigNameNotFoundError,
                TypeError,
            ):
                raise

        config_names = RuntimeConfig.list_configs()

        not_found_configs: t.List[RuntimeConfig] = []
        unauthorized_configs: t.List[RuntimeConfig] = []
        not_running_configs: t.List[RuntimeConfig] = []

        for config_name in config_names:
            try:
                config_obj = RuntimeConfig.load(config_name)
            except (
                exceptions.ConfigFileNotFoundError,
                exceptions.ConfigNameNotFoundError,
            ):
                raise

            try:
                runtime = self._runtime_repo.get_runtime(config_obj)
            except exceptions.RayNotRunningError:
                # Ray connection is set up in `RayRuntime.__init__()`. We'll get the
                # exception when runtime object is created.
                not_running_configs.append(config_obj)
                continue

            try:
                # TODO (ORQSDK-990): short circuit remote call with a token validity
                # check
                _ = runtime.get_workflow_run_status(wf_run_id)
            except exceptions.WorkflowRunNotFoundError:
                not_found_configs.append(config_obj)
                continue
            except exceptions.UnauthorizedError:
                unauthorized_configs.append(config_obj)
                continue

            return config_obj

        # We're here: no runtime knows about this workflow run ID
        raise exceptions.RuntimeQuerySummaryError(
            wf_run_id=wf_run_id,
            not_found_runtimes=[
                _runtime_info_from_config(config) for config in not_found_configs
            ],
            unauthorized_runtimes=[
                _runtime_info_from_config(config) for config in unauthorized_configs
            ],
            not_running_runtimes=[
                _runtime_info_from_config(config) for config in not_running_configs
            ],
        )


def _runtime_info_from_config(
    config: RuntimeConfig,
) -> exceptions.RuntimeQuerySummaryError.RuntimeInfo:
    try:
        uri = getattr(config, "uri")
    except AttributeError:
        uri = None

    return exceptions.RuntimeQuerySummaryError.RuntimeInfo(
        runtime_name=config.runtime_name,
        config_name=config.name,
        server_uri=uri,
    )
