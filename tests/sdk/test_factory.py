################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################


import pytest

from orquestra.sdk._client._base._driver._ce_runtime import CERuntime
from orquestra.sdk._client._base._factory import build_runtime_from_config
from orquestra.sdk._runtime._ray._dag import RayRuntime
from orquestra.sdk._shared.exceptions import QERemoved, RuntimeConfigError
from orquestra.sdk._shared.schema.configs import RuntimeConfiguration, RuntimeName


class TestBuildRuntimeFromConfig:
    @pytest.mark.parametrize(
        "config_type, runtime_options, expected_runtime",
        [
            (
                RuntimeName.RAY_LOCAL,
                {
                    "address": "auto",
                    "log_to_driver": False,
                    "storage": None,
                    "temp_dir": None,
                    "configure_logging": None,
                },
                RayRuntime,
            ),
            (RuntimeName.CE_REMOTE, {"uri": "blah", "token": "bla"}, CERuntime),
        ],
    )
    def test_happy_path(
        self, monkeypatch, config_type, runtime_options, expected_runtime
    ):
        if config_type == RuntimeName.RAY_LOCAL:
            # monkeypatch startup for Ray to not start the actual cluster
            monkeypatch.setattr(RayRuntime, "startup", lambda *_, **__: ...)

        runtime_config = RuntimeConfiguration(
            config_name="mock",
            runtime_name=config_type,
            runtime_options=runtime_options,
        )
        runtime = build_runtime_from_config(config=runtime_config)
        assert isinstance(runtime, expected_runtime)

    @pytest.mark.parametrize(
        "config_type, runtime_options",
        [
            (RuntimeName.CE_REMOTE, {"uri": "blah", "no_token": "bla"}),
        ],
    )
    def test_invalid_remote_config(self, config_type, runtime_options):
        runtime_config = RuntimeConfiguration(
            config_name="mock",
            runtime_name=config_type,
            runtime_options=runtime_options,
        )

        with pytest.raises(RuntimeConfigError):
            build_runtime_from_config(config=runtime_config)

    def test_qe_removed(self):
        runtime_config = RuntimeConfiguration(
            config_name="mock",
            runtime_name=RuntimeName.QE_REMOTE,
        )

        with pytest.raises(QERemoved):
            build_runtime_from_config(config=runtime_config)
