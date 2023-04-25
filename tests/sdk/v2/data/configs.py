################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import typing as t

from orquestra.sdk.schema.configs import CONFIG_FILE_CURRENT_VERSION

TEST_CONFIGS_DICT: t.Mapping[str, t.Mapping[str, t.Any]] = {
    "test_config_default": {
        "config_name": "test_config_default",
        "runtime_name": "QE_REMOTE",
        "runtime_options": {
            "uri": "test_config_default_uri",
            "token": "test_config_default_token",
        },
    },
    "test_config_no_runtime_options": {
        "config_name": "test_config_no_runtime_options",
        "runtime_name": "QE_REMOTE",
        "runtime_options": {},
    },
    "test_config_qe": {
        "config_name": "test_config_qe",
        "runtime_name": "QE_REMOTE",
        "runtime_options": {
            "uri": "test_uri",
            "token": "test_token",
        },
    },
    "actual_name": {
        "config_name": "actual_name",
        "runtime_name": "QE_REMOTE",
        "runtime_options": {
            "uri": "http://actual_name.domain",
            "token": "this_token_best_token",
        },
    },
}
TEST_CONFIG_JSON: t.Mapping[str, t.Any] = {
    "version": CONFIG_FILE_CURRENT_VERSION,
    "configs": TEST_CONFIGS_DICT,
    "default_config_name": "test_config_default",
}
