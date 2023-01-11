################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from orquestra.sdk.schema.configs import CONFIG_FILE_CURRENT_VERSION

TEST_CONFIG_JSON = {
    "version": CONFIG_FILE_CURRENT_VERSION,
    "configs": {
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
    },
    "default_config_name": "test_config_default",
}
