################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import typing as t

from orquestra.sdk._shared.schema.configs import CONFIG_FILE_CURRENT_VERSION

TEST_CONFIGS_DICT: t.Mapping[str, t.Mapping[str, t.Any]] = {
    "test_config_default": {
        "config_name": "test_config_default",
        "runtime_name": "CE_REMOTE",
        "runtime_options": {
            "uri": "test_config_default_uri",
            "token": "test_config_default_token",
        },
    },
    "test_config_no_runtime_options": {
        "config_name": "test_config_no_runtime_options",
        "runtime_name": "CE_REMOTE",
        "runtime_options": {},
    },
    "test_config_ce": {
        "config_name": "test_config_ce",
        "runtime_name": "CE_REMOTE",
        "runtime_options": {
            "uri": "test_uri",
            "token": "test_token",
        },
    },
    "actual_name": {
        "config_name": "actual_name",
        "runtime_name": "CE_REMOTE",
        "runtime_options": {
            "uri": "http://actual_name.domain",
            "token": "this_token_best_token",
        },
    },
    "proper_token": {
        "config_name": "proper_token",
        "runtime_name": "CE_REMOTE",
        "runtime_options": {
            "uri": "http://some_cool_uri.io",
            "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJlbWFpbCI6Im15X2hpZGRlbl9lbWFpbEBsb3ZlbHktZW1haWwuY29tIn0.HNQ4xovOEWQh7HbxmwiViVR-Xw792yXrbUDknzChncA",  # noqa
        },
    },
    "improper_token": {
        "config_name": "improper_token",
        "runtime_name": "CE_REMOTE",
        "runtime_options": {
            "uri": "http://some_cool_uri.io",
            "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJlbWFpbCI6Im15X2hpZGRl9lbWFpbEBsb3ZlbHktZW1haWwuY29tIn0.HNQ4xovOEWQh7HbxmwiViVR-Xw792yXrbUDknzChncA",  # noqa
        },
    },
}
TEST_CONFIG_JSON: t.Mapping[str, t.Any] = {
    "version": CONFIG_FILE_CURRENT_VERSION,
    "configs": TEST_CONFIGS_DICT,
    "default_config_name": "test_config_default",
}
