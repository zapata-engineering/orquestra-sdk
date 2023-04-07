################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Logging configuration for the ``orq`` CLI.

Note: we distribute a library (``orquestra-sdk``) and an app (the ``orq`` CLI). Logging
should be only configured by apps.
"""

import typing as t
import os
import logging
from orquestra.sdk._base import _env


def _is_truthy(env_var_value: t.Optional[str]):
    return env_var_value in {"1", "true"}


def _env_flag_set(env_var_name: str) -> bool:
    value = os.getenv(env_var_name)
    return _is_truthy(value)


def configure_verboseness_if_needed():
    if not _env_flag_set(_env.ORQ_VERBOSE):
        return

    logging.basicConfig(level=logging.DEBUG)
