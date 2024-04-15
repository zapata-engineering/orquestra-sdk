################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Logging configuration for the ``orq`` CLI.

Note: we distribute a library (``orquestra-sdk``) and an app (the ``orq`` CLI).
Logging should be only configured by apps.
"""

import logging

from ..._base import _env


def configure_verboseness_if_needed():  # pragma: no cover
    # This function is tested via a subprocess so it's not captured by our coverage
    # metrics.
    if not _env.flag_set(_env.ORQ_VERBOSE):
        return

    logging.basicConfig(level=logging.DEBUG)
