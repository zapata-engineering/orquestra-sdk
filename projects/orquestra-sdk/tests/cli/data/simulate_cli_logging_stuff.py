################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import logging

from orquestra.sdk._client._base.cli import _cli_logs

_cli_logs.configure_verboseness_if_needed()


logging.debug("root logger debug message")
logging.getLogger(__name__).debug("module logger debug message")
