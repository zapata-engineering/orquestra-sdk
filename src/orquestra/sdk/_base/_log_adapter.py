################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""
Log adapter adds a workflow context to logs, Workflow ID and Task ID.
"""

import logging
import warnings


def make_logger():
    logger = logging.getLogger(__name__)

    # Ensure it prints somewhere
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger


def workflow_logger() -> logging.Logger:
    """
    Deprecated: please use the standard ``logging`` module.

    Returns a Logger instance with a context of current workflow/task.

    Each call of this function creates a new object. It shouldn't be retained across
    task runs.
    """
    warnings.warn(
        "`sdk.workflow_logger()` is deprecated. Please use the standard `logging` module.",  # noqa: E501
        FutureWarning,
    )

    # TODO: remove this function.
    # https://zapatacomputing.atlassian.net/browse/ORQSDK-887

    return make_logger()


def wfprint(*values):
    """
    Deprecated: please use a standard ``print()``.

    This function wraps prints from workflow tasks.
    """
    warnings.warn(
        "`sdk.wfprint()` is deprecated. Please use a standard `print()`.",
        FutureWarning,
    )

    # TODO: remove this function.
    # https://zapatacomputing.atlassian.net/browse/ORQSDK-887
    logger = workflow_logger()
    logger.info(msg=" ".join(values))
