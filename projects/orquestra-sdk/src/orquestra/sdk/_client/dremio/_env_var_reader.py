################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################
import os

from orquestra.workflow_shared.exceptions import EnvVarNotFoundError


class EnvVarReader:
    def __init__(self, var_name: str):
        self._var_name = var_name

    def read(self) -> str:
        try:
            value = os.environ[self._var_name]
        except KeyError as e:
            raise EnvVarNotFoundError(
                "Environment variable is required but isn't set", self._var_name
            ) from e

        if value == "":
            raise EnvVarNotFoundError(
                "Environment variable is required but has no value", self._var_name
            )
        else:
            return value
