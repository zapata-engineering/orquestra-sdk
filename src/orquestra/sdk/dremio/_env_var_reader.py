import os
from orquestra.sdk.exceptions import EnvVarNotFoundError


class EnvVarReader:
    def __init__(self, var_name: str):
        self._var_name = var_name

    def read(self) -> str:
        try:
            return os.environ[self._var_name]
        except KeyError as e:
            raise EnvVarNotFoundError(
                "Environment variable is required but isn't set", self._var_name
            ) from e
