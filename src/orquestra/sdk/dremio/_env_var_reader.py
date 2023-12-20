import os


class EnvVarReader:
    def __init__(self, var_name: str):
        self._var_name = var_name

    def read(self) -> str:
        return os.getenv(self._var_name)
