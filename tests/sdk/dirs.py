################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import contextlib
import os
import tempfile


@contextlib.contextmanager
def _chdir(new_dir: str):
    original_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(original_dir)


@contextlib.contextmanager
def ch_temp_dir():
    with tempfile.TemporaryDirectory() as dir_path:
        with _chdir(dir_path):
            yield dir_path
