################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import json
import subprocess
import sys
from pathlib import Path

import pytest

BASE_PATH = Path(__file__).parent.parent.parent.parent
PYTHON_EXECUTABLE = python_executable = str(sys.executable)


@pytest.fixture(scope="module")
def _examples_dir():
    path = (BASE_PATH / "docs" / "examples").resolve()
    if path.is_dir():
        return str(path)

    raise NotADirectoryError(f"No such directory: {path}")


def run_command(command, shell=False, stdout=False, verbose=True, capture_output=True):
    """Run a command, handle output.
    * Command : list of system strings.
    * Return : (boolean) True if command could be run successfully, False otherwise
    """

    if verbose:
        print(f"Running: {command}")

    try:
        proc = subprocess.run(command, shell=shell, capture_output=capture_output)
    except Exception as ex:
        print(f"Exception running command {command}: {str(ex)}")
        return False

    if stdout:
        print(proc.stdout.decode())

    if verbose:
        print(proc.stderr.decode())

    if proc.returncode == 0:
        return True

    return False


class TempConfigFile(object):
    """
    Config manager to store the current contents of the config.json file and restore
    the file to its previous state on exit.
    """

    _default_config_save_file = Path.home() / ".orquestra" / "config.json"

    def __init__(self):
        if self._default_config_save_file.exists():
            with open(self._default_config_save_file, "r") as f:
                self._old_data = json.load(f)
        else:
            self._old_data = None

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        if self._old_data is None:
            self._default_config_save_file.unlink(missing_ok=True)

            assert not self._default_config_save_file.exists(), (
                f"The file created at {self._default_config_save_file} as part of this "
                "test was not correctly removed, you may have to delete it manually."
            )
        else:
            with open(self._default_config_save_file, "w") as f:
                json.dump(self._old_data, f, indent=2)

            with open(self._default_config_save_file, "r") as f:
                data = json.load(f)
            assert data == self._old_data, (
                f"Could not clean up changes to {self._default_config_save_file}. "
                f"File should contain:\n{self._old_data}"
            )


def test_quickstart(capsys, _examples_dir):
    command = f"{PYTHON_EXECUTABLE} {_examples_dir}/quickstart.py"
    result_status = run_command(command, shell=True, stdout=True)
    captured = capsys.readouterr()

    expected_output = "Hello Orquestra!"

    assert result_status, (
        f"Process {command} did not complete successfully."
        + "=" * 80
        + f"\nSTDOUT:\n{captured.out}"
        + "=" * 80
        + f"\nSTDERR:\n{captured.err}"
        + "=" * 80
    )
    assert expected_output in captured.out


def test_config_management(capsys, _examples_dir):
    """
    The config management example prints the details of a config three times: once
    before it is saved, once after it has been loaded from the file, and once to
    demonstrate that configs can be manually renamed. We confirm this by checking that
    the config details occur exactly 3 times in the captured output.
    """
    command = f"{PYTHON_EXECUTABLE} {_examples_dir}/config_management.py"

    with TempConfigFile():
        result_status = run_command(command, shell=True, stdout=True)
    captured = capsys.readouterr()

    assert result_status, (
        f"Process '{command}' did not complete successfully."
        + "=" * 80
        + f"\nSTDOUT:\n{captured.out}"
        + "=" * 80
        + f"\nSTDERR:\n{captured.err}"
        + "=" * 80
    )

    expected_outputs = [
        "RuntimeConfiguration 'prod-d' for runtime QE_REMOTE  with parameters:",
        "- uri: https://prod-d.orquestra.io",
        "- token: my_token",
    ]
    for output in expected_outputs:
        assert captured.out.count(output) == 2, (
            f"Found {captured.out.count(output)} instances of '{output}' in "
            f"'{captured.out}' (expected 2)."
        )
