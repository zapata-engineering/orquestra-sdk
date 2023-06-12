################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
A script for regenerating the recorded logs in the "./ray_temp" dir.

Prerequirements:
    * Delete ``~/.orquestra/ray`` to start with a clean state
    * ``orq up``
"""

import os
import shutil
from pathlib import Path

from orquestra.sdk._base._testing import _example_wfs

REAL_RAY_TEMP = Path.home() / ".orquestra" / "ray"
TEST_RAY_TEMP = Path(__file__).parent / "ray_temp"


def main():
    print(f"Removing old ray logs at {TEST_RAY_TEMP}")
    shutil.rmtree(TEST_RAY_TEMP, ignore_errors=True)

    wf_def = _example_wfs.wf_using_python_imports
    print("Starting the workflow on local Ray")

    tell_tale = "hello, there!"
    wf_run = wf_def(log_message=tell_tale).run("ray")

    wf_run.wait_until_finished()

    # Ray temp has session directories with timestamps + a 'session_latest' symlink.
    # We'll want to replicate this in our test data dir.
    real_session_dir = (REAL_RAY_TEMP / "session_latest").resolve()
    session_name = real_session_dir.stem

    real_logs_dir = (REAL_RAY_TEMP / "session_latest" / "logs").resolve()
    test_logs_dir = TEST_RAY_TEMP / session_name / "logs"

    print(f"Copying {real_logs_dir} into {test_logs_dir}")
    shutil.copytree(src=real_logs_dir, dst=test_logs_dir, symlinks=True)

    print("Recreating the 'session_latest' symlink")
    original_cwd = Path.cwd()
    os.chdir(TEST_RAY_TEMP)
    os.symlink(
        src=session_name,
        dst="session_latest",
        target_is_directory=True,
    )
    os.chdir(original_cwd)


if __name__ == "__main__":
    main()
