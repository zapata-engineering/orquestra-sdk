################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
A script for regenerating the recorded logs in the "./ray_temp" dir. The
recorded logs are used as inputs for tests.

It requires orquestra SDK client to be installed.

This file is a developer-only utility. It automates updating the test inputs,
e.g. after moving to a different Ray version.

Prerequirements:
    * Delete ``~/.orquestra/ray`` to start with a clean state
    * ``orq up``

Warning: always review the log file contents before committing them to git. They
might contain sensitive information!
"""

import os
import shutil
from pathlib import Path

from orquestra.sdk._client._base._testing import _example_wfs  # type: ignore

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

    print("Removing log files we don't care about in tests:")
    # We're using 'glob' even for concrete paths with no need for `{}` or `*` because it
    # allows us to skip already non-existing paths.
    print(test_logs_dir)
    for delete_path in [
        *test_logs_dir.glob("events"),
        *test_logs_dir.glob("old"),
        *test_logs_dir.glob("dashboard.???"),
        *test_logs_dir.glob("dashboard_agent.log"),
        *test_logs_dir.glob("debug_state.txt"),
        *test_logs_dir.glob("debug_state_gcs.txt"),
        *test_logs_dir.glob("gcs_server.???"),
        *test_logs_dir.glob("log_monitor.???"),
        *test_logs_dir.glob("monitor.???"),
        *test_logs_dir.glob("python-core-worker-*_*.log"),
        *test_logs_dir.glob("raylet.???"),
        *test_logs_dir.glob("ray_client_server.???"),
    ]:
        print(f"- {delete_path}")
        try:
            # 1. Assume the path is a directory.
            shutil.rmtree(delete_path)
        except NotADirectoryError:
            # 2. Not a directory? It's a single file then.
            os.remove(delete_path)


if __name__ == "__main__":
    main()
