################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
"""
We've had problems where lazy loading module attributes made some of the import
schemes not work for the users. This file tries a couple of ways to import the
'secrets' module to test it automatically.
"""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "code_lines",
    [
        [
            "import orquestra.sdk",
            "print(orquestra.sdk.secrets)",
        ],
        [
            "import orquestra.sdk",
            "import orquestra.sdk.secrets",
            "print(orquestra.sdk.secrets)",
        ],
        [
            "from orquestra import sdk",
            "print(sdk.secrets)",
        ],
        [
            "from orquestra import sdk",
            "import orquestra.sdk.secrets",
            "print(sdk.secrets)",
            "print(orquestra.sdk.secrets)",
        ],
        [
            "import orquestra.sdk.secrets as secrets",
            "print(secrets)",
        ],
    ],
)
def test_running_import(code_lines):
    # Given
    code = "; ".join(code_lines)

    # When
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
        ]
    )

    # Then
    # If the code causes import error, the python subprocess would return a non-zero
    # exit code.
    proc.check_returncode()
