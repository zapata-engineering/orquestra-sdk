################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for ``orquestra.sdk._base._logs._regrouping``.
"""

from pathlib import Path

import pytest

from orquestra.sdk._base._logs import _regrouping


class TestIsWorker:
    @staticmethod
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param(
                Path(
                    "/tmp/ray/session_latest/logs/worker-"
                    "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-"
                    "01000000-249.err"
                ),
                id="ce-path-latest",
            ),
            pytest.param(
                Path(
                    "session_latest/logs/worker-"
                    "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-"
                    "01000000-249.err"
                ),
                id="ray-tmp-relative-latest",
            ),
            pytest.param(
                Path(
                    "session_2023-06-12_14-46-40_833957_38917/logs/worker-"
                    "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-"
                    "01000000-249.err"
                ),
                id="ray-tmp-relative-timestamp",
            ),
            pytest.param(
                Path(
                    "session_latest/logs/worker-"
                    "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-"
                    "01000000-249.err"
                ),
                id="ray-tmp-relative-latest",
            ),
            pytest.param(
                Path(
                    "/Users/emiliano/.orquestra/ray/session_latest/logs/worker-"
                    "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-"
                    "01000000-249.err"
                ),
                id="local-absolute",
            ),
        ],
    )
    def test_valid(path: Path):
        assert _regrouping.is_worker(path=path) is True

    @staticmethod
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param(
                Path(
                    "session_2023-06-12_14-46-40_833957_38917/logs/"
                    "runtime_env_setup-01000000.log"
                ),
                id="env-setup",
            ),
        ],
    )
    def test_invalid(path: Path):
        assert _regrouping.is_worker(path=path) is False


class TestIsEnvSetup:
    @staticmethod
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param(
                Path(
                    "session_2023-06-12_14-46-40_833957_38917/logs/"
                    "runtime_env_setup-01000000.log"
                ),
                id="relative-timestamp",
            ),
            pytest.param(
                Path("session_latest/logs/runtime_env_setup-01000000.log"),
                id="relative-latest",
            ),
            pytest.param(
                Path("/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"),
                id="absolute-ce",
            ),
            pytest.param(
                Path(
                    "/Users/emiliano/.orquestra/ray/"
                    "session_2023-06-12_14-46-40_833957_38917/logs/"
                    "runtime_env_setup-01000000.log"
                ),
                id="absolute-local-timestamp",
            ),
            pytest.param(
                Path(
                    "/Users/emiliano/.orquestra/ray/session_latest/logs/"
                    "runtime_env_setup-01000000.log"
                ),
                id="absolute-local-latest",
            ),
        ],
    )
    def test_valid(path: Path):
        assert _regrouping.is_env_setup(path=path) is True

    @staticmethod
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param(
                Path(
                    "/tmp/ray/session_latest/logs/worker-"
                    "903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-"
                    "01000000-249.err"
                ),
                id="worker",
            ),
        ],
    )
    def test_invalid(path: Path):
        assert _regrouping.is_env_setup(path=path) is False
