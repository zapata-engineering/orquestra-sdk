################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import subprocess
import sys
from pathlib import Path


class TestRayLogger:
    def test_ray_logs_silenced(self, tmp_path: Path):
        # Given
        test_case = """
from orquestra.workflow_runtime._ray import _dag
params = _dag.RayParams(configure_logging=False)
_ = _dag.RayRuntime.startup(params)
        """
        logger = tmp_path / "logger.py"
        logger.write_text(test_case)
        # When
        result = subprocess.run([sys.executable, logger], capture_output=True)
        # Then
        result.check_returncode()
        stderr = result.stderr.decode()
        # Seen when connecting to an existing cluster
        assert "Started a local Ray instance" not in stderr
        # Seen when the Ray workflows is starting for the first time
        assert "Initializing workflow manager" not in stderr

    def test_ray_logs_not_silenced(self, tmp_path: Path):
        # Given
        test_case = """
from orquestra.workflow_runtime._ray import _dag
params = _dag.RayParams(configure_logging=True)
_ = _dag.RayRuntime.startup(params)
        """
        logger = tmp_path / "logger.py"
        logger.write_text(test_case)
        # When
        result = subprocess.run([sys.executable, logger], capture_output=True)

        # Then
        result.check_returncode()
        stderr = result.stderr.decode()
        assert "Started a local Ray instance" in stderr
        # Seen when the Ray workflows is starting for the first time
        assert "Initializing workflow manager" in stderr
