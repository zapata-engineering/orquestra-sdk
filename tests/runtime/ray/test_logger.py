import pytest

from orquestra.sdk._ray import _dag


@pytest.mark.slow
class TestRayLogger:
    def test_ray_logs_silenced(self, capsys: pytest.CaptureFixture):
        # Given
        params = _dag.RayParams(configure_logging=False)
        # Ensure Ray is down
        _dag.RayRuntime.shutdown()

        # When
        _ = _dag.RayRuntime.startup(params)
        # Then
        capture = capsys.readouterr()
        # Seen when connecting to an existing cluster
        assert "Connecting to existing Ray cluster at address" not in capture.err
        # Seen when the Ray workflows is starting for the first time
        assert "Initializing workflow manager" not in capture.err

    def test_ray_logs_not_silenced(self, capsys: pytest.CaptureFixture):
        # Given
        params = _dag.RayParams(configure_logging=True)
        # Ensure Ray is down
        _dag.RayRuntime.shutdown()

        # When
        _ = _dag.RayRuntime.startup(params)
        # Then
        capture = capsys.readouterr()
        # Seen when connecting to an existing cluster
        assert "Started a local Ray instance" in capture.err
        # Seen when the Ray workflows is starting for the first time
        assert "Initializing workflow manager" in capture.err
