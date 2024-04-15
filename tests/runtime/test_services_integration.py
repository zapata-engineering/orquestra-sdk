################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import subprocess

import pytest

from orquestra.sdk._client._base._services import RayManager
from orquestra.sdk._client._base._testing._connections import ray_suitable_temp_dir


@pytest.mark.slow
class TestRayCLI:
    @staticmethod
    def test_up_down(monkeypatch):
        with ray_suitable_temp_dir() as tmp_path:
            orq_dir = tmp_path / ".orquestra"
            monkeypatch.setenv("ORQ_RAY_TEMP_PATH", str(orq_dir / "ray"))
            monkeypatch.setenv("ORQ_RAY_STORAGE_PATH", str(orq_dir / "ray_storage"))
            monkeypatch.setenv("ORQ_RAY_PLASMA_PATH", str(orq_dir / "ray_plasma"))

            ray = RayManager()

            ray.up()
            assert ray.is_running()
            ray.down()
            assert not ray.is_running()

    @staticmethod
    def test_failure(monkeypatch: pytest.MonkeyPatch):
        # Given
        with ray_suitable_temp_dir() as tmp_path:
            tmp_path.mkdir(exist_ok=True)
            monkeypatch.chdir(tmp_path)
            # As of Ray 2.3.1 setting paths to "." is a reliable way to make 'ray start'
            # fail.
            # Since Ray 2.6 temp path has to be absolute path, not relative
            monkeypatch.setenv("ORQ_RAY_TEMP_PATH", str(tmp_path))
            monkeypatch.setenv("ORQ_RAY_STORAGE_PATH", ".")
            monkeypatch.setenv("ORQ_RAY_PLASMA_PATH", ".")

            ray = RayManager()

            with pytest.raises(subprocess.CalledProcessError) as exc_info:
                # When
                ray.up()

            # Then
            assert b"Local node IP" in exc_info.value.stdout
            assert b"URI has empty scheme" in exc_info.value.stderr
