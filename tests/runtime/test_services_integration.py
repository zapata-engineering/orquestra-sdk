################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk._base._services import RayManager
from orquestra.sdk._base._testing._connections import ray_suitable_temp_dir


@pytest.mark.slow
@pytest.mark.skip("PATH issues on CI: ORQSDK-771")
def test_ray_roundtrip(monkeypatch: pytest.MonkeyPatch):
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
