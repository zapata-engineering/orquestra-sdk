################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for orquestra.sdk._base._services.
"""

import builtins
import subprocess
import sys
import typing as t
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock

import pytest
import python_on_whales

from orquestra.sdk._base import _services

from ..reloaders import restore_loaded_modules


def _fake_run_for_ray(
    ray_start_retcode: t.Optional[int] = None,
    ray_status_retcode: t.Optional[int] = None,
    ray_stop_retcode: t.Optional[int] = None,
) -> t.Callable:
    """
    Prepares a mock for ``subprocess.run``.

    If a given ``*_retcode`` arg is not ``None``, and the corresponding Ray command is
    called, the result will mimic getting the subprocess retcode.

    If a given ``*_retcode`` arg is ``None``, and the corresponding Ray command is
    called, an ``AssertionError`` will be raised.
    """

    def _fake_run(cmd, *args, **kwargs):
        for cmd_head, retcode_spec in [
            (["ray", "start"], ray_start_retcode),
            (["ray", "status"], ray_status_retcode),
            (["ray", "stop"], ray_stop_retcode),
        ]:
            if cmd[:2] == cmd_head:
                if (retcode := retcode_spec) is not None:
                    proc_mock = Mock()
                    proc_mock.returncode = retcode
                    return proc_mock
                else:
                    pytest.fail()

    return _fake_run


def _mock_subprocess_for_ray(
    monkeypatch,
    ray_start_retcode: t.Optional[int] = None,
    ray_status_retcode: t.Optional[int] = None,
    ray_stop_retcode: t.Optional[int] = None,
):
    """
    See docstring for ``_fake_run_for_ray``.
    """
    mock_subprocess = Mock()
    mock_subprocess.run = _fake_run_for_ray(
        ray_start_retcode=ray_start_retcode,
        ray_status_retcode=ray_status_retcode,
        ray_stop_retcode=ray_stop_retcode,
    )
    monkeypatch.setattr(
        _services,
        "subprocess",
        mock_subprocess,
    )


@pytest.fixture
def monkeypatch_import(monkeypatch):
    original_import = builtins.__import__

    def _import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in ("python_on_whales"):
            raise ModuleNotFoundError(f"Mocked module not found {name}")
        return original_import(
            name, globals=globals, locals=locals, fromlist=fromlist, level=level
        )

    monkeypatch.setattr(builtins, "__import__", _import)


@contextmanager
def unload_module(module_name: str):
    for module in tuple(sys.modules.keys()):
        if module.startswith(module_name):
            del sys.modules[module]

    with restore_loaded_modules():
        yield


class TestFluentManager:
    """
    Tests' boundaries::

        [FluentManager]─────[python_on_whales]
    """

    def test_name(self):
        # Given
        sm = _services.FluentManager()

        # When
        name = sm.name

        # Then
        assert name == "Fluent Bit"

    class TestDockerClientWrapper:
        def test_no_python_on_whales_no_client(self, monkeypatch_import):
            # Given
            with unload_module("python_on_whales"):
                # When
                sm = _services.FluentManager()
            # Then
            assert sm._docker_client._client is None

        @pytest.mark.parametrize(
            "action_to_test",
            [
                "up",
                "down",
            ],
        )
        def test_no_python_on_whales_raises(self, monkeypatch_import, action_to_test):
            # Given
            with unload_module("python_on_whales"):
                sm = _services.FluentManager()
            action = getattr(sm, action_to_test)

            # When
            with pytest.raises(ModuleNotFoundError) as exc_info:
                action()

            # Then
            assert exc_info.exconly() == (
                "ModuleNotFoundError: In order to use Docker based logging, please "
                "make sure you install the optional dependencies with:\n`pip install "
                "orquestra-sdk[all]`"
            )

        def test_no_python_on_whales_is_fluent_running(self, monkeypatch_import):
            # Given
            with unload_module("python_on_whales"):
                sm = _services.FluentManager()

            # When
            running = sm.is_running()

            # Then
            assert not running

        def test_with_python_on_whales_has_client(self):
            # Given
            with unload_module("python_on_whales"):
                # When
                sm = _services.FluentManager()
            # Then
            assert sm._docker_client._client is not None

    class TestUp:
        def test_fluentbit_not_running(self):
            # Given
            sm = _services.FluentManager()
            # Prevent calling real docker engine.
            client_mock = Mock()
            sm._docker_client = client_mock

            # When
            sm.up()

            # Then
            # [Expect no exception]
            client_mock.build.assert_called()
            client_mock.up.assert_called()

        def test_fluentbit_already_running(self):
            # Given
            sm = _services.FluentManager()
            # Prevent calling real docker engine.
            client_mock = Mock()
            sm._docker_client = client_mock

            # When
            sm.up()

            # Then
            # [Expect no exception]
            client_mock.build.assert_called()
            client_mock.up.assert_called()

        def test_no_docker_daemon(self):
            # Given
            sm = _services.FluentManager()
            # Simulate python_on_whales' behavior when docker daemon isn't running.
            client_mock = Mock()
            client_mock.up.side_effect = _services.DockerException("Mocked out")
            sm._docker_client = client_mock

            with pytest.raises(RuntimeError):
                # When
                sm.up()

    class TestDown:
        def test_no_docker_daemon(self):
            # Given
            sm = _services.FluentManager()
            # Simulate python_on_whales' behavior when docker daemon isn't running.
            client_mock = Mock()
            client_mock.down.side_effect = _services.DockerException("Mocked out")
            sm._docker_client = client_mock

            # When
            sm.down()

            # Then
            client_mock.down.assert_called()

        def test_fluentbit_not_running(self):
            # Given
            sm = _services.FluentManager()
            # Prevent calling real docker engine.
            client_mock = Mock()
            sm._docker_client = client_mock

            # When
            sm.down()

            # Then
            client_mock.down.assert_called()

        def test_fluentbit_already_running(self):
            # Given
            sm = _services.FluentManager()
            # Prevent calling real docker engine.
            client_mock = Mock()
            sm._docker_client = client_mock

            # When
            sm.down()

            # Then
            client_mock.down.assert_called()

    class TestIsRunning:
        def test_fluentbit_not_running(self):
            # Given
            sm = _services.FluentManager()
            client_mock = Mock()
            client_mock.ps.return_value = []
            sm._docker_client = client_mock

            # When
            running = sm.is_running()

            # Then
            assert not running

        def test_fluentbit_services_running(self):
            # Given
            sm = _services.FluentManager()
            container1 = Mock()
            container1.state.running = True
            container1.name = _services.FLUENTBIT_CONTAINER_NAME

            container2 = Mock()
            container2.state.running = True
            container2.name = "some_other_container_name"

            client_mock = Mock()
            client_mock.ps.return_value = [container1, container2]
            sm._docker_client = client_mock

            # When
            running = sm.is_running()

            # Then
            assert running

        def test_no_docker_daemon(self):
            # Given
            # Simulate python_on_whales' behavior when docker daemon isn't running.
            client_mock = Mock()
            client_mock.ps.side_effect = _services.DockerException("Mocked out")
            sm = _services.FluentManager()
            sm._docker_client = client_mock

            # When
            running = sm.is_running()

            # Then
            assert not running


class TestRayManager:
    """
    Tests' boundaries::

        [RayManager]─────[subprocess]
    """

    def test_name(self):
        # Given
        sm = _services.RayManager()

        # When
        name = sm.name

        # Then
        assert name == "Ray"

    class TestUp:
        def test_ray_not_running(self, monkeypatch):
            # Given
            sm = _services.RayManager()
            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(
                monkeypatch,
                ray_start_retcode=0,
                ray_status_retcode=0,
            )

            # When
            sm.up()

            # Then
            # [Expect no exception]

        def test_ray_already_running(self, monkeypatch):
            # Given
            sm = _services.RayManager()
            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(
                monkeypatch,
                ray_start_retcode=1,
                ray_status_retcode=0,
            )

            # When
            sm.up()

            # Then
            # [Expect no exception]

        def test_ray_failed_to_start(self, monkeypatch):
            # Given
            sm = _services.RayManager()
            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(
                monkeypatch,
                ray_start_retcode=0,
                ray_status_retcode=0,
            )
            monkeypatch.setattr(sm, "is_running", lambda: False)

            # When
            with pytest.raises(RuntimeError) as exc_info:
                sm.up()

            # Then
            assert "Couldn't start Ray" in str(exc_info)

    class TestDown:
        def test_no_services_running(self, monkeypatch):
            # Given
            sm = _services.RayManager()

            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(monkeypatch, ray_stop_retcode=0)

            # When
            sm.down()

            # Then
            # [Expect no exception]

        def test_ray_already_running(self, monkeypatch):
            # Given
            sm = _services.RayManager()

            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(monkeypatch, ray_stop_retcode=0)

            # When
            sm.down()

            # Then
            # [Expect no exception]

    class TestIsRunning:
        def test_no_services_running(self, monkeypatch):
            # Given
            sm = _services.RayManager()

            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(monkeypatch, ray_status_retcode=1)

            # When
            is_running = sm.is_running()

            # Then
            assert not is_running

        def test_ray_already_running(self, monkeypatch):
            # Given
            sm = _services.RayManager()

            # Mimic 'ray' CLI behavior.
            _mock_subprocess_for_ray(monkeypatch, ray_status_retcode=0)

            # When
            is_running = sm.is_running()

            # Then
            assert is_running


class TestServiceManager:
    """
    Tests' boundaries::

        [ServiceManager]───┬─[FluentManager]
                           └─[RayManager]
    """

    class TestUp:
        def test_happy_path(self):
            # Given
            fluent = Mock()
            ray = Mock()
            sm = _services.ServiceManager(ray, fluent)
            # When
            sm.up()
            # Then
            fluent.up.assert_called()
            ray.up.assert_called()

        def test_fluent_fails(self):
            # Given
            fluent = Mock()
            fluent.up.side_effect = RuntimeError
            ray = Mock()
            sm = _services.ServiceManager(ray, fluent)
            # When
            with pytest.raises(RuntimeError):
                sm.up()
            # Then
            fluent.up.assert_called()
            ray.up.assert_not_called()

        def test_ray_fails(self):
            # Given
            fluent = Mock()
            ray = Mock()
            ray.up.side_effect = RuntimeError
            sm = _services.ServiceManager(ray, fluent)
            # When
            with pytest.raises(RuntimeError):
                sm.up()
            # Then
            fluent.up.assert_called()
            ray.up.assert_called()

        def test_ray_subprocess_fails(self):
            # Given
            fluent = Mock()
            ray = Mock()
            ray.up.side_effect = subprocess.CalledProcessError(1, "mocked-out")
            sm = _services.ServiceManager(ray, fluent)
            # When
            with pytest.raises(subprocess.CalledProcessError):
                sm.up()
            # Then
            fluent.up.assert_called()
            ray.up.assert_called()

    class TestDown:
        def test_happy_path(self):
            # Given
            fluent = Mock()
            ray = Mock()
            sm = _services.ServiceManager(ray, fluent)
            # When
            sm.down()
            # Then
            fluent.down.assert_called()
            ray.down.assert_called()

        def test_ray_subprocess_fails(self):
            pass
            # Given
            fluent = Mock()
            ray = Mock()
            ray.down.side_effect = subprocess.CalledProcessError(1, "mocked-out")
            sm = _services.ServiceManager(ray, fluent)
            # When
            with pytest.raises(subprocess.CalledProcessError):
                sm.down()
            # Then
            fluent.down.assert_called()
            ray.down.assert_called()

    class TestIsFluentRunning:
        @pytest.mark.parametrize("result", [True, False])
        def test_happy_path(self, result):
            # Given
            fluent = Mock()
            fluent.is_running.return_value = result
            ray = Mock()
            sm = _services.ServiceManager(ray, fluent)
            # When
            running = sm.is_fluentbit_running()
            # Then
            fluent.is_running.assert_called()
            ray.is_running.assert_not_called()
            assert running == result

    class TestIsRayRunning:
        @pytest.mark.parametrize("result", [True, False])
        def test_happy_path(self, result):
            # Given
            fluent = Mock()
            ray = Mock()
            ray.is_running.return_value = result
            sm = _services.ServiceManager(ray, fluent)
            # When
            running = sm.is_ray_running()
            # Then
            fluent.is_running.assert_not_called()
            ray.is_running.assert_called()
            assert running == result

        def test_ray_subprocess_fails(self):
            # Given
            fluent = Mock()
            ray = Mock()
            ray.is_running.side_effect = subprocess.CalledProcessError(1, "mocked-out")
            sm = _services.ServiceManager(ray, fluent)
            # When
            with pytest.raises(subprocess.CalledProcessError):
                sm.is_ray_running()
            # Then
            ray.is_running.assert_called()


class TestRayLocation:
    class TestRayTemp:
        def test_with_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
            # Given
            monkeypatch.setenv("ORQ_RAY_TEMP", str(tmp_path))

            # When
            location = _services.ray_temp_path()

            # Then
            assert location == tmp_path

        def test_without_env(self):
            # Given
            # Nothing

            # When
            location = _services.ray_temp_path()

            # Then
            assert location == Path.home() / ".orquestra" / "ray"

    class TestRayStorage:
        def test_with_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
            # Given
            monkeypatch.setenv("ORQ_RAY_STORAGE", str(tmp_path))

            # When
            location = _services.ray_storage_path()

            # Then
            assert location == tmp_path

        def test_without_env(self):
            # Given
            # Nothing

            # When
            location = _services.ray_storage_path()

            # Then
            assert location == Path.home() / ".orquestra" / "ray_storage"
