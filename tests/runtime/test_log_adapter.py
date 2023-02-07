################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import subprocess
import sys
import typing as t
from unittest.mock import Mock

from orquestra.sdk._base import _log_adapter
from orquestra.sdk._ray import _dag, _ray_logs

TEST_ARGO_NODE_ID = "hello-orquestra-nk148-r000-4219834842"
TEST_ARGO_TEMPLATE = """{
    "name": "hello",
    "inputs": "",
    "outputs": ""
}"""


def test_is_argo_backend(monkeypatch):
    monkeypatch.setenv("ARGO_NODE_ID", TEST_ARGO_NODE_ID)

    assert _log_adapter.is_argo_backend()


def test_not_argo_backend():
    assert not _log_adapter.is_argo_backend()


def test_get_argo_backend_ids(monkeypatch):
    monkeypatch.setenv("ARGO_NODE_ID", TEST_ARGO_NODE_ID)
    monkeypatch.setenv("ARGO_TEMPLATE", TEST_ARGO_TEMPLATE)

    wf_run_id, task_inv_id, task_run_id = _log_adapter.get_argo_backend_ids()

    assert wf_run_id == "hello-orquestra-nk148"
    assert task_inv_id == "hello"
    assert task_run_id == TEST_ARGO_NODE_ID


def test_get_ray_backend_ids(monkeypatch):
    """
    Test boundary::
        [_dag.get_current_ids]

    The Ray underlying machinery is tested in integration tests for RayRuntime.
    """
    # Given
    wf_run_id = "wf.1"
    task_inv_id = "inv-1-generate-data"
    task_run_id = f"{wf_run_id}@{task_inv_id}"
    monkeypatch.setattr(
        _dag,
        "get_current_ids",
        Mock(return_value=(wf_run_id, task_inv_id, task_run_id)),
    )

    # When
    ids = _log_adapter.get_ray_backend_ids()

    # Then
    assert ids == (wf_run_id, task_inv_id, task_run_id)


class TestMakeLogger:
    class TestIntegration:
        """
        It's very difficult to verify how we arrange loggers and formatters because
        pytest gets into way. These tests verify the boundary of our system -
        ``_make_logger()`` in a subprocess.

        The ``_log_adapter`` module produces logs that need to be readable by
        ``_ray_logs`` module. This is why we use ``_ray_logs`` data structures for
        assertions.

        System overview::

            _make_logger() -> stdout -> _ray_logs.parse_log_line()

        """

        @staticmethod
        def _run_script(script: t.Sequence[str]) -> subprocess.CompletedProcess:
            # We wanna pass this as a single line to 'python -c my;script;lines'
            test_script_joined = ";".join(script)

            proc = subprocess.run(
                [sys.executable, "-c", test_script_joined], capture_output=True
            )

            return proc

        def test_both_ids(self):
            # Given
            test_script = [
                "from orquestra.sdk._base import _log_adapter",
                'logger = _log_adapter._make_logger(wf_run_id="wf.1", task_inv_id="inv2", task_run_id="task_run_3")',  # noqa: E501
                'logger.info("hello!")',
            ]

            # When
            proc = self._run_script(test_script)

            # Then
            # Expect logs printed to stderr
            assert proc.stdout == b""
            assert proc.stderr != b""

            lines = proc.stderr.splitlines()
            assert len(lines) == 1

            record = _ray_logs.parse_log_line(lines[0])
            assert record is not None

            # Expect timezone-aware dates
            assert record.timestamp.tzinfo is not None

            assert record.level == "INFO"
            assert record.message == "hello!"
            assert record.wf_run_id == "wf.1"
            assert record.task_inv_id == "inv2"
            assert record.task_run_id == "task_run_3"

        def test_no_ids(self):
            # Given
            test_script = [
                "from orquestra.sdk._base import _log_adapter",
                "logger = _log_adapter._make_logger(wf_run_id=None, task_inv_id=None, task_run_id=None)",  # noqa: E501
                'logger.info("hello!")',
            ]

            # When
            proc = self._run_script(test_script)

            # Then

            # Expect logs printed to stderr
            assert proc.stdout == b""
            assert proc.stderr != b""

            lines = proc.stderr.splitlines()
            assert len(lines) == 1

            record = _ray_logs.parse_log_line(lines[0])
            assert record is not None

            # Expect timezone-aware dates
            assert record.timestamp.tzinfo is not None

            assert record.level == "INFO"
            assert record.message == "hello!"
            assert record.wf_run_id is None
            assert record.task_inv_id is None
            assert record.task_run_id is None
