################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import json
import subprocess
import sys

from ray.workflow import workflow_context

from orquestra.sdk._base import _log_adapter
from orquestra.sdk._ray import _ray_logs

TEST_ARGO_NODE_ID = "hello-orquestra-nk148-r000-4219834842"
TEST_ARGO_TEMPLATE = """{
    "name": "hello",
    "inputs": "",
    "outputs": ""
}"""
TEST_WF_RUN_ID = "hello-orquestra-nk148"
TEST_TASK_RUN_ID = "hello-orquestra-nk148@some-task-id-xyz"


def test_is_argo_backend(monkeypatch):
    monkeypatch.setenv("ARGO_NODE_ID", TEST_ARGO_NODE_ID)

    assert _log_adapter.is_argo_backend()


def test_not_argo_backend():
    assert not _log_adapter.is_argo_backend()


def test_get_argo_backend_ids(monkeypatch):
    monkeypatch.setenv("ARGO_NODE_ID", TEST_ARGO_NODE_ID)

    workflow_id, task_id = _log_adapter.get_argo_backend_ids()
    assert workflow_id == TEST_WF_RUN_ID
    assert task_id == TEST_ARGO_NODE_ID


def test_get_argo_step_name(monkeypatch):
    monkeypatch.setenv("ARGO_TEMPLATE", TEST_ARGO_TEMPLATE)
    assert _log_adapter.get_argo_step_name() == "hello"


def test_get_ray_backend_ids():
    with workflow_context.workflow_task_context(
        context=workflow_context.WorkflowTaskContext(
            workflow_id=TEST_WF_RUN_ID,
            task_id=TEST_TASK_RUN_ID,
        )
    ):
        workflow_id, task_id = _log_adapter.get_ray_backend_ids()
        assert workflow_id == TEST_WF_RUN_ID
        assert task_id == TEST_TASK_RUN_ID


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
        def _run_script(script: str) -> subprocess.CompletedProcess:
            # We wanna pass this as a single line to 'python -c my;script;lines'
            test_script_joined = ";".join(script.splitlines())

            proc = subprocess.run(
                [sys.executable, "-c", test_script_joined], capture_output=True
            )

            return proc

        def test_both_ids(self):
            # Given
            test_script = """from orquestra.sdk._base import _log_adapter
logger = _log_adapter._make_logger(wf_run_id="wf.1", task_run_id="task_run_2")
logger.info("hello!")"""

            # When
            proc = self._run_script(test_script)

            # Then
            # Expect logs printed to stderr
            assert proc.stdout == b""
            assert proc.stderr != b""

            lines = proc.stderr.splitlines()
            assert len(lines) == 1

            record = _ray_logs.parse_log_line(lines[0], searched_id=None)
            assert record is not None

            # Expect timezone-aware dates
            assert record.timestamp.tzinfo is not None

            assert record.level == "INFO"
            assert record.message == "hello!"
            assert record.wf_run_id == "wf.1"
            assert record.task_run_id == "task_run_2"

        def test_no_ids(self):
            # Given
            test_script = """from orquestra.sdk._base import _log_adapter
                logger = _log_adapter._make_logger(wf_run_id=None, task_run_id=None)
                logger.info("hello!")"""

            # When
            proc = self._run_script(test_script)

            # Then

            # Expect logs printed to stderr
            assert proc.stdout == b""
            assert proc.stderr != b""

            lines = proc.stderr.splitlines()
            assert len(lines) == 1

            record = _ray_logs.parse_log_line(lines[0], searched_id=None)
            assert record is not None

            # Expect timezone-aware dates
            assert record.timestamp.tzinfo is not None

            assert record.level == "INFO"
            assert record.message == "hello!"
            assert record.wf_run_id is None
            assert record.task_run_id is None
