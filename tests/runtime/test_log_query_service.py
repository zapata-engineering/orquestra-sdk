################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Unit tests for LogQuery service.
"""
from json import JSONDecodeError

import pytest

from orquestra.sdk._ray import _query_service


class TestLogQueryService:
    @pytest.fixture()
    def log_service(self, tmp_path):
        wf_id = "test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b"
        yield _query_service.LogQueryService(run_id=wf_id, logs_dir=tmp_path)

    @pytest.fixture()
    def create_fake_log_file(self, tmp_path):
        fake_file = tmp_path / "test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b"
        logs_content = 'test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b: [1662998041.178105000, {"log":"2022-09-12 11:54:02,693\\tINFO step_executor.py:362 -- Step status [RUNNING]\\t[test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b@invocation-0-task-do-something-useful]\\t[1/3]\\n"}]\n'  # noqa: E501
        fake_file.write_text(logs_content)
        return fake_file

    def test_if_json(self, log_service):
        data = '{"log":"Nothing"}'
        result = log_service._if_json(data)
        assert result is True

    def test_if_json_no_json(self, log_service):
        data = "Something"
        result = log_service._if_json(data)
        assert result is False

    def test_load_structured_log_record(self, log_service):
        record = '{"timestamp": "2022-09-12 12:17:14,377", "level": "WARNING", "filename": "workflow_defs.py:12", "message": {"run_id": "hello_orquestra-dbe86320-1d6f-4e88-9a80-a14a7e6b0c17@invocation-0-task-hello", "logs": "Useful work done here..."}}'  # noqa: E501
        parsed_content = log_service._load_structured_log_record(record)

        assert "2022-09-12" in parsed_content.timestamp
        assert "WARNING" in parsed_content.level
        assert "Useful work done here" in parsed_content.message.logs

    def test_load_structured_log_record_negative(self, log_service):
        with pytest.raises(JSONDecodeError):
            _ = log_service._load_structured_log_record("Nothing")

    def test_load_ustructured_log_record(self, log_service, create_fake_log_file):
        record = 'test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b: [1662998041.178105000, {"log":"2022-09-12 11:54:02,693\tINFO step_executor.py:362 -- Step status [RUNNING]\t[test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b@invocation-0-task-do-something-useful]\t[1/3]\\n"}]\n'  # noqa: E501
        parsed_content = log_service._load_unstructured_log_record(record)

        assert "2022-09-12" in parsed_content.timestamp
        assert "INFO" in parsed_content.level
        assert "RUNNING" in parsed_content.message.logs

    def test_load_ustructured_log_record_not_full_record(
        self, log_service, create_fake_log_file
    ):
        parsed_content = log_service._load_unstructured_log_record("Nothing")

        assert parsed_content is None

    def test_get_full_structured_from_ray_unstructured_logs(
        self, log_service, create_fake_log_file
    ):
        parsed_content = log_service.get_full_structured_logs()
        assert "2022-09-12" in parsed_content[0].timestamp
        assert "INFO" in parsed_content[0].level
        assert "RUNNING" in parsed_content[0].message.logs

    def test_get_message_logs(self, log_service, create_fake_log_file):
        log_content = log_service.get_message_logs()
        assert "Step status [RUNNING]" in log_content[0]
        assert "test_wf-6aa1058c-a875-4ab1-9863-97c4d0b6013b" in log_content[0]


class TestLogQueryServiceHistorical:
    @pytest.fixture()
    def log_service(self, tmp_path):
        yield _query_service.LogQueryService(run_id="", logs_dir=tmp_path)

    @pytest.fixture()
    def create_fake_log_file(self, tmp_path):
        fake_file = tmp_path / "ray.worker"
        fake_file.write_text(
            """ray.worker: [1662974234.095385413, {"log":":actor_name:WorkflowManagementActor"}]
ray.worker: [1662974234.095385413, {"log":"2022-09-12 12:17:13,152\\tINFO workflow_access.py:193 -- run_or_resume: hello_orquestra-dbe86320-1d6f-4e88-9a80-a14a7e6b0c17, orquestra.sdk._ray._ir_runtime._aggregate_outputs,ObjectRef(7a9e42f4c44b8d41ffffffffffffffffffffffff0100000001000000)"}]
ray.worker: [1662974234.095385413, {"log":"2022-09-12 12:17:13,153\\tINFO workflow_access.py:204 -- Workflow job [id=hello_orquestra-dbe86320-1d6f-4e88-9a80-a14a7e6b0c17] started."}]
ray.worker: [1662974234.096308913, {"log":":actor_name:WorkflowManagementActor"}]
ray.worker: [1662974234.098013496, {"log":":task_name:_resume_workflow_step_executor"}]
ray.worker: [1662974234.098541288, {"log":":task_name:_resume_workflow_step_executor"}]
ray.worker: [1662974234.100378121, {"log":":task_name:_workflow_step_executor_remote"}]
ray.worker: [1662974234.100378121, {"log":"2022-09-12 12:17:14,377\\tINFO step_executor.py:362 -- Step status [RUNNING] [hello_orquestra-dbe86320-1d6f-4e88-9a80-a14a7e6b0c17@invocation-0-task-hello] [1/3]"}]
ray.worker: [1662974234.100378121, {"log":"{\\"timestamp\\": \\"2022-09-12 12:17:14,377\\", \\"level\\": \\"WARNING\\", \\"filename\\": \\"workflow_defs.py:12\\", \\"message\\": {\\"run_id\\": \\"hello_orquestra-dbe86320-1d6f-4e88-9a80-a14a7e6b0c17@invocation-0-task-hello\\", \\"logs\\": \\"Useful work done here...\\"}}"}]"""  # noqa: E501
        )
        return fake_file

    def test_get_full_structured_logs(self, log_service, create_fake_log_file):
        parsed_content = log_service.get_full_structured_logs()
        assert len(parsed_content) == 4

    def test_get_message_logs(self, log_service, create_fake_log_file):
        log_content = log_service.get_message_logs()
        assert len(log_content) == 4
        assert "Useful work done here..." in log_content[3]
