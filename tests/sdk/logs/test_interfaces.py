################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pytest

from orquestra.sdk._client._base._logs._interfaces import LogOutput, WorkflowLogs


def _logs_output(name: str):
    return LogOutput(
        out=[f"<{name} stdout sentinel>"], err=[f"<{name} stderr sentinel>"]
    )


class TestWorkflowLogs:
    @pytest.fixture
    def logs(self):
        per_task = {"<task inv sentinel>": _logs_output("per task")}
        env_setup = _logs_output("env setup")
        system = _logs_output("system")
        other = _logs_output("other")
        return WorkflowLogs(
            per_task=per_task,
            env_setup=env_setup,
            system=system,
            other=other,
        )

    @staticmethod
    def test_parametrized_getter(logs):
        assert (
            logs.get_log_type(WorkflowLogs.WorkflowLogTypeName.PER_TASK)
            == logs.per_task
        )
        assert (
            logs.get_log_type(WorkflowLogs.WorkflowLogTypeName.ENV_SETUP)
            == logs.env_setup
        )
        assert logs.get_log_type(WorkflowLogs.WorkflowLogTypeName.SYSTEM) == logs.system
        assert logs.get_log_type(WorkflowLogs.WorkflowLogTypeName.OTHER) == logs.other

    @staticmethod
    def test_raises_valueerror_for_unrecognised_name(logs):
        with pytest.raises(ValueError) as e:
            _ = logs.get_log_type("floop")
        assert e.exconly() == "ValueError: Unknown workflow log type 'floop'."
