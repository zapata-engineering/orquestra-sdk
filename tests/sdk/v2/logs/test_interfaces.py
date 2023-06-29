################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import pytest

from orquestra.sdk._base._logs._interfaces import WorkflowLogs


class TestWorkflowLogs:
    @pytest.fixture
    def logs(self):
        per_task = {"<task inv sentinel>": ["<per task sentinel>"]}
        env_setup = ["<env setup sentinel>"]
        system = ["<system sentinel>"]
        other = ["<other sentinel>"]
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
