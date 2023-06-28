################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from orquestra.sdk._base._logs._interfaces import WorkflowLogs, WorkflowLogTypeName


class TestWorkflowLogs:
    @staticmethod
    def test_parametrized_getter():
        per_task = {"<task inv sentinel>": ["<per task sentinel>"]}
        env_setup = ["<env setup sentinel>"]
        system = ["<system sentinel>"]
        other = ["<other sentinel>"]
        logs = WorkflowLogs(
            per_task=per_task,
            env_setup=env_setup,
            system=system,
            other=other,
        )

        assert logs.get_log_type(WorkflowLogTypeName.PER_TASK) == per_task
        assert logs.get_log_type(WorkflowLogTypeName.ENV_SETUP) == env_setup
        assert logs.get_log_type(WorkflowLogTypeName.SYSTEM) == system
        assert logs.get_log_type(WorkflowLogTypeName.OTHER) == other
