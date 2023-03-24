################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf list' glue code.
"""
import datetime
import typing as t
from unittest.mock import Mock

import pytest

from orquestra.sdk._base.cli._dorq._workflow import _list
from orquestra.sdk.schema.workflow_run import RunStatus, State


class TestAction:
    """
    Test boundaries::
        [_list.Action]->[arg resolvers]
                      ->[repos]
                      ->[presenter]
    """

    @staticmethod
    @pytest.mark.parametrize("interactive", [True, False])
    def test_data_passing(interactive, capsys):
        """
        Verifies how we pass variables between subcomponents.
        """
        # Given

        def return_wf():
            run = Mock()
            run.id = "fake id"
            run.status = RunStatus(
                state=State.RUNNING,
                start_time=datetime.datetime.fromtimestamp(0),
                end_time=datetime.datetime.fromtimestamp(0),
            )
            run.task_runs = []
            return run

        # CLI inputs
        config = ["<config sentinel>"]
        limit: t.Any = "<limit sentinel>"
        max_age = "<max_age sentinel>"
        state: t.Any = "<state sentinel>"

        # Resolved values
        resolved_configs = ["<resolved config 1>", "<resolved config 2>"]
        resolved_limit = "<resolved_limit>"
        resolved_max_age = "<resolved_max_age>"
        resolved_state = "<resolved_state>"

        # Mocks
        presenter = Mock()

        wf_run_repo = Mock()

        wf_runs = [return_wf(), return_wf()]
        wf_run_repo.list_wf_runs.return_value = wf_runs

        config_resolver = Mock()
        config_resolver.resolve_multiple.return_value = resolved_configs

        summary_repo = Mock()
        showed_mocks = [Mock(), Mock()]
        summary_repo.wf_list_summary.return_value = showed_mocks

        wf_run_filter_resolver = Mock()
        wf_run_filter_resolver.resolve_limit.return_value = resolved_limit
        wf_run_filter_resolver.resolve_max_age.return_value = resolved_max_age
        wf_run_filter_resolver.resolve_state.return_value = resolved_state

        action = _list.Action(
            presenter=presenter,
            summary_repo=summary_repo,
            wf_run_repo=wf_run_repo,
            config_resolver=config_resolver,
            wf_run_filter_resolver=wf_run_filter_resolver,
        )

        # When
        action.on_cmd_call(
            config=config,
            limit=limit,
            max_age=max_age,
            state=state,
            interactive=interactive,
        )

        # Then
        # We should pass input config args to config resolver.
        config_resolver.resolve_multiple.assert_called_with(config)

        # We should pass input filter args to the filter resolver.
        wf_run_filter_resolver.resolve_limit.assert_called_with(
            limit, interactive=interactive
        )
        wf_run_filter_resolver.resolve_max_age.assert_called_with(
            max_age, interactive=interactive
        )
        wf_run_filter_resolver.resolve_state.assert_called_with(
            state, interactive=interactive
        )

        # We should pass resolved values to run repo.
        for resolved_config in resolved_configs:
            wf_run_repo.list_wf_runs.assert_any_call(
                resolved_config,
                limit=resolved_limit,
                max_age=resolved_max_age,
                state=resolved_state,
            )

        # We expect printing of the workflow runs returned from the repo.
        expected_wf_runs_list = wf_runs + wf_runs
        summary_repo.wf_list_summary.assert_called_with(expected_wf_runs_list)
        presenter.show_wf_list.assert_called_with(showed_mocks)

        # This specifies all of the filters, so we shouldn't get anything flagging up
        # to the user
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""
