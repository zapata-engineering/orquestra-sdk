################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from unittest.mock import create_autospec, sentinel

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base._api.repos import WFDefRepo
from orquestra.sdk._base.abc import RuntimeInterface


class TestWFDefRepo:
    class TestGetWFDef:
        @staticmethod
        @pytest.fixture
        def repo():
            return WFDefRepo()

        @staticmethod
        @pytest.fixture
        def runtime_mock():
            return create_autospec(RuntimeInterface)

        @staticmethod
        def test_passing_values(repo: WFDefRepo, runtime_mock):
            # Given
            wf_run_id = sentinel.wf_run_id
            wf_def = sentinel.wf_def
            runtime_mock.get_workflow_run_status(wf_run_id).workflow_def = wf_def

            # When
            result = repo.get_wf_def(wf_run_id=wf_run_id, runtime=runtime_mock)

            # Then
            assert result == wf_def

        @staticmethod
        @pytest.mark.parametrize(
            "exception",
            [
                exceptions.UnauthorizedError(),
                exceptions.WorkflowRunNotFoundError(),
            ],
        )
        def test_exceptions(repo: WFDefRepo, runtime_mock, exception):
            # Given
            runtime_mock.get_workflow_run_status.side_effect = exception

            # Then
            with pytest.raises(type(exception)):
                # When
                repo.get_wf_def(wf_run_id=sentinel.wf_run_id, runtime=runtime_mock)
