################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import sys
import typing as t
from unittest.mock import Mock

from orquestra import sdk
from orquestra.sdk._base.cli._corq._format import per_command
from orquestra.sdk._base.cli._dorq._ui import _errors, _presenters
from orquestra.sdk.schema.responses import ServiceResponse


@sdk.task
def add(a, b):
    return a + b


@sdk.workflow
def my_wf():
    return add(1, 2)


class TestWrappedCorqOutputPresenter:
    class TestPassingDataToCorq:
        """
        Tests WrappedCorqOutputPresenter's methods that delegate formatting outputs to
        the older, corq formatters.
        """

        @staticmethod
        def test_show_wf_run(monkeypatch):
            """
            Validates that we call the corq subroutine for formatting outputs. Not very
            interesting ATM. To be expanded when we get rid of corq.
            """
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)

            wf_run = my_wf().run("in_process").get_status_model()

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_wf_run(wf_run)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert wf_run in response_model.workflow_runs

        @staticmethod
        def test_show_submitted_wf_run(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)

            wf_run_id = "wf.1"

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_submitted_wf_run(wf_run_id)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert response_model.workflow_runs[0].id == wf_run_id

    class TestPrinting:
        """
        Tests WrappedCorqOutputPresenter's methods that print outputs directly.
        """

        @staticmethod
        def test_stopped_wf_run(capsys):
            # Given
            wf_run_id = "wf.1"
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_stopped_wf_run(wf_run_id)

            # Then
            captured = capsys.readouterr()
            assert f"Workflow run {wf_run_id} stopped" in captured.out

    @staticmethod
    def test_handling_error(monkeypatch):
        # Given
        status_int = 42
        status_code_enum = Mock()
        status_code_enum.value = status_int
        pretty_print_mock = Mock(return_value=status_code_enum)
        monkeypatch.setattr(_errors, "pretty_print_exception", pretty_print_mock)

        exit_mock = Mock()
        monkeypatch.setattr(sys, "exit", exit_mock)

        exception: t.Any = "<exception object sentinel>"

        presenter = _presenters.WrappedCorqOutputPresenter()

        # When
        presenter.show_error(exception)

        # Then
        # We expect passing error to pretty printer.
        pretty_print_mock.assert_called_with(exception)

        # We expect status code was passed to sys.exit()
        exit_mock.assert_called_with(status_int)


class TestServicesPresneter:
    class TestShowServices:
        def test_running(self, capsys):
            # Given
            services = [ServiceResponse(name="mocked", is_running=True, info=None)]
            presenter = _presenters.ServicePresenter()
            # When
            presenter.show_services(services)
            # Then
            captured = capsys.readouterr()
            assert "mocked  Running" in captured.out

        def test_not_running(self, capsys):
            # Given
            services = [ServiceResponse(name="mocked", is_running=False, info=None)]
            presenter = _presenters.ServicePresenter()
            # When
            presenter.show_services(services)
            # Then
            captured = capsys.readouterr()
            assert "mocked  Not Running" in captured.out

        def test_with_info(self, capsys):
            # Given
            services = [
                ServiceResponse(name="mocked", is_running=False, info="something")
            ]
            presenter = _presenters.ServicePresenter()
            # When
            presenter.show_services(services)
            # Then
            captured = capsys.readouterr()
            assert "mocked  Not Running  something" in captured.out
