################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import sys
import typing as t
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra import sdk
from orquestra.sdk._base import serde
from orquestra.sdk._base.cli._corq._format import per_command
from orquestra.sdk._base.cli._dorq._ui import _errors, _presenters
from orquestra.sdk.schema.ir import ArtifactFormat
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

        class TestDumpedWFResult:
            @staticmethod
            def test_json(capsys):
                # Given
                details = serde.DumpDetails(
                    file_path=Path("tests/some-path/wf.1234_1.json"),
                    format=ArtifactFormat.JSON,
                )
                presenter = _presenters.WrappedCorqOutputPresenter()

                # When
                presenter.show_dumped_wf_result(details)

                # Then
                captured = capsys.readouterr()
                # We can't assert on the full path because separators are
                # platform-dependent.
                assert "Artifact saved at tests" in captured.out
                assert "wf.1234_1.json as a text json file." in captured.out

            @staticmethod
            def test_pickle(capsys):
                # Given
                details = serde.DumpDetails(
                    file_path=Path("tests/some-path/wf.1234_1.pickle"),
                    format=ArtifactFormat.ENCODED_PICKLE,
                )
                presenter = _presenters.WrappedCorqOutputPresenter()

                # When
                presenter.show_dumped_wf_result(details)

                # Then
                captured = capsys.readouterr()
                # We can't assert on the full path because separators are
                # platform-dependent.
                assert "Artifact saved at tests" in captured.out
                assert "wf.1234_1.pickle as a binary pickle file." in captured.out

            @staticmethod
            def test_other(capsys):
                # Given
                details = serde.DumpDetails(
                    file_path=Path("tests/some-path/wf.1234_1.npz"),
                    format=ArtifactFormat.NUMPY_ARRAY,
                )
                presenter = _presenters.WrappedCorqOutputPresenter()

                # When
                presenter.show_dumped_wf_result(details)

                # Then
                captured = capsys.readouterr()
                # We can't assert on the full path because separators are
                # platform-dependent.
                assert "Artifact saved at tests" in captured.out
                assert "wf.1234_1.npz as NUMPY_ARRAY." in captured.out

        @staticmethod
        def test_show_artifact_values(capsys):
            # Given
            values = [set([21, 38]), {"hello": "there"}]
            wf_run_id = "wf.1234"
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_workflow_outputs(values, wf_run_id)

            # Then
            captured = capsys.readouterr()
            assert (
                "Workflow run wf.1234 has 2 outputs.\n"
                "\n"
                "Output 0. Object type: <class 'set'>\n"
                "Pretty printed value:\n"
                "{21, 38}\n"
                "\n"
                "Output 1. Object type: <class 'dict'>\n"
                "Pretty printed value:\n"
                "{'hello': 'there'}\n"
            ) == captured.out

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


class TestLoginPresenter:
    @pytest.mark.parametrize("ce", [True, False])
    def test_prompt_for_login(self, capsys, ce):
        # Given
        url = "cool_url"
        login_url = "cool_login_url"
        presenter = _presenters.LoginPresenter()

        # When
        presenter.prompt_for_login(login_url, url, ce)

        # Then
        captured = capsys.readouterr()
        assert "Please follow this URL to proceed with login" in captured.out
        assert login_url in captured.out
        assert "Then save the token using command:" in captured.out
        assert (
            f"orq login -s {url} -t <paste your token here>" + (" --ce" if ce else "")
            in captured.out
        )

    def test_prompt_config_saved(self, capsys):
        # Given
        url = "cool_url"
        config_name = "cool_config_name"
        presenter = _presenters.LoginPresenter()

        # When
        presenter.prompt_config_saved(url, config_name)

        # Then
        captured = capsys.readouterr()
        assert f"Configuration name for {url} is {config_name}" in captured.out
