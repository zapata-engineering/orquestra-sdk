################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import sys
import typing as t
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from orquestra import sdk
from orquestra.sdk._base import serde
from orquestra.sdk._base.cli._corq._format import per_command
from orquestra.sdk._base.cli._dorq._ui import _errors
from orquestra.sdk._base.cli._dorq._ui import _models as ui_models
from orquestra.sdk._base.cli._dorq._ui import _presenters
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.responses import ServiceResponse
from orquestra.sdk.schema.workflow_run import RunStatus, State


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

        @staticmethod
        def test_show_logs(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)
            task_invocation = "my_task_invocation"
            task_logs = ["my_log"]
            logs = {task_invocation: task_logs}

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert task_logs[0] in response_model.logs
            assert task_invocation in response_model.logs[0]

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
        def test_show_dumped_wf_logs(capsys):
            # Given
            dummy_path: Path = Path("/my/cool/path")
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_dumped_wf_logs(dummy_path)

            # Then
            captured = capsys.readouterr()
            assert f"Workflow logs saved at {dummy_path}" in captured.out

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


class TestArtifactPresenter:
    class TestDumpedWFResult:
        @staticmethod
        def test_json(capsys):
            # Given
            details = serde.DumpDetails(
                file_path=Path("tests/some-path/wf.1234_1.json"),
                format=ArtifactFormat.JSON,
            )
            presenter = _presenters.ArtifactPresenter()

            # When
            presenter.show_dumped_artifact(details)

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
            presenter = _presenters.ArtifactPresenter()

            # When
            presenter.show_dumped_artifact(details)

            # Then
            captured = capsys.readouterr()
            # We can't assert on the full path because separators are
            # platform-dependent.
            assert "Artifact saved at tests" in captured.out
            assert "wf.1234_1.pickle as a binary pickle file." in captured.out

        @staticmethod
        def test_other_format(capsys):
            # Given
            details = serde.DumpDetails(
                file_path=Path("tests/some-path/wf.1234_1.npz"),
                format=ArtifactFormat.NUMPY_ARRAY,
            )
            presenter = _presenters.ArtifactPresenter()

            # When
            presenter.show_dumped_artifact(details)

            # Then
            captured = capsys.readouterr()
            # We can't assert on the full path because separators are
            # platform-dependent.
            assert "Artifact saved at tests" in captured.out
            assert "wf.1234_1.npz as NUMPY_ARRAY." in captured.out

    @staticmethod
    def test_show_workflow_outputs(capsys):
        # Given
        values = [set([21, 38]), {"hello": "there"}]
        wf_run_id = "wf.1234"
        presenter = _presenters.ArtifactPresenter()

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
    def test_show_task_outputs(capsys):
        # Given
        values = [set([21, 38]), {"hello": "there"}]
        wf_run_id = "wf.1234"
        task_inv_id = "inv6"
        presenter = _presenters.ArtifactPresenter()

        # When
        presenter.show_task_outputs(values, wf_run_id, task_inv_id)

        # Then
        captured = capsys.readouterr()
        assert (
            "In workflow wf.1234, task invocation inv6 produced 2 outputs.\n"
            "\n"
            "Output 0. Object type: <class 'set'>\n"
            "Pretty printed value:\n"
            "{21, 38}\n"
            "\n"
            "Output 1. Object type: <class 'dict'>\n"
            "Pretty printed value:\n"
            "{'hello': 'there'}\n"
        ) == captured.out


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
        assert "We were unable to automatically log you in" in captured.out
        assert (
            "Please login to your Orquestra account using the following URL."
            in captured.out
        )
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

    def test_print_login_help(self, capsys):
        presenter = _presenters.LoginPresenter()

        # When
        presenter.print_login_help()

        # Then
        captured = capsys.readouterr()
        assert "Continue the login process in your web browser." in captured.out

    @pytest.mark.parametrize("success", [True, False])
    def test_open_url_in_browser(self, success: bool, monkeypatch: pytest.MonkeyPatch):
        open_browser = Mock(return_value=success)
        monkeypatch.setattr("webbrowser.open", open_browser)
        presenter = _presenters.LoginPresenter()
        url = "test_url"

        browser_opened = presenter.open_url_in_browser(url)

        assert browser_opened == success
        open_browser.assert_called_once_with(url)


DATA_DIR = Path(__file__).parent / "data"


UTC_INSTANT = datetime(2023, 2, 24, 12, 26, 7, 704015, tzinfo=timezone.utc)
ET_INSTANT_1 = datetime(
    2023,
    2,
    24,
    7,
    26,
    7,
    704015,
    tzinfo=timezone(timedelta(hours=-5)),
)
ET_INSTANT_2 = datetime(
    2023,
    2,
    24,
    7,
    28,
    37,
    123,
    tzinfo=timezone(timedelta(hours=-5)),
)


class TestWorkflowRunPresenter:
    @staticmethod
    @pytest.mark.parametrize(
        "summary,expected_path",
        [
            pytest.param(
                ui_models.WFRunSummary(
                    wf_def_name="hello_orq",
                    wf_run_id="wf.1",
                    wf_run_status=RunStatus(
                        state=State.WAITING,
                        start_time=UTC_INSTANT,
                    ),
                    task_rows=[],
                    n_tasks_succeeded=0,
                    n_task_invocations_total=21,
                ),
                DATA_DIR / "wf_runs" / "waiting.txt",
                id="waiting",
            ),
            pytest.param(
                ui_models.WFRunSummary(
                    wf_def_name="hello_orq",
                    wf_run_id="wf.1",
                    wf_run_status=RunStatus(
                        state=State.RUNNING,
                        start_time=ET_INSTANT_1,
                    ),
                    task_rows=[
                        ui_models.WFRunSummary.TaskRow(
                            task_fn_name="generate_data",
                            inv_id="inv-1-gen-dat",
                            status=RunStatus(
                                state=State.SUCCEEDED,
                                start_time=ET_INSTANT_1,
                                end_time=ET_INSTANT_2,
                            ),
                            message=None,
                        ),
                        ui_models.WFRunSummary.TaskRow(
                            task_fn_name="train_model",
                            inv_id="inv-2-tra-mod",
                            status=RunStatus(
                                state=State.RUNNING,
                                start_time=ET_INSTANT_2,
                            ),
                            message=None,
                        ),
                    ],
                    n_tasks_succeeded=1,
                    n_task_invocations_total=2,
                ),
                DATA_DIR / "wf_runs" / "running.txt",
                id="running",
            ),
        ],
    )
    def test_show_wf_run(capsys, summary: ui_models.WFRunSummary, expected_path: Path):
        # Given
        presenter = _presenters.WFRunPresenter()

        # When
        presenter.show_wf_run(summary)

        # Then
        captured = capsys.readouterr()

        expected = expected_path.read_text()
        assert captured.out == expected
