################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import sys
import typing as t
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest

from orquestra import sdk
from orquestra.sdk._base import serde
from orquestra.sdk._base._dates import Instant
from orquestra.sdk._base._logs._interfaces import LogOutput
from orquestra.sdk._base._spaces._structs import Project, Workspace
from orquestra.sdk._base.cli._ui import _errors
from orquestra.sdk._base.cli._ui import _models as ui_models
from orquestra.sdk._base.cli._ui import _presenters
from orquestra.sdk._base.cli._ui._corq_format import per_command
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.responses import ResponseStatusCode, ServiceResponse
from orquestra.sdk.schema.workflow_run import RunStatus, State


@sdk.task
def add(a, b):
    return a + b


@sdk.workflow
def my_wf():
    return add(1, 2)


@pytest.fixture
def sys_exit_mock(monkeypatch):
    exit_mock = Mock()
    monkeypatch.setattr(sys, "exit", exit_mock)
    return exit_mock


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
        def test_show_logs_with_dict(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)
            task_invocation = "my_task_invocation"
            task_logs = LogOutput(out=["my_log"], err=[])
            logs = {task_invocation: task_logs}

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert "stdout:" in response_model.logs
            assert task_logs.out[0] in response_model.logs
            assert task_invocation in response_model.logs[0]

        @staticmethod
        def test_show_logs_with_logoutput(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)
            logs = LogOutput(out=["my_log"], err=[])

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert "stdout:" in response_model.logs
            assert logs.out[0] in response_model.logs

        @staticmethod
        def test_print_stdout_when_available(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)
            logs = LogOutput(out=["my_log"], err=[])

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert "stdout:" in response_model.logs
            assert "stderr:" not in response_model.logs

        @staticmethod
        def test_print_stderr_when_available(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)
            logs = LogOutput(out=[], err=["my_log"])

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert "stdout:" not in response_model.logs
            assert "stderr:" in response_model.logs

        @staticmethod
        def test_print_both_when_available(monkeypatch):
            # Given
            pretty_print_mock = Mock()
            monkeypatch.setattr(per_command, "pretty_print_response", pretty_print_mock)
            logs = LogOutput(out=["my_log"], err=["my_log"])

            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            called_args = pretty_print_mock.call_args.args
            response_model = called_args[0]
            assert "stdout:" in response_model.logs
            assert "stderr:" in response_model.logs

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

    class TestShowLogs:
        @staticmethod
        def test_with_mapped_logs(capsys):
            # Given
            logs = {
                "<task invocation id sentinel>": LogOutput(
                    out=[
                        "<log line 1 sentinel>",
                        "<log line 2 sentinel>",
                    ],
                    err=[],
                )
            }
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            captured = capsys.readouterr()
            for line in [
                "task-invocation-id: <task invocation id sentinel>",
                "stdout:",
                "<log line 1 sentinel>",
                "<log line 2 sentinel>",
            ]:
                assert line in captured.out
            assert "=" * 80 not in captured.out

        @staticmethod
        def test_with_logoutput_logs(capsys):
            # Given
            logs = LogOutput(
                out=["<log line 1 sentinel>", "<log line 2 sentinel>"], err=[]
            )
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            captured = capsys.readouterr()
            assert (
                "stdout:\n<log line 1 sentinel>\n<log line 2 sentinel>\n"
                in captured.out
            )
            assert "=" * 80 not in captured.out

        @staticmethod
        def test_with_log_type(capsys):
            # Given
            logs = LogOutput(
                out=["<log line 1 sentinel>", "<log line 2 sentinel>"], err=[]
            )
            log_type = Mock(value="<log type sentinel>")
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs, log_type=log_type)

            # Then
            captured = capsys.readouterr()
            for line in [
                "=== <LOG TYPE SENTINEL> LOGS ===================================================",  # noqa: E501
                "stdout:",
                "<log line 1 sentinel>",
                "<log line 2 sentinel>",
                "================================================================================",  # noqa: E501
            ]:
                assert line in captured.out

        @staticmethod
        def test_stderr_output(capsys):
            # Given
            logs = LogOutput(
                out=[], err=["<log line 1 sentinel>", "<log line 2 sentinel>"]
            )
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            captured = capsys.readouterr()
            assert (
                "stderr:\n<log line 1 sentinel>\n<log line 2 sentinel>\n"
                in captured.out
            )

        @staticmethod
        def test_both_output(capsys):
            # Given
            logs = LogOutput(
                out=["<log line 1 sentinel>"], err=["<log line 2 sentinel>"]
            )
            presenter = _presenters.WrappedCorqOutputPresenter()

            # When
            presenter.show_logs(logs)

            # Then
            captured = capsys.readouterr()
            assert (
                "stdout:\n<log line 1 sentinel>\nstderr:\n<log line 2 sentinel>\n"
                in captured.out
            )

    @staticmethod
    def test_handling_error(monkeypatch, sys_exit_mock):
        # Given
        status_int = 42
        status_code_enum = Mock()
        status_code_enum.value = status_int
        pretty_print_mock = Mock(return_value=status_code_enum)
        monkeypatch.setattr(_errors, "pretty_print_exception", pretty_print_mock)

        exception: t.Any = "<exception object sentinel>"

        presenter = _presenters.WrappedCorqOutputPresenter()

        # When
        presenter.show_error(exception)

        # Then
        # We expect passing error to pretty printer.
        pretty_print_mock.assert_called_with(exception)

        # We expect status code was passed to sys.exit()
        sys_exit_mock.assert_called_with(status_int)


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


class TestServicesPresenter:
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

    @staticmethod
    def test_show_failure(capsys, sys_exit_mock):
        # Given
        presenter = _presenters.ServicePresenter()
        services = [
            ServiceResponse(
                name="background service", is_running=False, info="something"
            )
        ]

        # When
        presenter.show_failure(services)

        # Then
        captured = capsys.readouterr()
        assert services[0].name in captured.out
        assert services[0].info in captured.out
        assert captured.err == ""

        sys_exit_mock.assert_called_with(ResponseStatusCode.SERVICES_ERROR.value)


class TestLoginPresenter:
    def test_prompt_for_login(self, capsys):
        # Given
        url = "cool_url"
        login_url = "cool_login_url"
        presenter = _presenters.LoginPresenter()

        # When
        presenter.prompt_for_login(login_url, url)

        # Then
        captured = capsys.readouterr()
        assert "We were unable to automatically log you in" in captured.out
        assert (
            "Please login to your Orquestra account using the following URL."
            in captured.out
        )
        assert login_url in captured.out
        assert "Then save the token using command:" in captured.out
        assert f"orq login -s {url} -t <paste your token here>" in captured.out

    def test_prompt_config_saved(self, capsys):
        # Given
        url = "cool_url"
        config_name = "cool_config_name"
        runtime_name = "cool_runtime_name"
        presenter = _presenters.LoginPresenter()

        # When
        presenter.prompt_config_saved(url, config_name, runtime_name)

        # Then
        captured = capsys.readouterr()
        assert (
            f"Configuration name for {url} "
            f"with runtime {runtime_name} "
            f"is '{config_name}'" in captured.out
        )

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


class TestConfigPresenter:
    @staticmethod
    def test_rpint_configs_list(capsys):
        presenter = _presenters.ConfigPresenter()
        configs = [create_autospec(RuntimeConfiguration) for _ in range(3)]
        for i, _ in enumerate(configs):
            configs[i].config_name = f"<config name sentinel {i}>"
            configs[i].runtime_name = f"<runtime name sentinel {i}>"
            configs[i].runtime_options = {"uri": f"<uri sentinel {i}>"}
        status = {config.config_name: True for config in configs}
        status[configs[1].config_name] = False

        # When
        presenter.print_configs_list(configs, status)

        assert capsys.readouterr().out == (
            "Stored configs:\n"
            "Config Name               Runtime                    Server URI        Current Token\n"  # noqa: E501
            "<config name sentinel 0>  <runtime name sentinel 0>  <uri sentinel 0>  ✓\n"
            "<config name sentinel 1>  <runtime name sentinel 1>  <uri sentinel 1>  ⨉\n"
            "<config name sentinel 2>  <runtime name sentinel 2>  <uri sentinel 2>  ✓\n"
        )


DATA_DIR = Path(__file__).parent / "data"


UTC_INSTANT = Instant(datetime(2023, 2, 24, 12, 26, 7, 704015, tzinfo=timezone.utc))
ET_INSTANT_1 = Instant(
    datetime(
        2023,
        2,
        24,
        7,
        26,
        7,
        704015,
        tzinfo=timezone.utc,
    )
)
ET_INSTANT_2 = Instant(
    datetime(
        2023,
        2,
        24,
        7,
        28,
        37,
        123,
        tzinfo=timezone.utc,
    )
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
    def test_show_wf_run(
        monkeypatch, capsys, summary: ui_models.WFRunSummary, expected_path: Path
    ):
        # Given
        presenter = _presenters.WFRunPresenter()
        monkeypatch.setattr(
            _presenters,
            "_format_datetime",
            lambda x: "Fri Feb 24 08:26:07 2023" if x else "",
        )
        # When
        presenter.show_wf_run(summary)

        # Then
        captured = capsys.readouterr()

        expected = expected_path.read_text()
        assert captured.out == expected


class TestPromptPresenter:
    class TestWorkspacesList:
        def test_workspaces_list_to_prompt(self):
            # given
            workspace1 = Workspace("id1", "name1")
            workspace2 = Workspace("id2", "name2")
            workspace3 = Workspace("id3", "name3")
            workspace_list = [workspace1, workspace2, workspace3]
            presenter = _presenters.PromptPresenter()

            # when
            labels, workspaces = presenter.workspaces_list_to_prompt(
                workspaces=workspace_list
            )

            assert workspaces == workspace_list
            assert "id1" in labels[0]
            assert "name1" in labels[0]
            assert "id2" in labels[1]
            assert "name2" in labels[1]
            assert "id3" in labels[2]
            assert "name3" in labels[2]

        def test_workspace_list_to_prompt_env_var_set(self, monkeypatch):
            current_workspace_id = "id3"
            monkeypatch.setenv("ORQ_CURRENT_WORKSPACE", current_workspace_id)
            workspace1 = Workspace("id1", "name1")
            workspace2 = Workspace("id2", "name2")
            workspace3 = Workspace(current_workspace_id, "name3")

            workspace_list = [workspace1, workspace2, workspace3]
            presenter = _presenters.PromptPresenter()

            # when
            labels, workspaces = presenter.workspaces_list_to_prompt(
                workspaces=workspace_list
            )

            # current workspace should be the 1st one in the list
            assert workspaces == [workspace3, workspace1, workspace2]
            assert current_workspace_id in labels[0]
            assert "name3" in labels[0]
            assert "CURRENT WORKSPACE" in labels[0]

            assert "id1" in labels[1]
            assert "name1" in labels[1]

            assert "id2" in labels[2]
            assert "name2" in labels[2]

    class TestProjectList:
        def test_project_list_to_prompt(self):
            # given
            project1 = Project("id1", "ws", "name1")
            project2 = Project("id2", "ws", "name2")
            project3 = Project("id3", "ws", "name3")
            project_list = [project1, project2, project3]
            presenter = _presenters.PromptPresenter()

            # when
            labels, projects = presenter.project_list_to_prompt(project_list)

            assert projects == project_list
            assert "id1" in labels[0]
            assert "name1" in labels[0]
            assert "id2" in labels[1]
            assert "name2" in labels[1]
            assert "id3" in labels[2]
            assert "name3" in labels[2]

        def test_workspace_list_to_prompt_env_set(self, monkeypatch):
            current_project_id = "id3"
            monkeypatch.setenv("ORQ_CURRENT_PROJECT", current_project_id)
            project1 = Project("id1", "ws", "name1")
            project2 = Project("id2", "ws", "name2")
            project3 = Project(current_project_id, "ws", "name3")

            project_list = [project1, project2, project3]
            presenter = _presenters.PromptPresenter()

            # when
            labels, projects = presenter.project_list_to_prompt(project_list)

            # current project should be the 1st one in the list
            assert projects == [project3, project1, project2]
            assert current_project_id in labels[0]
            assert "name3" in labels[0]
            assert "CURRENT PROJECT" in labels[0]

            assert "id1" in labels[1]
            assert "name1" in labels[1]

            assert "id2" in labels[2]
            assert "name2" in labels[2]
