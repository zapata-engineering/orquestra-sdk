################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
import sys
import typing as t
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest
from rich.console import Console

from orquestra import sdk
from orquestra.sdk._client._base.cli._ui import _errors
from orquestra.sdk._client._base.cli._ui import _models as ui_models
from orquestra.sdk._client._base.cli._ui import _presenters
from orquestra.sdk._shared import serde
from orquestra.sdk._shared._spaces._structs import Project, Workspace
from orquestra.sdk._shared.dates._dates import Instant
from orquestra.sdk._shared.logs._interfaces import LogOutput
from orquestra.sdk._shared.schema.configs import RuntimeConfiguration
from orquestra.sdk._shared.schema.ir import ArtifactFormat
from orquestra.sdk._shared.schema.responses import ResponseStatusCode, ServiceResponse
from orquestra.sdk._shared.schema.workflow_run import RunStatus, State


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


CONSOLE_WIDTH = 120


@pytest.fixture
def test_console():
    return Console(file=StringIO(), width=CONSOLE_WIDTH)


class TestLogsPresenter:
    @staticmethod
    @pytest.fixture
    def rule():
        def _inner(prefix: t.Optional[str] = None):
            line = "─" * CONSOLE_WIDTH
            if prefix is None:
                return line
            return f"{prefix} {line[len(prefix) + 1:]}"

        return _inner

    @staticmethod
    def test_show_logs_with_dict(test_console: Console):
        # Given
        task_invocation = "my_task_invocation"
        task_logs = LogOutput(out=["my_log"], err=[])
        logs = {task_invocation: task_logs}

        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert "my_task_invocation" in output
        assert "stdout" in output
        assert "my_log" in output

    @staticmethod
    def test_show_logs_with_logoutput(test_console: Console):
        # Given
        logs = LogOutput(out=["my_log"], err=[])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert "stdout" in output
        assert "my_log" in output

    @staticmethod
    def test_print_stdout_when_available(test_console: Console):
        # Given
        logs = LogOutput(out=["my_log"], err=[])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert "stdout" in output
        assert "stderr" not in output

    @staticmethod
    def test_print_stderr_when_available(test_console: Console):
        # Given
        logs = LogOutput(out=[], err=["my_log"])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert "stdout" not in output
        assert "stderr" in output

    @staticmethod
    def test_print_both_when_available(test_console: Console):
        # Given
        logs = LogOutput(out=["my_log"], err=["my_log"])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert "stdout" in output
        assert "stderr" in output

    @staticmethod
    def test_show_dumped_wf_logs(test_console: Console):
        # Given
        dummy_path: Path = Path("/my/cool/path")
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_dumped_wf_logs(dummy_path)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert f"Workflow logs saved at {dummy_path}" in output

    @staticmethod
    def test_with_mapped_logs(test_console, rule: t.Callable[..., str]):
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
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        for line in [
            "<task invocation id sentinel>",
            "stdout",
            "<log line 1 sentinel>",
            "<log line 2 sentinel>",
        ]:
            assert line in output
        assert rule() not in output

    @staticmethod
    def test_with_logoutput_logs(test_console: Console):
        # Given
        logs = LogOutput(out=["<log line 1 sentinel>", "<log line 2 sentinel>"], err=[])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        expected = "\n".join(
            [
                "                                  ",
                "  stdout   <log line 1 sentinel>  ",
                "           <log line 2 sentinel>  ",
                "                                  ",
                "",
            ]
        )
        assert output == expected

    @staticmethod
    def test_with_log_type(test_console, rule: t.Callable[..., str]):
        # Given
        logs = LogOutput(out=["<log line 1 sentinel>", "<log line 2 sentinel>"], err=[])
        log_type = Mock(value="<log type sentinel>")
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs, log_type=log_type)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        expected = "\n".join(
            [
                rule("<log type sentinel> logs"),
                "                                  ",
                "  stdout   <log line 1 sentinel>  ",
                "           <log line 2 sentinel>  ",
                "                                  ",
                rule(),
                "",
            ]
        )
        assert expected == output

    @staticmethod
    def test_stderr_output(test_console: Console):
        # Given
        logs = LogOutput(out=[], err=["<log line 1 sentinel>", "<log line 2 sentinel>"])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        expected = "\n".join(
            [
                "                                  ",
                "  stderr   <log line 1 sentinel>  ",
                "           <log line 2 sentinel>  ",
                "                                  ",
                "",
            ]
        )
        assert output == expected

    @staticmethod
    def test_both_output(test_console: Console):
        # Given
        logs = LogOutput(out=["<log line 1 sentinel>"], err=["<log line 2 sentinel>"])
        presenter = _presenters.LogsPresenter(console=test_console)

        # When
        presenter.show_logs(logs)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        expected = "\n".join(
            [
                "                                  ",
                "  stdout   <log line 1 sentinel>  ",
                "  stderr   <log line 2 sentinel>  ",
                "                                  ",
                "",
            ]
        )
        assert output == expected


class TestWrappedCorqOutputPresenter:
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
        def test_json(test_console: Console):
            # Given
            details = serde.DumpDetails(
                file_path=Path("tests/some-path/wf.1234_1.json"),
                format=ArtifactFormat.JSON,
            )
            presenter = _presenters.ArtifactPresenter(console=test_console)

            # When
            presenter.show_dumped_artifact(details)

            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            # We can't assert on the full path because separators are
            # platform-dependent.
            assert "Artifact saved at tests" in output
            assert "wf.1234_1.json as a text json file." in output

        @staticmethod
        def test_pickle(test_console: Console):
            # Given
            details = serde.DumpDetails(
                file_path=Path("tests/some-path/wf.1234_1.pickle"),
                format=ArtifactFormat.ENCODED_PICKLE,
            )
            presenter = _presenters.ArtifactPresenter(console=test_console)

            # When
            presenter.show_dumped_artifact(details)

            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            # We can't assert on the full path because separators are
            # platform-dependent.
            assert "Artifact saved at tests" in output
            assert "wf.1234_1.pickle as a binary pickle file." in output

        @staticmethod
        def test_other_format(test_console: Console):
            # Given
            details = serde.DumpDetails(
                file_path=Path("tests/some-path/wf.1234_1.npz"),
                format=ArtifactFormat.NUMPY_ARRAY,
            )
            presenter = _presenters.ArtifactPresenter(console=test_console)

            # When
            presenter.show_dumped_artifact(details)

            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            # We can't assert on the full path because separators are
            # platform-dependent.
            assert "Artifact saved at tests" in output
            assert "wf.1234_1.npz as NUMPY_ARRAY." in output

    @staticmethod
    @pytest.mark.skipif(
        sys.platform.startswith("win32"),
        reason="Windows uses different symbols than macOS and Linux",
    )
    def test_show_workflow_outputs(test_console: Console):
        # Given
        values = [set([21, 38]), {"hello": "there"}]
        wf_run_id = "wf.1234"
        presenter = _presenters.ArtifactPresenter(console=test_console)

        # When
        presenter.show_workflow_outputs(values, wf_run_id)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        expected = "\n".join(
            [
                "Workflow run wf.1234 has 2 outputs.",
                "                                               ",
                "  Index   Type             Pretty Printed      ",
                " ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ ",
                "  0       <class 'set'>    {21, 38}            ",
                "  1       <class 'dict'>   {'hello': 'there'}  ",
                "                                               ",
                "",
            ]
        )
        assert output == expected

    @staticmethod
    @pytest.mark.skipif(
        sys.platform.startswith("win32"),
        reason="Windows uses different symbols than macOS and Linux",
    )
    def test_show_task_outputs(test_console: Console):
        # Given
        values = [set([21, 38]), {"hello": "there"}]
        wf_run_id = "wf.1234"
        task_inv_id = "inv6"
        presenter = _presenters.ArtifactPresenter(console=test_console)

        # When
        presenter.show_task_outputs(values, wf_run_id, task_inv_id)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        expected = "\n".join(
            [
                "In workflow wf.1234, task invocation inv6 produced 2 outputs.",
                "                                               ",
                "  Index   Type             Pretty Printed      ",
                " ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ ",
                "  0       <class 'set'>    {21, 38}            ",
                "  1       <class 'dict'>   {'hello': 'there'}  ",
                "                                               ",
                "",
            ]
        )
        assert output == expected


class TestServicesPresenter:
    class TestShowServices:
        def test_running(self, test_console: Console):
            # Given
            services = [ServiceResponse(name="mocked", is_running=True, info=None)]
            presenter = _presenters.ServicePresenter(console=test_console)
            # When
            presenter.show_services(services)
            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            expected = "\n".join(
                [
                    "                       ",
                    "  mocked   Running     ",
                    "                       ",
                    "",
                ]
            )
            assert output == expected

        def test_not_running(self, test_console: Console):
            # Given
            services = [ServiceResponse(name="mocked", is_running=False, info=None)]
            presenter = _presenters.ServicePresenter(console=test_console)
            # When
            presenter.show_services(services)
            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            expected = "\n".join(
                [
                    "                           ",
                    "  mocked   Not Running     ",
                    "                           ",
                    "",
                ]
            )
            assert output == expected

        def test_with_info(self, test_console: Console):
            # Given
            services = [
                ServiceResponse(name="mocked", is_running=False, info="something")
            ]
            presenter = _presenters.ServicePresenter(console=test_console)
            # When
            presenter.show_services(services)
            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            expected = "\n".join(
                [
                    "                                    ",
                    "  mocked   Not Running   something  ",
                    "                                    ",
                    "",
                ]
            )
            assert output == expected

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
    def test_show_submitted_wf_run(test_console: Console):
        # Given
        wf_run_id = "wf.1"

        presenter = _presenters.WFRunPresenter(console=test_console)

        # When
        presenter.show_submitted_wf_run(wf_run_id)

        # Then
        expected = "Workflow Submitted! Run ID: wf.1\n"
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()
        assert output == expected

    @staticmethod
    @pytest.mark.skipif(
        sys.platform.startswith("win32"),
        reason="Windows uses different symbols than macOS and Linux",
    )
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
                        end_time=None,
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
                        end_time=None,
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
                                end_time=None,
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
        monkeypatch, test_console, summary: ui_models.WFRunSummary, expected_path: Path
    ):
        # Given
        presenter = _presenters.WFRunPresenter(console=test_console)
        monkeypatch.setattr(
            _presenters,
            "_format_datetime",
            lambda x: "Fri Feb 24 08:26:07 2023" if x else "",
        )
        # When
        presenter.show_wf_run(summary)

        # Then
        assert isinstance(test_console.file, StringIO)
        output = test_console.file.getvalue()

        expected = expected_path.read_text()
        assert output == expected

    @pytest.mark.skipif(
        sys.platform.startswith("win32"),
        reason="Windows uses different symbols than macOS and Linux",
    )
    class TestShowWFList:
        @staticmethod
        def test_show_wf_list_with_owner(
            monkeypatch: pytest.MonkeyPatch, test_console: Console
        ):
            # Given
            expected_path = DATA_DIR / "list_wf_runs_with_owner.txt"
            summary = ui_models.WFList(
                wf_rows=[
                    ui_models.WFList.WFRow(
                        workflow_run_id="wf1",
                        status="mocked status",
                        tasks_succeeded="x/y",
                        start_time=UTC_INSTANT,
                    ),
                    ui_models.WFList.WFRow(
                        workflow_run_id="wf2",
                        status="mocked status",
                        tasks_succeeded="x/y",
                        start_time=None,
                        owner="taylor.swift@zapatacomputing.com",
                    ),
                ]
            )
            presenter = _presenters.WFRunPresenter(console=test_console)
            monkeypatch.setattr(
                _presenters,
                "_format_datetime",
                lambda x: "Fri Feb 24 08:26:07 2023" if x else "",
            )
            # When
            presenter.show_wf_list(summary)

            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            expected = expected_path.read_text()
            assert output == expected

        @staticmethod
        def test_show_wf_list_without_owner(
            monkeypatch: pytest.MonkeyPatch, test_console: Console
        ):
            # Given
            expected_path = DATA_DIR / "list_wf_runs.txt"
            summary = ui_models.WFList(
                wf_rows=[
                    ui_models.WFList.WFRow(
                        workflow_run_id="wf1",
                        status="mocked status",
                        tasks_succeeded="x/y",
                        start_time=UTC_INSTANT,
                    ),
                    ui_models.WFList.WFRow(
                        workflow_run_id="wf2",
                        status="mocked status",
                        tasks_succeeded="x/y",
                        start_time=None,
                    ),
                ]
            )
            presenter = _presenters.WFRunPresenter(console=test_console)
            monkeypatch.setattr(
                _presenters,
                "_format_datetime",
                lambda x: "Fri Feb 24 08:26:07 2023" if x else "",
            )
            # When
            presenter.show_wf_list(summary)

            # Then
            assert isinstance(test_console.file, StringIO)
            output = test_console.file.getvalue()
            expected = expected_path.read_text()
            assert output == expected


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


class TestGraphPresenter:
    class TestView:
        def test_default_file(self, monkeypatch):
            # Given
            mock_graph = create_autospec(_presenters.Digraph)

            # When
            _presenters.GraphPresenter().view(mock_graph)

            # Then
            mock_graph.view.assert_called_once_with(cleanup=True)

        def test_explicit_reraise(self, monkeypatch):
            # Given
            mock_graph = create_autospec(_presenters.Digraph)
            mock_graph.view.side_effect = _presenters.ExecutableNotFound("foo")

            # When
            with pytest.raises(_presenters.ExecutableNotFound):
                _presenters.GraphPresenter().view(mock_graph)

            # Then
            mock_graph.view.assert_called_once_with(cleanup=True)
