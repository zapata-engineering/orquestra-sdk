################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Tests the 'orq login' and `orq set token` actions.

Testing dimensions:
- config file exists/doesn't exist
- config file contains the configuration/doesn't contain it
- the user passes in args/doesn't pass in args
- there's a connection error in the mean time
"""


import argparse
import io
from argparse import Namespace
from pathlib import Path

import pytest
import responses

from orquestra.sdk import exceptions
from orquestra.sdk._base import _config, _db
from orquestra.sdk._base.cli._corq import action
from orquestra.sdk.schema.configs import (
    CONFIG_FILE_CURRENT_VERSION,
    RuntimeConfiguration,
    RuntimeConfigurationFile,
    RuntimeName,
)
from orquestra.sdk.schema.local_database import StoredWorkflowRun

from ..project_state import TINY_WORKFLOW_DEF

EMPTY_CONFIG_FILE = RuntimeConfigurationFile(
    version=CONFIG_FILE_CURRENT_VERSION,
    configs={},
)


def _config_with_entry(config_name: str, server_uri="http://localhost"):
    return RuntimeConfigurationFile(
        version=CONFIG_FILE_CURRENT_VERSION,
        configs={
            config_name: RuntimeConfiguration(
                config_name=config_name,
                runtime_name=RuntimeName.QE_REMOTE,
                runtime_options={"uri": server_uri, "token": "foobar"},
            )
        },
        default_config_name=config_name,
    )


def _malformed_config_with_entry(config_name: str):
    return RuntimeConfigurationFile(
        version=CONFIG_FILE_CURRENT_VERSION,
        configs={
            config_name: RuntimeConfiguration(
                config_name=config_name,
                runtime_name=RuntimeName.QE_REMOTE,
                runtime_options={},
            )
        },
    )


CONFIG_WITH_REMOTE = _config_with_entry("remote")
CONFIG_WITH_REMOTE_NO_OPTS = _malformed_config_with_entry("remote")


def _read_config_file(dirpath: Path) -> RuntimeConfigurationFile:
    """
    Reads the contents of the user config file at 'dirpath'.
    """
    config_file_path = dirpath / _config.CONFIG_FILE_NAME
    return RuntimeConfigurationFile.parse_file(config_file_path)


def _write_new_config(
    dirpath: Path,
    config_file: RuntimeConfigurationFile,
):
    """
    Overwrites the user config at 'dirpath' with a file with a single configuration:
    'runtime_config'.
    """
    config_file_path = dirpath / _config.CONFIG_FILE_NAME
    config_file_path.write_text(config_file.json())


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=True) as mocked_responses:
        yield mocked_responses


class TestWithDefaultConfigOnly:
    """
    Tests for 'orq login' without any args. 'config' can be resolved to the
    sensible default of "remote", but we need at least a 'server_uri'.
    """

    def test_no_config_file(self, capsys, patch_config_location):
        """
        No config file - probably that's the first time the user ever logs in.
        We don't have 'server_uri'.
        """
        # Applying just 'patch_config_location' makes the CLI think there's no
        # config file.
        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config=None,
                    default_config=True,
                )
            )
        captured = capsys.readouterr()
        assert "Please provide the quantum engine server uri" in captured.err

    def test_config_file_without_remote(self, capsys, patch_config_location):
        """
        There is a config file, but it doesn't contain "remote". We don't
        have 'server_uri'.
        """
        _write_new_config(patch_config_location, EMPTY_CONFIG_FILE)

        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config=None,
                    default_config=True,
                )
            )

        captured = capsys.readouterr()
        assert "Please provide the quantum engine server uri" in captured.err

    def test_config_file_with_remote_malformed(self, capsys, patch_config_location):
        """
        There is a config file, it contains "remote", but we don't
        have 'server_uri' because the file was edited in the mean time.
        """
        _write_new_config(patch_config_location, CONFIG_WITH_REMOTE_NO_OPTS)

        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config=None,
                    default_config=True,
                )
            )

        captured = capsys.readouterr()
        assert "Please provide the quantum engine server uri" in captured.err


class TestWithConfigNameOnly:
    """
    Tests for 'orq login -c <config_name>' - the common case for refreshing
    the token.
    """

    def test_no_config_file(self, capsys, patch_config_location):
        """
        No config file - probably that's the first time the user ever logs in.
        We don't have 'server_uri'.
        """
        # Applying just 'patch_config_location' makes the CLI think there's no
        # config file.
        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config="a-cluster-name",
                    default_config=False,
                )
            )
        captured = capsys.readouterr()
        assert "Please provide the quantum engine server uri" in captured.err

    def test_config_file_without_entry(self, capsys, patch_config_location):
        """
        There is a config file, but it doesn't contain the config the user
        asked for. We don't have 'server_uri'.
        """
        _write_new_config(patch_config_location, EMPTY_CONFIG_FILE)

        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config="a-cluster-name",
                    default_config=False,
                )
            )

        captured = capsys.readouterr()
        assert "Please provide the quantum engine server uri" in captured.err

    def test_config_file_with_entry_malformed(self, capsys, patch_config_location):
        """
        There is a config file, it contains the config the user asked for,
        but we don't have 'server_uri' because the file was edited in the mean time.
        """
        _write_new_config(
            patch_config_location, _malformed_config_with_entry("a-cluster-name")
        )

        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config="a-cluster-name",
                    default_config=False,
                )
            )

        captured = capsys.readouterr()
        assert "Please provide the quantum engine server uri" in captured.err

    def test_config_file_with_entry_ok(
        self, capsys, patch_config_location, mocked_responses, monkeypatch
    ):
        """
        There is a config file, it contains "a-cluster-name", and there's
        'server_uri'. Great success!
        """
        _write_new_config(patch_config_location, _config_with_entry("a-cluster-name"))

        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/login",
            headers={"Location": "http://localhost/test"},
        )

        monkeypatch.setattr("sys.stdin", io.StringIO("my token"))

        action.orq_login(
            argparse.Namespace(
                server_uri=None,
                runtime=None,
                config="a-cluster-name",
                default_config=False,
            )
        )

        captured = capsys.readouterr()

        assert "Please follow this URL to proceed with login" in captured.out
        assert captured.err == ""


class TestWithServerUri:
    """
    Tests for 'orq login -c <config_name> -s <server_uri>' - the explicit
    form that first-time users and Studio rely on. All these cases should
    not return errors.
    """

    @pytest.fixture
    def mock_login_request(self, mocked_responses):
        mocked_responses.add(
            responses.GET,
            "https://prod-d.orquestra.io/v1/login",
            headers={"Location": "http://localhost/test"},
        )

    @pytest.fixture
    def mock_pasting_stdin(self, monkeypatch):
        token = "my token"
        monkeypatch.setattr("sys.stdin", io.StringIO(token))
        return token

    @pytest.mark.parametrize(
        "config_name",
        [None, "a-cluster-name"],
    )
    def test_no_config_file(
        self,
        capsys,
        patch_config_location,
        mock_login_request,
        mock_pasting_stdin,
        config_name,
    ):
        """
        No config file - probably that's the first time the user ever logs in.
        """
        # Applying just 'patch_config_location' makes the CLI think there's no
        # config file.
        action.orq_login(
            argparse.Namespace(
                server_uri="https://prod-d.orquestra.io/",
                runtime=None,
                config=config_name,
                default_config=False,
            )
        )

        captured = capsys.readouterr()

        assert "Please follow this URL to proceed with login" in captured.out
        assert captured.err == ""

        result_config = _read_config_file(patch_config_location)
        assert result_config.configs["prod-d"].config_name == "prod-d"
        assert result_config.configs["prod-d"].runtime_name == RuntimeName.QE_REMOTE
        assert result_config.configs["prod-d"].runtime_options == {
            "uri": "https://prod-d.orquestra.io/",
            "token": mock_pasting_stdin,
        }

    @pytest.mark.parametrize(
        "config_name",
        [None, "a-cluster-name"],
    )
    def test_config_file_without_entry(
        self,
        capsys,
        patch_config_location,
        mock_login_request,
        mock_pasting_stdin,
        config_name,
    ):
        """
        There is a config file, but it doesn't contain the config the user
        asked for.
        """
        _write_new_config(patch_config_location, EMPTY_CONFIG_FILE)

        action.orq_login(
            argparse.Namespace(
                server_uri="https://prod-d.orquestra.io/",
                runtime=None,
                config=config_name,
                default_config=False,
            )
        )

        captured = capsys.readouterr()
        assert "Please follow this URL to proceed with login" in captured.out
        assert captured.err == ""

        result_config = _read_config_file(patch_config_location)
        assert result_config.configs["prod-d"].config_name == "prod-d"
        assert result_config.configs["prod-d"].runtime_name == RuntimeName.QE_REMOTE
        assert result_config.configs["prod-d"].runtime_options == {
            "uri": "https://prod-d.orquestra.io/",
            "token": mock_pasting_stdin,
        }

    @pytest.mark.parametrize(
        "config_name",
        [None, "a-cluster-name"],
    )
    def test_config_file_with_entry_malformed(
        self,
        capsys,
        patch_config_location,
        mock_login_request,
        mock_pasting_stdin,
        config_name,
    ):
        """
        There is a config file, it contains the config the user asked for,
        but we don't have 'server_uri' because the file was edited in the mean time.
        """
        _write_new_config(
            patch_config_location, _malformed_config_with_entry("a-cluster-name")
        )

        action.orq_login(
            argparse.Namespace(
                server_uri="https://prod-d.orquestra.io/",
                runtime=None,
                config=config_name,
                default_config=False,
            )
        )
        # if config name exists and user uses it - we just update entry
        sentinel_config_name = config_name if config_name else "prod-d"

        captured = capsys.readouterr()
        assert "Please follow this URL to proceed with login" in captured.out
        assert captured.err == ""

        result_config = _read_config_file(patch_config_location)
        assert (
            result_config.configs[sentinel_config_name].config_name
            == sentinel_config_name
        )
        assert (
            result_config.configs[sentinel_config_name].runtime_name
            == RuntimeName.QE_REMOTE
        )
        assert result_config.configs[sentinel_config_name].runtime_options == {
            "uri": "https://prod-d.orquestra.io/",
            "token": mock_pasting_stdin,
        }

    @pytest.mark.parametrize(
        "config_name",
        [None, "a-cluster-name"],
    )
    def test_config_file_with_entry_ok(
        self,
        capsys,
        patch_config_location,
        mock_login_request,
        mocked_responses,
        mock_pasting_stdin,
        config_name,
    ):
        """
        There is a config file, it contains "a-cluster-name", and there's
        a different 'server_uri'. Should use the one we're providing
        """
        _write_new_config(patch_config_location, _config_with_entry("a-cluster-name"))

        action.orq_login(
            argparse.Namespace(
                server_uri="https://prod-d.orquestra.io/",
                runtime=None,
                config=config_name,
                default_config=False,
            )
        )
        # if config name exists and user uses it - we just update entry
        sentinel_config_name = config_name if config_name else "prod-d"

        captured = capsys.readouterr()
        assert "Please follow this URL to proceed with login" in captured.out
        assert captured.err == ""

        result_config = _read_config_file(patch_config_location)
        assert (
            result_config.configs[sentinel_config_name].config_name
            == sentinel_config_name
        )
        assert (
            result_config.configs[sentinel_config_name].runtime_name
            == RuntimeName.QE_REMOTE
        )
        assert result_config.configs[sentinel_config_name].runtime_options == {
            "uri": "https://prod-d.orquestra.io/",
            "token": mock_pasting_stdin,
        }


class TestConnectionErrors:
    """
    A handful of test cases when getting the OAuth prompt URL went wrong.
    """

    def test_no_connection(self, capsys, patch_config_location):
        # No request mocks => there will be a request sent to localhost =>
        # nothing listens there => simulation of a wrong url.
        _write_new_config(
            patch_config_location,
            _config_with_entry(
                "remote",
                server_uri="http://localhost:12345/i-hope-theres-nothing-here",
            ),
        )

        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config=None,
                    default_config=True,
                )
            )
        captured = capsys.readouterr()
        assert "Unable to communicate" in captured.err

    def test_invalid_response(self, capsys, patch_config_location, mocked_responses):
        """
        We get the "login url" from QE's response headers. This test
        simulates a case where QE returns no headers.
        """

        # No request mocks => there will be a request sent to localhost =>
        # nothing listens there => simulation of a wrong url.
        _write_new_config(patch_config_location, CONFIG_WITH_REMOTE)

        # Mocking the request to return a response with no headers.
        mocked_responses.add(responses.GET, "http://localhost/v1/login")

        with pytest.raises(SystemExit):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config=None,
                    default_config=True,
                )
            )

        captured = capsys.readouterr()
        assert "Unable to get login url" in captured.err


class TestFlowWithSeparateTokenSave:
    """
    Tests for cases where the token is too long to paste into stdin on macOS.
    The workaround is to use `orq login` followed by `orq set token`.
    """

    def test_happy_path(
        self,
        monkeypatch,
        capsys,
        mocked_responses,
        patch_config_location,
        tmp_path,
    ):
        # [Set up]
        # 1. Set up fresh config & short-circuit HTTP requests
        _write_new_config(patch_config_location, CONFIG_WITH_REMOTE)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/login",
            headers={"Location": "http://localhost/test"},
        )

        # 2. Run 'orq login' and <ctrl>+C
        def _simulate_ctrl_c(config_name):
            raise KeyboardInterrupt()

        monkeypatch.setattr(action._login, "_get_token_from_stdin", _simulate_ctrl_c)

        with pytest.raises(KeyboardInterrupt):
            action.orq_login(
                argparse.Namespace(
                    server_uri=None,
                    runtime=None,
                    config=None,
                    default_config=True,
                )
            )

        # 3. Write a long token to a file
        token = "zapata" * 1000
        token_path = tmp_path / "token.txt"
        token_path.write_text(token)

        # [Execute]
        # 4. Run 'orq set token'
        action.orq_set_token(
            argparse.Namespace(
                server_uri=None,
                config=None,
                token_file=str(token_path),
            )
        )

        # [Assert]
        # For some reason I couldn't recapture stdout+stderr. I'm gonna read
        # the config from FS for assertions instead.
        result_config_file = _read_config_file(patch_config_location)
        assert result_config_file == RuntimeConfigurationFile(
            version=CONFIG_FILE_CURRENT_VERSION,
            configs={
                "remote": RuntimeConfiguration(
                    config_name="remote",
                    runtime_name=RuntimeName.QE_REMOTE,
                    runtime_options={
                        "uri": CONFIG_WITH_REMOTE.configs["remote"].runtime_options[
                            "uri"
                        ],
                        "token": token,
                    },
                ),
            },
            default_config_name="remote",
        )


def test_token_expired(
    patch_config_location, tmp_path, mock_workflow_db_location, mocked_responses
):
    """Scenario: the user logged in some time ago, submitted a workflow,
    and now tries to get the wf run status, but the token expired in the
    mean time.

    Preparation:
    1. User config file with a runtime configuration entry. It should
        contain some token value already.
    2. Orquestra project directory that contains a StoredWorkflowDef.
    3. Mock the QE API to return 401.

    Action: orq get workflow-run <id>

    Assertion: the response tells the user to log in again.
    """
    # Prep 1.
    remote_uri = "http://localhost"
    config_name = "remote"
    _write_new_config(
        patch_config_location, _config_with_entry(config_name, remote_uri)
    )

    # Prep 2.
    wf_run_id = "a_run_id"
    with _db.WorkflowDB.open_project_db(tmp_path) as db:
        db.save_workflow_run(
            StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name=config_name,
                workflow_def=TINY_WORKFLOW_DEF,
            )
        )

    # Prep 3.
    mocked_responses.add(
        responses.GET,
        f"{remote_uri}/v1/workflow?workflowid={wf_run_id}",
        status=401,
    )

    # Action!
    with pytest.raises(exceptions.UnauthorizedError) as exc_info:
        action.orq_get_workflow_run(
            Namespace(
                workflow_run_id=wf_run_id,
                config=config_name,
                directory=str(tmp_path),
            )
        )
    assert "401 Client Error" in str(exc_info)
