################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Test suite for Ray-specific CLI v2 tests."""

import argparse
import typing as t
from contextlib import suppress as do_not_raise
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

import orquestra.sdk._base._config as v2_config
import orquestra.sdk._base.loader
from orquestra.sdk._base import _db
from orquestra.sdk._base._testing._connections import make_ray_conn
from orquestra.sdk._base.cli._corq import action
from orquestra.sdk._base.cli._corq._main import _parse_age_clarg
from orquestra.sdk._ray import _dag
from orquestra.sdk.exceptions import (
    InvalidTaskIDError,
    InvalidWorkflowRunError,
    NotFoundError,
    WorkflowNotFoundError,
)
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.ir import ArtifactFormat
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.responses import (
    GetLogsResponse,
    GetWorkflowRunResponse,
    GetWorkflowRunResultsResponse,
    ResponseFormat,
    ResponseStatusCode,
    StopWorkflowRunResponse,
    SubmitWorkflowDefResponse,
)
from orquestra.sdk.schema.workflow_run import RunStatus, State, WorkflowRun

from ..project_state import TINY_WORKFLOW_DEF

SAMPLE_WORKFLOW_DEFS_FILE = """import orquestra.sdk as sdk
@sdk.task
def hello():
    return "hello world!"

@sdk.workflow
def wf():
    return [hello()]
"""


@pytest.fixture
def orq_project_dir_with_defs(tmp_path, mock_workflow_db_location):
    """Sets up a realistic Orquestra project directory under a temporary path, with
    a`./workflow_defs.py` inside."""
    (tmp_path / "workflow_defs.py").write_text(SAMPLE_WORKFLOW_DEFS_FILE)
    return tmp_path


BASE_PATH = Path(__file__).parent.parent.parent


def _examples_dir(name):
    path = (
        BASE_PATH.parent / "src" / "orquestra" / "sdk" / "examples" / name
    ).resolve()
    if path.is_dir():
        return str(path)

    raise NotADirectoryError(f"No such directory: {path}")


class CustomClass:
    """It's only purpose is to test pickles."""

    def __init__(self, name):
        self._name = name


LOCAL_RAY_ADDRESS = "local"
TEST_CONFIG_NAME = "ray"
TEST_CONFIG_LOCAL = "local"


@pytest.fixture(scope="module")
def setup_ray():
    with make_ray_conn() as ray_params:
        yield ray_params


@pytest.mark.slow
class TestCLIWithRay:
    """Tests the CLI behavior with a real Ray connection. To make the suite
    self-contained these tests start Ray on their own, making the tests process
    "Ray head node". See more info at:
    https://docs.ray.io/en/releases-1.9.2/starting-ray.html#starting-ray-on-a-single-machine
    """

    def test_submit_v2_workflow(self, setup_ray, mock_workflow_db_location):
        args = argparse.Namespace(
            workflow_def_name="hello_workflow",
            directory=_examples_dir("."),
            config=TEST_CONFIG_NAME,
            verbose=False,
            force=False,
        )
        response = action.orq_submit_workflow_def(args)
        assert isinstance(response, SubmitWorkflowDefResponse)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK
        assert response.workflow_runs is not None
        assert "Successfully submitted workflow." in response.meta.message

    class TestOrqGetWorkflowRun:
        @pytest.fixture()
        def submited_workflow(
            self,
            orq_project_dir_with_defs,
        ):
            project_dir = str(orq_project_dir_with_defs)
            # Submit a run
            submit_resp = action.orq_submit_workflow_def(
                argparse.Namespace(
                    workflow_def_name=None,
                    directory=project_dir,
                    config=TEST_CONFIG_NAME,
                    verbose=False,
                    force=False,
                )
            )
            return submit_resp

        def test_happy_path(
            self,
            orq_project_dir_with_defs,
            submited_workflow,
            setup_ray,
        ):

            project_dir = str(orq_project_dir_with_defs)

            assert submited_workflow.meta.success
            assert isinstance(submited_workflow, SubmitWorkflowDefResponse)
            run_id = submited_workflow.workflow_runs[0].id

            # Get the status
            response = action.orq_get_workflow_run(
                argparse.Namespace(
                    directory=project_dir,
                    workflow_run_id=run_id,
                    config=TEST_CONFIG_NAME,
                )
            )

            assert response.meta.success
            assert response.meta.code == ResponseStatusCode.OK
            assert isinstance(response, GetWorkflowRunResponse)
            assert response.workflow_runs[0].id == run_id
            assert response.workflow_runs[0].status.state in {
                State.SUCCEEDED,
                State.RUNNING,
            }

        def test_happy_path_no_config(
            self,
            tmp_path,
            monkeypatch,
            orq_project_dir_with_defs,
            submited_workflow,
            setup_ray,
        ):
            def _mock_get_config_dir(_):
                return tmp_path

            monkeypatch.setattr(
                v2_config, "_get_config_directory", _mock_get_config_dir
            )

            project_dir = str(orq_project_dir_with_defs)
            run_id = submited_workflow.workflow_runs[0].id

            # Get the status
            response = action.orq_get_workflow_run(
                argparse.Namespace(
                    directory=project_dir,
                    workflow_run_id=run_id,
                    config=TEST_CONFIG_LOCAL,
                )
            )

            assert response.meta.success
            assert response.meta.code == ResponseStatusCode.OK
            assert isinstance(response, GetWorkflowRunResponse)
            assert response.workflow_runs[0].id == run_id
            assert response.workflow_runs[0].status.state in {
                State.SUCCEEDED,
                State.RUNNING,
            }

        def test_no_run(self, orq_project_dir_with_defs, setup_ray):
            test_run_id = "hello"
            with pytest.raises(WorkflowNotFoundError) as exc_info:
                action.orq_get_workflow_run(
                    argparse.Namespace(
                        output_format=ResponseFormat.JSON,
                        directory=str(orq_project_dir_with_defs),
                        workflow_run_id=test_run_id,
                        config=TEST_CONFIG_NAME,
                    )
                )
            assert f"Workflow run with ID {test_run_id} not found" in str(exc_info)

    class TestOrqGetWorkflowRunAll:
        def test_happy_path(self, orq_project_dir_with_defs, setup_ray):
            project_dir = str(orq_project_dir_with_defs)

            # 1. Submit two runs
            run_ids = []
            for run_i in range(2):
                submit_resp = action.orq_submit_workflow_def(
                    argparse.Namespace(
                        workflow_def_name=None,
                        directory=project_dir,
                        config=TEST_CONFIG_NAME,
                        verbose=False,
                        force=False,
                    )
                )

                assert submit_resp.meta.success
                assert isinstance(submit_resp, SubmitWorkflowDefResponse)
                run_ids.append(submit_resp.workflow_runs[0].id)

            # 2. Get status for all runs
            response = action.orq_get_workflow_run(
                argparse.Namespace(
                    directory=project_dir,
                    workflow_run_id=None,
                    config=TEST_CONFIG_NAME,
                )
            )
            assert response.meta.success
            assert response.meta.code == ResponseStatusCode.OK

            assert isinstance(response, GetWorkflowRunResponse)
            for run_id in run_ids:
                assert run_id in [wf_run.id for wf_run in response.workflow_runs]

    @pytest.mark.parametrize(
        "run_outputs",
        [
            [],
            [None],
            ["hello, world"],
            ["hello", "world"],
            [{}, {"general": "kenobi"}],
        ],
    )
    def test_get_workflow_v2_results_for_jsonable_outputs(
        self, tmp_path, run_outputs, monkeypatch, setup_ray
    ):
        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_workflow_run_outputs_non_blocking",
            MagicMock(return_value=run_outputs),
        )

        run_id = "a_wf_run_id"
        args = argparse.Namespace(
            workflow_run_id=run_id,
            directory=tmp_path,
            config=TEST_CONFIG_NAME,
        )
        response = action.orq_get_workflow_run_results(args)

        assert isinstance(response, GetWorkflowRunResultsResponse)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK
        assert response.workflow_run_id == run_id
        assert len(response.workflow_results) == len(run_outputs)
        for result in response.workflow_results:
            assert result.serialization_format == ArtifactFormat.JSON

    def test_get_workflow_v2_results_for_jsonable_outputs_no_config(
        self, tmp_path, monkeypatch, setup_ray
    ):
        def _mock_get_config_dir(_):
            return tmp_path

        monkeypatch.setattr(v2_config, "_get_config_directory", _mock_get_config_dir)

        run_output = ["hello, world"]
        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_workflow_run_outputs_non_blocking",
            MagicMock(return_value=run_output),
        )

        run_id = "a_wf_run_id"
        args = argparse.Namespace(
            workflow_run_id=run_id,
            directory=tmp_path,
            config=TEST_CONFIG_LOCAL,
        )
        response = action.orq_get_workflow_run_results(args)

        assert isinstance(response, GetWorkflowRunResultsResponse)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK
        assert len(response.workflow_results) == len(run_output)
        for result in response.workflow_results:
            assert result.serialization_format == ArtifactFormat.JSON

    @pytest.mark.parametrize(
        "run_outputs",
        [
            [set(["a set", "isn't json-serializable"])],
            [CustomClass("neither is"), CustomClass("a custom class")],
        ],
    )
    def test_get_workflow_v2_results_for_nonjsonable_outputs(
        self, tmp_path, run_outputs, monkeypatch, setup_ray
    ):
        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_workflow_run_outputs_non_blocking",
            MagicMock(return_value=run_outputs),
        )

        run_id = "a_wf_run_id"
        args = argparse.Namespace(
            workflow_run_id=run_id,
            directory=tmp_path,
            config=TEST_CONFIG_NAME,
        )
        response = action.orq_get_workflow_run_results(args)

        assert isinstance(response, GetWorkflowRunResultsResponse)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK
        assert response.workflow_run_id == run_id
        assert len(response.workflow_results) == len(run_outputs)
        for result in response.workflow_results:
            assert result.serialization_format == ArtifactFormat.ENCODED_PICKLE

    @pytest.mark.parametrize(
        "run_outputs, task_id, expected, raises",
        [
            ({"hello": "world"}, None, {"hello": "world"}, do_not_raise()),
            (
                {"hello": "world", "2nd": "object"},
                None,
                {"hello": "world", "2nd": "object"},
                do_not_raise(),
            ),
            (
                {"hello": "world", "2nd": "object"},
                "2nd",
                {"2nd": "object"},
                do_not_raise(),
            ),
            (
                {"hello": "world", "2nd": "object"},
                "NonExisting",
                None,
                pytest.raises(InvalidTaskIDError),
            ),
        ],
    )
    def test_get_artifacts(
        self,
        tmp_path,
        monkeypatch,
        run_outputs,
        task_id,
        expected,
        raises,
        setup_ray,
    ):
        def _mock_get_config_dir(_):
            return tmp_path

        monkeypatch.setattr(v2_config, "_get_config_directory", _mock_get_config_dir)

        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_available_outputs",
            MagicMock(return_value=run_outputs),
        )

        run_id = "a_wf_run_id"
        args = argparse.Namespace(
            workflow_run_id=run_id,
            directory=tmp_path,
            config=TEST_CONFIG_LOCAL,
            task_id=task_id,
        )
        with raises:
            response = action.orq_get_artifacts(args)
            assert response.meta.success
            assert response.meta.code == ResponseStatusCode.OK
            assert response.artifacts == expected

    @pytest.mark.parametrize("run_id", [None, "invalid_id"])
    def test_get_workflow_v2_results_for_invalid_run_id(
        self, run_id, tmp_path, monkeypatch, setup_ray
    ):
        def _mock_get_outputs(self, *args, **kwargs):
            raise ValueError()

        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_workflow_run_outputs_non_blocking",
            _mock_get_outputs,
        )

        args = argparse.Namespace(
            workflow_run_id=run_id,
            directory=tmp_path,
            config=TEST_CONFIG_NAME,
        )
        with pytest.raises(InvalidWorkflowRunError) as exc_info:
            action.orq_get_workflow_run_results(args)
        assert "Invalid workflow run id" in str(exc_info)
        assert str(run_id) in str(exc_info)

    def test_orq_stop_workflow_run_no_run(self, tmp_path, setup_ray):
        run_id: str = "hello"
        args = argparse.Namespace(
            output_format=ResponseFormat.JSON,
            directory=tmp_path,
            workflow_run_id=run_id,
            config=TEST_CONFIG_NAME,
        )
        with pytest.raises(_dag.exceptions.NotFoundError):
            action.orq_stop_workflow_run(args)

    def test_orq_stop_workflow_run_no_config(self, tmp_path, monkeypatch, setup_ray):
        def _mock_get_config_dir(_):
            return tmp_path

        monkeypatch.setattr(v2_config, "_get_config_directory", _mock_get_config_dir)

        def run(self, *args, **kwargs):
            return WorkflowRun(
                id="hello-1",
                workflow_def=TINY_WORKFLOW_DEF,
                task_runs=[],
                status=RunStatus(
                    state=State.RUNNING,
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                ),
            )

        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_workflow_run_status",
            run,
        )

        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "stop_workflow_run",
            value=callable,
        )

        args = argparse.Namespace(
            directory=tmp_path,
            workflow_run_id="hello-1",
            config=TEST_CONFIG_LOCAL,
        )

        # TODO: remove this
        tmp_path.joinpath(".orquestra", "workflow_runs", "hello-1").mkdir(parents=True)
        response = action.orq_stop_workflow_run(args)
        assert isinstance(response, StopWorkflowRunResponse)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK
        assert "Successfully terminated workflow." in response.meta.message

    def test_orq_stop_workflow_run(self, tmp_path, monkeypatch, setup_ray):
        def run(self, *args, **kwargs):
            return WorkflowRun(
                id="hello-1",
                workflow_def=TINY_WORKFLOW_DEF,
                task_runs=[],
                status=RunStatus(
                    state=State.RUNNING,
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                ),
            )

        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "get_workflow_run_status",
            run,
        )

        monkeypatch.setattr(
            orquestra.sdk._ray._dag.RayRuntime,
            "stop_workflow_run",
            value=callable,
        )

        args = argparse.Namespace(
            directory=tmp_path,
            workflow_run_id="hello-1",
            config=TEST_CONFIG_NAME,
        )

        # TODO: remove this
        tmp_path.joinpath(".orquestra", "workflow_runs", "hello-1").mkdir(parents=True)
        response = action.orq_stop_workflow_run(args)
        assert isinstance(response, StopWorkflowRunResponse)
        assert response.meta.success
        assert response.meta.code == ResponseStatusCode.OK
        assert "Successfully terminated workflow." in response.meta.message

    class TestListWorkflowRuns:
        def _populate_db(self, ids: list, config_names: t.Optional[list] = None):
            if not config_names:
                config_names = ["local" for _ in ids]
            assert len(ids) == len(config_names)

            with _db.WorkflowDB.open_db() as db:
                for id, config_name in zip(ids, config_names):
                    db.save_workflow_run(
                        StoredWorkflowRun(
                            workflow_run_id=id,
                            config_name=config_name,
                            workflow_def=TINY_WORKFLOW_DEF,
                        ),
                    )

        def _run(self, run_id, *args, **kwargs):
            start_time = datetime.now(timezone.utc)
            if "wf_age:" in run_id:
                _, wf_age = [run_info.strip() for run_info in run_id.split("wf_age:")]
                start_time -= _parse_age_clarg(
                    parser=argparse.ArgumentParser(), age=wf_age
                )

            return WorkflowRun(
                id=run_id,
                workflow_def=TINY_WORKFLOW_DEF,
                task_runs=[],
                status=RunStatus(
                    state=State.RUNNING,
                    start_time=start_time,
                    end_time=datetime.now(timezone.utc),
                ),
            )

        def _skip_run(self, run_id, *args, **kwargs):
            if run_id == "SKIP ME":
                raise NotFoundError
            return self._run(run_id, *args, **kwargs)

        def test_gets_all_runs(
            self,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
        ):
            run_ids = ["hello", "there", "general", "kenobi"]
            self._populate_db(run_ids)
            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                self._run,
            )
            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix=None,
                max_age=None,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=True,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert (
                f"Found {len(run_ids)} matching workflow runs." in response.meta.message
            )
            assert sorted(run_ids) == sorted([run.id for run in response.workflow_runs])

        def test_gets_all_available_runs(
            self,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
        ):
            run_ids = ["hello", "there", "general", "kenobi", "new_one", "SKIP ME"]
            self._populate_db(run_ids)
            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                self._skip_run,
            )
            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix=None,
                max_age=None,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=True,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert (
                f"Found {len(run_ids) - 1} matching workflow runs."
                in response.meta.message
            )
            assert "SKIP ME" not in [run.id for run in response.workflow_runs]

        def test_gets_limited_runs(
            self,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
        ):
            run_ids = [
                "hello wf_age: 4s",
                "there wf_age: 3s",
                "general wf_age: 2s",
                "kenobi wf_age: 1s",
            ]
            limit = 2
            self._populate_db(run_ids)
            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                self._run,
            )
            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=limit,
                prefix=None,
                max_age=None,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=False,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert f"Found {limit} matching workflow runs." in response.meta.message
            assert run_ids[::-1][:limit] == [run.id for run in response.workflow_runs]

        def test_filter_by_prefix(
            self,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
        ):
            valid_run_ids = [
                "hello",
                "HeLlO",
                "HELLO",
                "hello_there",
                "helloworld",
                "helloDarknessMyOldFriend",
            ]
            invalid_run_ids = ["hell_o", "you_say_goodbye_i_say_hello"]
            self._populate_db(valid_run_ids + invalid_run_ids)
            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                self._run,
            )
            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix="hello",
                max_age=None,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=False,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert (
                f"Found {len(valid_run_ids)} matching workflow runs."
                in response.meta.message
            )
            assert sorted(valid_run_ids) == sorted(
                [run.id for run in response.workflow_runs]
            )

        @pytest.mark.parametrize(
            ("config_param", "all_configs_param", "expected_wfs"),
            [
                ("local", False, 5),
                (None, False, 5),
                (None, True, 10),
                ("local", True, 10),
                ("fake", False, 5),  # if the config doesn't exist - use default
            ],
            ids=[
                "set config",
                "use default config",
                "set all",
                "set config and all",
                "set fake config",
            ],
        )
        def test_config_names(
            self,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
            config_param,
            all_configs_param,
            expected_wfs,
        ):
            valid_config_name = "local"

            valid_run_ids = ["lorem", "ipsum", "dolor", "sit", "amet"]
            valid_run_configs = [valid_config_name] * len(valid_run_ids)
            invalid_run_ids = ["qui", "totam", "molestiae", "id", "exercitationem"]
            invalid_run_configs = ["mollit", "anim", "id", "est", "laborum"]
            self._populate_db(
                valid_run_ids + invalid_run_ids,
                config_names=valid_run_configs + invalid_run_configs,
            )
            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                self._run,
            )
            monkeypatch.setattr(
                v2_config,
                "read_config",
                lambda _: RuntimeConfiguration(
                    config_name="local",
                    runtime_name=RuntimeName.RAY_LOCAL,
                    runtime_options={
                        "address": "auto",
                        "log_to_driver": False,
                        "storage": None,
                        "temp_dir": None,
                    },
                ),
            ),

            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix=None,
                max_age=None,
                status=None,
                config=config_param,
                additional_project_dirs=[],
                all=all_configs_param,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert (
                f"Found {expected_wfs} matching workflow runs." in response.meta.message
            )

        @pytest.mark.parametrize(
            "max_age",
            [
                timedelta(minutes=1),
                timedelta(hours=1),
                timedelta(days=1),
            ],
        )
        def test_filter_by_max_age(
            self,
            max_age,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
        ):
            amount_workflows = 10
            valid_run_ids = [f"young_{n}" for n in range(amount_workflows)]
            invalid_run_ids = [f"old_{n}" for n in range(amount_workflows)]
            self._populate_db(valid_run_ids + invalid_run_ids)

            def _run_with_age_based_on_id(self, run_id: str, *args, **kwargs):
                now = datetime.now(timezone.utc)
                if run_id.startswith("young"):
                    start_time = (
                        now
                        - max_age
                        * (amount_workflows - int(run_id.replace("young_", "")) - 1)
                        / amount_workflows
                    )
                else:
                    start_time = now - max_age * (
                        amount_workflows - int(run_id.replace("old_", "")) + 1
                    )
                return WorkflowRun(
                    id=run_id,
                    workflow_def=TINY_WORKFLOW_DEF,
                    task_runs=[],
                    status=RunStatus(
                        state=State.RUNNING,
                        start_time=start_time,
                        end_time=now,
                    ),
                )

            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                _run_with_age_based_on_id,
            )
            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix=None,
                max_age=max_age,
                status=None,
                config=None,
                additional_project_dirs=[],
                all=False,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert (
                f"Found {len(valid_run_ids)} matching workflow runs."
                in response.meta.message
            )
            assert sorted(valid_run_ids) == sorted(
                [run.id for run in response.workflow_runs]
            )

        def test_filter_by_status(
            self,
            tmp_path,
            mock_workflow_db_location,
            monkeypatch,
            setup_ray,
        ):
            valid_run_ids = [f"running_{n}" for n in range(10)]
            invalid_run_ids = [f"failed_{n}" for n in range(10)]
            self._populate_db(valid_run_ids + invalid_run_ids)

            def _run_with_status_based_on_id(self, run_id: str, *args, **kwargs):
                if run_id.startswith("running"):
                    state = State.RUNNING
                else:
                    state = State.FAILED
                return WorkflowRun(
                    id=run_id,
                    workflow_def=TINY_WORKFLOW_DEF,
                    task_runs=[],
                    status=RunStatus(
                        state=state,
                        start_time=datetime.now(timezone.utc),
                        end_time=datetime.now(timezone.utc),
                    ),
                )

            monkeypatch.setattr(
                orquestra.sdk._ray._dag.RayRuntime,
                "get_workflow_run_status",
                _run_with_status_based_on_id,
            )
            args = argparse.Namespace(
                directory=str(tmp_path.absolute()),
                limit=None,
                prefix=None,
                max_age=None,
                status=State.RUNNING,
                config=None,
                additional_project_dirs=[],
                all=False,
            )

            response = action.orq_list_workflow_runs(args)

            assert response.meta.success
            assert (
                f"Found {len(valid_run_ids)} matching workflow runs."
                in response.meta.message
            )
            assert sorted(valid_run_ids) == sorted(
                [run.id for run in response.workflow_runs]
            )


def _setup_runtime_mock(monkeypatch, runtime):
    monkeypatch.setattr(
        orquestra.sdk._ray._dag.RayRuntime,
        "from_runtime_configuration",
        Mock(return_value=runtime),
    )


class TestCLIAgainstRuntimeMock:
    """
    Boundaries of the system-under-test: [CLI action]-[RayRuntime].
    """

    class TestGetLogs:
        """
        Tests for the 'orq get logs' action.
        """

        @pytest.fixture
        def mocked_nonsense_config(self, monkeypatch):
            """
            Mocks reading config. The values are non-sensical, but it doesn't require
            Ray connection (contrary to ``patch_config_for_ray()``).
            """
            monkeypatch.setattr(
                v2_config,
                "read_config",
                Mock(
                    return_value=RuntimeConfiguration(
                        config_name=TEST_CONFIG_NAME,
                        runtime_name=RuntimeName.RAY_LOCAL,
                        runtime_options={
                            "address": "shouldnt-matter",
                            "log_to_driver": True,
                            "storage": "shouldnt-matter",
                            "_temp_dir": "shouldnt-matter",
                        },
                    )
                ),
            )

        @pytest.mark.parametrize("query_by_run_id", [True, False])
        def test_follow(
            self,
            tmp_path,
            monkeypatch,
            capsys,
            mocked_nonsense_config,
            query_by_run_id: bool,
        ):
            """
            Validates that when RayRuntime.iter_logs() yields something, it gets
            printed to stdout.
            """
            # Given
            wf_run_id = "hello_there_1234"
            tell_tale = "general kenobi!"

            runtime_mock = Mock()

            def _mock_iter_logs(run_id):
                if run_id in {None, wf_run_id}:
                    return [[tell_tale]]
                else:
                    return []

            runtime_mock.iter_logs = _mock_iter_logs

            _setup_runtime_mock(monkeypatch, runtime_mock)

            # When
            query_id = wf_run_id if query_by_run_id else None
            action.orq_get_logs(
                argparse.Namespace(
                    workflow_or_task_run_id=query_id,
                    follow=True,
                    output_format=ResponseFormat.PLAIN_TEXT,
                    directory=tmp_path,
                    config=TEST_CONFIG_NAME,
                )
            )

            # Then
            captured = capsys.readouterr()
            assert tell_tale in captured.out
            assert "Ctrl-C" in captured.err

        @pytest.mark.parametrize("query_by_run_id", [True, False])
        def test_historical(
            self,
            tmp_path,
            monkeypatch,
            capsys,
            mocked_nonsense_config,
            query_by_run_id: bool,
        ):
            """
            Validates that when RayRuntime.get_full_logs() returns something, it's
            returned in the response.
            """
            # Given
            wf_run_id = "hello_there_1234"
            tell_tale = "general kenobi!"

            runtime_mock = Mock()

            def _mock_get_full_logs(run_id):
                if run_id in {None, wf_run_id}:
                    return {wf_run_id: [tell_tale]}
                else:
                    return {}

            runtime_mock.get_full_logs = _mock_get_full_logs
            _setup_runtime_mock(monkeypatch, runtime_mock)

            # When
            query_id = wf_run_id if query_by_run_id else None
            response = action.orq_get_logs(
                argparse.Namespace(
                    workflow_or_task_run_id=query_id,
                    follow=False,
                    output_format=ResponseFormat.PLAIN_TEXT,
                    directory=tmp_path,
                    config=TEST_CONFIG_NAME,
                )
            )

            # Then
            captured = capsys.readouterr()

            # The logs should be in the response
            assert isinstance(response, GetLogsResponse)
            assert tell_tale in response.logs

            # We should print nothing
            assert len(captured.out) == 0
            assert len(captured.err) == 0


class TestCLIWithRayFailures:
    """Tests the CLI behavior when connection to ray fails."""

    @pytest.fixture
    def patch_config(self, monkeypatch):
        monkeypatch.setattr(
            v2_config,
            "read_config",
            lambda _: RuntimeConfiguration(
                config_name=TEST_CONFIG_NAME,
                runtime_name=RuntimeName.RAY_LOCAL,
                runtime_options={
                    "address": LOCAL_RAY_ADDRESS,
                    "log_to_driver": False,
                    "storage": None,
                    "temp_dir": None,
                },
            ),
        )

    def test_ray_connection_failure(self, tmp_path, monkeypatch, capsys, patch_config):
        tell_tale = "Testing Ray Failure"

        def _error(*args, **kwargs):
            raise ConnectionError(tell_tale)

        monkeypatch.setattr(
            _dag.RayRuntime,
            "startup",
            _error,
        )
        args = argparse.Namespace(
            output_format=ResponseFormat.JSON,
            directory=tmp_path.absolute(),
            workflow_run_id=None,
            config=TEST_CONFIG_NAME,
        )

        # TODO: remove this
        tmp_path.joinpath(".orquestra", "workflow_runs").mkdir(parents=True)

        with pytest.raises(ConnectionError) as exc_info:
            action.orq_get_workflow_run(args)
        assert tell_tale in str(exc_info)
