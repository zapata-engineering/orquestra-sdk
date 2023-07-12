################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
RuntimeInterface implementation that uses Ray DAG/Ray Core API.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import re
import typing as t
import warnings
from datetime import timedelta
from pathlib import Path

import pydantic

from orquestra.sdk.schema.responses import WorkflowResult

from .. import exceptions
from .._base import _dates, _services, serde
from .._base._db import WorkflowDB
from .._base._env import RAY_GLOBAL_WF_RUN_ID_ENV
from .._base._logs._interfaces import LogReader
from .._base._spaces._structs import ProjectRef
from .._base.abc import RuntimeInterface
from ..schema import ir
from ..schema.configs import RuntimeConfiguration
from ..schema.local_database import StoredWorkflowRun
from ..schema.workflow_run import (
    ProjectId,
    RunStatus,
    State,
    TaskInvocationId,
    TaskRun,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
    WorkspaceId,
)
from . import _client, _id_gen, _ray_logs
from ._build_workflow import TaskResult, make_ray_dag
from ._client import RayClient
from ._wf_metadata import InvUserMetadata, WfUserMetadata, pydatic_to_json_dict


def _instant_from_timestamp(
    unix_timestamp: t.Optional[float],
) -> t.Optional[_dates.Instant]:
    if unix_timestamp is None:
        return None
    return _dates.from_unix_time(unix_timestamp)


def _generate_wf_run_id(wf_def: ir.WorkflowDef):
    """
    Implements the "tagging" design doc:
    https://zapatacomputing.atlassian.net/wiki/spaces/ORQSRUN/pages/479920161/Logging+Tagging

    Assumed to be globally unique.

    Example value: "wf.multioutput_wf.91aa7aa"
    """
    wf_name = wf_def.name
    hex_str = _id_gen.gen_short_uid(char_length=7)

    return f"wf.{wf_name}.{hex_str}"


if _client.WorkflowStatus is not None:
    RAY_ORQ_STATUS = {
        _client.WorkflowStatus.RUNNING: State.RUNNING,
        _client.WorkflowStatus.CANCELED: State.TERMINATED,
        _client.WorkflowStatus.SUCCESSFUL: State.SUCCEEDED,
        _client.WorkflowStatus.FAILED: State.FAILED,
        _client.WorkflowStatus.RESUMABLE: State.FAILED,
        _client.WorkflowStatus.PENDING: State.WAITING,
    }
else:
    RAY_ORQ_STATUS = {}


def _task_state_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> State:
    """
    Heuristic to figure out SDK task run state from Ray's workflow status and times.

    Note that `wf_status` is status of the whole workflow, but this procedure
    is meant to be applied for tasks.
    """
    if wf_status == _client.WorkflowStatus.RESUMABLE:
        return State.FAILED
    elif wf_status == _client.WorkflowStatus.CANCELED:
        return State.TERMINATED

    if start_time:
        if end_time:
            return State.SUCCEEDED
        elif wf_status == _client.WorkflowStatus.FAILED:
            return State.FAILED

    if not start_time:
        return State.WAITING

    return RAY_ORQ_STATUS[wf_status]


def _workflow_state_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> State:
    if start_time and end_time and wf_status == _client.WorkflowStatus.RUNNING:
        # If we ask Ray right after a workflow has been completed, Ray reports
        # workflow status as "RUNNING". This happens even after we await Ray
        # workflow completion via 'ray.get()'.
        return State.SUCCEEDED

    if not start_time and not end_time and wf_status == _client.WorkflowStatus.RUNNING:
        # In theory this case shouldn't happen, but experience shows that Ray
        # has sometime problems with race conditions in status reporting. This
        # is a defensive 'if' that allows us to be certain that whenever our
        # workflow run is "running", it always has start_time.
        return State.WAITING

    return RAY_ORQ_STATUS[wf_status]


def _task_status_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> RunStatus:
    return RunStatus(
        state=_task_state_from_ray_meta(
            wf_status=wf_status,
            start_time=start_time,
            end_time=end_time,
        ),
        start_time=_instant_from_timestamp(start_time),
        end_time=_instant_from_timestamp(end_time),
    )


def _workflow_status_from_ray_meta(
    wf_status: _client.WorkflowStatus,
    start_time: t.Optional[float],
    end_time: t.Optional[float],
) -> RunStatus:
    return RunStatus(
        state=_workflow_state_from_ray_meta(
            wf_status=wf_status,
            start_time=start_time,
            end_time=end_time,
        ),
        start_time=_instant_from_timestamp(start_time),
        end_time=_instant_from_timestamp(end_time),
    )


@dataclasses.dataclass(frozen=True)
class RayParams:
    """Parameters we pass to Ray. See Ray documentation for reference of what values are
    possible and what they do:
    - https://docs.ray.io/en/latest/package-ref.html#ray-init
    - https://docs.ray.io/en/latest/workflows/package-ref.html#ray.workflow.init
    """

    address: t.Optional[str] = None
    log_to_driver: bool = True
    storage: t.Optional[t.Union[str, _client.Storage]] = None
    _temp_dir: t.Optional[str] = None
    configure_logging: bool = True


# Defensive timeout for Ray operations that are expected to be instant.
# Sometimes, Ray behaves unintuitively.
JUST_IN_CASE_TIMEOUT = 10.0


class RayRuntime(RuntimeInterface):
    def __init__(
        self,
        config: RuntimeConfiguration,
        project_dir: Path,
        client: t.Optional[RayClient] = None,
    ):
        self._client = client or RayClient()

        ray_params = RayParams(
            address=config.runtime_options["address"],
            log_to_driver=config.runtime_options["log_to_driver"],
            storage=config.runtime_options["storage"],
            _temp_dir=config.runtime_options["temp_dir"],
            configure_logging=config.runtime_options["configure_logging"],
        )
        self.startup(ray_params)

        self._config = config
        self._project_dir = project_dir

        self._log_reader: LogReader = _ray_logs.DirectRayReader(
            _services.ray_temp_path()
        )

    @classmethod
    def startup(cls, ray_params: RayParams):
        """
        Initialize a global Ray connection. If you need a separate connection
        with different params, you need to call .shutdown first(). Globals
        suck.

        This operation is idempotent – calling it multiple times with the same
        arguments has the same effect as calling it once.

        Args:
            ray_params: connection params. Most notable field: 'address' –
                depending on the value, a new Ray cluster will be spawned, or
                we'll just connect to an already existing one.

        Raises:
            exceptions.RayActorNameClashError: when multiple Ray actors exist with the
                same name.
        """

        # Turn off internal Ray logs, unless there is an error
        # If Ray is set to configure logging, this will be overridden
        logger = logging.getLogger("ray")
        logger.setLevel(logging.ERROR)

        client = RayClient()
        try:
            client.init(**dataclasses.asdict(ray_params))
        except ConnectionError as e:
            raise exceptions.RayNotRunningError from e

        try:
            client.workflow_init()
        except ValueError as e:
            message = (
                "This is likely due to a race condition in starting the local "
                "runtime. Please try again."
            )
            # NOTE: We check for the specific case of an actor name clash before
            # raising RayActorNameClashError. If it's anything else we still want
            # to advise the user that it's a probable race condition.
            if re.findall(
                r"Actor with name|already exists in the namespace workflow",
                str(e),
                re.IGNORECASE,
            ):
                raise exceptions.RayActorNameClashError(message) from e
            else:
                raise ValueError(message) from e

    @classmethod
    def shutdown(cls):
        """Clean up Ray connection. Call it if you want to connect to Ray with different
        parameters.

        Safe to call multiple times in a row.
        """
        client = RayClient()
        client.shutdown()

    def create_workflow_run(
        self, workflow_def: ir.WorkflowDef, project: t.Optional[ProjectRef]
    ) -> WorkflowRunId:
        if project:
            warnings.warn(
                "Ray doesn't support project-scoped workflows. "
                "Project and workspace IDs will be ignored.",
                category=exceptions.UnsupportedRuntimeFeature,
            )

        global_run_id = os.getenv(RAY_GLOBAL_WF_RUN_ID_ENV)
        wf_run_id = global_run_id or _generate_wf_run_id(workflow_def)

        dag = make_ray_dag(
            self._client,
            workflow_def=workflow_def,
            workflow_run_id=wf_run_id,
            project_dir=self._project_dir,
        )
        wf_user_metadata = WfUserMetadata(workflow_def=workflow_def)

        # Unfortunately, Ray doesn't validate uniqueness of workflow IDs. Let's
        # hope we won't get a collision.
        _ = self._client.run_dag_async(
            dag,
            workflow_id=wf_run_id,
            metadata=pydatic_to_json_dict(wf_user_metadata),
        )

        wf_run = StoredWorkflowRun(
            workflow_run_id=wf_run_id,
            config_name=self._config.config_name,
            workflow_def=workflow_def,
            is_qe=False,
        )
        with WorkflowDB.open_project_db(self._project_dir) as db:
            db.save_workflow_run(wf_run)

        return wf_run_id

    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError
        """
        try:
            wf_status = self._client.get_workflow_status(workflow_id=workflow_run_id)
            wf_meta = self._client.get_workflow_metadata(workflow_id=workflow_run_id)
        except (_client.workflow_exceptions.WorkflowNotFoundError, ValueError) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run {workflow_run_id} wasn't found"
            ) from e

        wf_user_metadata = WfUserMetadata.parse_obj(wf_meta["user_metadata"])
        wf_def = wf_user_metadata.workflow_def

        inv_ids = wf_def.task_invocations.keys()
        # We assume that:
        # - create_workflow_run() created a separate Ray Task for each IR's
        #   TaskInvocation
        # - each Ray Task's name was set to TaskInvocation.id
        ray_task_metas = [
            self._client.get_task_metadata(workflow_id=workflow_run_id, name=inv_id)
            for inv_id in inv_ids
        ]

        return WorkflowRun(
            id=workflow_run_id,
            workflow_def=wf_def,
            task_runs=[
                TaskRun(
                    id=task_meta["user_metadata"]["task_run_id"],
                    invocation_id=task_meta["user_metadata"]["task_invocation_id"],
                    status=_task_status_from_ray_meta(
                        wf_status=wf_status,
                        start_time=task_meta["stats"].get("start_time"),
                        end_time=task_meta["stats"].get("end_time"),
                    ),
                    message=None,
                )
                for task_meta in ray_task_metas
            ],
            status=_workflow_status_from_ray_meta(
                wf_status=wf_status,
                start_time=wf_meta["stats"].get("start_time"),
                end_time=wf_meta["stats"].get("end_time"),
            ),
        )

    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Sequence[t.Any]:
        try:
            workflow_status = self.get_workflow_run_status(workflow_run_id)
        except exceptions.WorkflowRunNotFoundError:
            raise

        workflow_state = workflow_status.status.state
        if workflow_state != State.SUCCEEDED:
            raise exceptions.WorkflowRunNotSucceeded(
                f"{workflow_run_id} has not succeeded. {workflow_state}",
                workflow_state,
            )

        # By this line we're assuming the workflow run exists, otherwise we wouldn't get
        # its status. If the following line raises errors we treat them as unexpected.
        ray_result = self._client.get_workflow_output(workflow_run_id)

        if isinstance(ray_result, TaskResult):
            # If we have a TaskResult, we're a >=0.47.0 result
            # We can assume this is pre-seralised in the form:
            # tuple(WorkflowResult, ...)
            return ray_result.unpacked
        else:
            # If we have anything else, this should be a tuple of objects
            # These are returned values from workflow tasks
            assert isinstance(ray_result, tuple)
            # In >=0.47.0, the values are serialised.
            # So, we have to serialise them here.
            return tuple(
                serde.result_from_artifact(r, ir.ArtifactFormat.AUTO)
                for r in ray_result
            )

    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> t.Dict[ir.TaskInvocationId, WorkflowResult]:
        """
        Raises:
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: if no run
                with `workflow_run_id` was found.
        """
        # The approach is based on two steps:
        # 1. Get task run status.
        # 2. Ask Ray for outputs of only the succeeded ones.
        #
        # It would be nice to only ask Ray only once to get all outputs, async,
        # catch errors for the tasks that haven't been finished yet, and return
        # the available outputs. At the time of writing this (ray 2.0.0), it
        # isn't possible. Ray always blocks for workflow completion.
        try:
            wf_run = self.get_workflow_run_status(workflow_run_id)
        except exceptions.WorkflowRunNotFoundError as e:
            # Explicitly re-raise.
            raise e

        # Note: matching task invocations with other objects down below relies on the
        # sequence order.
        succeeded_inv_ids: t.Sequence[ir.TaskInvocationId] = [
            run.invocation_id
            for run in wf_run.task_runs
            if run.status.state == State.SUCCEEDED
        ]

        succeeded_obj_refs: t.List[_client.ObjectRef] = [
            self._client.get_task_output_async(
                workflow_id=workflow_run_id,
                # We rely on the fact that _make_ray_dag() assigns invocation
                # ID as the Ray task name.
                task_id=inv_id,
            )
            for inv_id in succeeded_inv_ids
        ]

        # The values are supposed to be ready to get, but we're using a timeout
        # to ensure we don't block indefinitely.
        succeeded_values: t.Sequence[t.Any] = self._client.get(
            succeeded_obj_refs, timeout=JUST_IN_CASE_TIMEOUT
        )

        # We need to check if the task output was a TaskResult or any other value.
        # A TaskResult means this is a >=0.47.0 workflow and there is a serialized
        # value (WorkflowResult) in TaskResult.packed
        # Anything else is a <0.47.0 workflow and the value should be serialized

        serialized_succeeded_values = [
            v.packed
            if isinstance(v, TaskResult)
            else serde.result_from_artifact(v, ir.ArtifactFormat.AUTO)
            for v in succeeded_values
        ]

        return dict(
            zip(
                succeeded_inv_ids,
                serialized_succeeded_values,
            )
        )

    def stop_workflow_run(
        self, workflow_run_id: WorkflowRunId, *, force: t.Optional[bool] = None
    ) -> None:
        # cancel doesn't throw exceptions on non-existing runs... using this as
        # a workaround to inform client of an error
        try:
            _ = self._client.get_workflow_status(workflow_id=workflow_run_id)
        except (_client.workflow_exceptions.WorkflowNotFoundError, ValueError) as e:
            raise exceptions.NotFoundError(
                f"Workflow run {workflow_run_id} wasn't found"
            ) from e
        self._client.cancel(workflow_run_id)

    def get_workflow_logs(self, wf_run_id: WorkflowRunId):
        return self._log_reader.get_workflow_logs(wf_run_id)

    def get_task_logs(self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
        return self._log_reader.get_task_logs(wf_run_id, task_inv_id)

    def list_workflow_runs(
        self,
        *,
        limit: t.Optional[int] = None,
        max_age: t.Optional[timedelta] = None,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
        workspace: t.Optional[WorkspaceId] = None,
        project: t.Optional[ProjectId] = None,
    ) -> t.List[WorkflowRun]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace. Not supported
                on this runtime.

        Returns:
                A list of the workflow runs
        """
        now = _dates.now()

        if state is not None:
            if not isinstance(state, list):
                state_list = [state]
            else:
                state_list = state
        else:
            state_list = None

        # Grab the workflows we know about from the DB
        all_workflows = self._client.list_all()

        wf_runs = []
        for wf_run_id, _ in all_workflows:
            try:
                wf_run = self.get_workflow_run_status(wf_run_id)
            except exceptions.NotFoundError:
                continue

            # Let's filter the workflows at this point, instead of iterating over a list
            # multiple times
            if state_list is not None and wf_run.status.state not in state_list:
                continue
            if max_age is not None and (
                now - (wf_run.status.start_time or now) > max_age
            ):
                continue
            wf_runs.append(wf_run)

        # We have to wait until we have all the workflow runs before sorting
        if limit is not None:
            wf_runs = sorted(wf_runs, key=lambda run: run.status.start_time or now)[
                -limit:
            ]
        return wf_runs

    def get_workflow_project(self, wf_run_id: WorkflowRunId):
        raise exceptions.WorkspacesNotSupportedError()
