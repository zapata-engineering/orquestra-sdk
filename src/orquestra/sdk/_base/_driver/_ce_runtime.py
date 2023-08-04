################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
RuntimeInterface implementation that uses Compute Engine.
"""
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

from orquestra.sdk import Project, ProjectRef, Workspace, exceptions
from orquestra.sdk._base import _retry, serde
from orquestra.sdk._base._db import WorkflowDB
from orquestra.sdk._base._logs import _regrouping
from orquestra.sdk._base._logs._interfaces import WorkflowLogs
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.kubernetes.quantity import parse_quantity
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.ir import ArtifactFormat, TaskInvocationId, WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.responses import ComputeEngineWorkflowResult, WorkflowResult
from orquestra.sdk.schema.workflow_run import (
    ProjectId,
    State,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
    WorkspaceId,
)

from . import _client, _exceptions, _models


def _get_max_resources(workflow_def: WorkflowDef) -> _models.Resources:
    max_gpu = None
    max_memory = None
    max_cpu = None
    for inv in workflow_def.task_invocations.values():
        if inv.resources is None:
            continue
        if inv.resources.memory is not None:
            if parse_quantity(inv.resources.memory) > parse_quantity(max_memory or "0"):
                max_memory = inv.resources.memory
        if inv.resources.cpu is not None:
            if parse_quantity(inv.resources.cpu) > parse_quantity(max_cpu or "0"):
                max_cpu = inv.resources.cpu
        if inv.resources.gpu is not None:
            if int(inv.resources.gpu) > int(max_gpu or "0"):
                max_gpu = inv.resources.gpu
    return _models.Resources(cpu=max_cpu, memory=max_memory, gpu=max_gpu, nodes=None)


def _verify_workflow_resources(
    wf_resources: _models.Resources, max_resources: _models.Resources
):
    # Default workflow CPU and memory defined on platform side, if Nones are passed
    # https://github.com/zapatacomputing/workflow-driver/blob/1d4d0552e44cfd11238b823cbf66a33d4d2e8593/pkg/config/config.go#L22  # noqa
    default_workflow_cpu = "2"
    default_workflow_memory = "2Gi"
    insufficient_resources = []
    if max_resources.cpu and parse_quantity(max_resources.cpu) > parse_quantity(
        wf_resources.cpu or default_workflow_cpu
    ):
        insufficient_resources.append("CPU")

    if max_resources.memory and parse_quantity(max_resources.memory) > parse_quantity(
        wf_resources.memory or default_workflow_memory
    ):
        insufficient_resources.append("Memory")

    if max_resources.gpu and int(max_resources.gpu) > int(wf_resources.gpu or "0"):
        insufficient_resources.append("GPU")

    if insufficient_resources:
        raise exceptions.WorkflowSyntaxError(
            "Following workflow resources are "
            "insufficient to schedule all tasks: "
            f"{insufficient_resources}."
            "Please increase those resources in "
            "workflow resources and try again."
        )


class CERuntime(RuntimeInterface):
    """
    A runtime for communicating with the Compute Engine API endpoints
    """

    def __init__(
        self,
        config: RuntimeConfiguration,
        client: _client.DriverClient,
        verbose: bool = False,
    ):
        """
        Args:
            config: contains the runtime configuration, including the name of the
                config being used and the associated runtime options. These options
                control how to connect to a CE cluster.
            verbose: if `True`, CERuntime may print debug information about
                its inner working to stderr.

        Raises:
            RuntimeConfigError: when the config is invalid
        """
        self._config = config
        self._verbose = verbose

        self._client = client

    def create_workflow_run(
        self, workflow_def: WorkflowDef, project: Optional[ProjectRef]
    ) -> WorkflowRunId:
        """
        Schedules a workflow definition for execution

        Args:
            workflow_def: the IR of the workflow to run
            project: Project dir (workspace and project ID) on which the workflow
            will be run
        Raises:
            WorkflowSyntaxError: when the workflow definition was rejected by the remote
                cluster
            WorkflowRunNotStarted: for all other errors that prevented the workflow run
                from being scheduled
            UnauthorizedError: if the current token was rejected by the remote cluster

        Returns:
            the workflow run ID
        """

        max_invocation_resources = _get_max_resources(workflow_def)

        if workflow_def.resources is not None:
            resources = _models.Resources(
                cpu=workflow_def.resources.cpu,
                memory=workflow_def.resources.memory,
                gpu=workflow_def.resources.gpu,
                nodes=workflow_def.resources.nodes,
            )
        else:
            resources = max_invocation_resources

        _verify_workflow_resources(resources, max_invocation_resources)

        try:
            workflow_def_id = self._client.create_workflow_def(workflow_def, project)

            workflow_run_id = self._client.create_workflow_run(
                workflow_def_id, resources
            )
        except _exceptions.InvalidWorkflowDef as e:
            raise exceptions.WorkflowSyntaxError(
                "Unable to start the workflow run "
                "- there are errors in the workflow definition."
            ) from e
        except _exceptions.UnsupportedSDKVersion as e:
            raise exceptions.WorkflowRunNotStarted(
                (
                    "This is an unsupported version of orquestra-sdk.\n{}{}"
                    'Try updating with `pip install -U "orquestra-sdk[all]"`'
                ).format(
                    ""
                    if e.submitted_version is None
                    else f" - Current version: {e.submitted_version}\n",
                    ""
                    if e.supported_versions is None
                    else f" - Supported versions: {e.supported_versions}\n",
                )
            ) from e
        except _exceptions.InvalidWorkflowRunRequest as e:
            raise exceptions.WorkflowRunNotStarted(
                "Unable to start the workflow run."
            ) from e
        except _exceptions.ForbiddenError as e:
            if project:
                raise exceptions.ProjectInvalidError(
                    f"Unable to start the workflow run "
                    f"invalid workspace: {project.workspace_id}"
                ) from e
            else:
                raise exceptions.UnauthorizedError(
                    "Unable to start the workflow run "
                ) from e
        except _exceptions.InvalidTokenError as e:
            raise exceptions.UnauthorizedError(
                "Unable to start the workflow run "
                "- the authorization token was rejected by the remote cluster."
            ) from e

        with WorkflowDB.open_db() as db:
            db.save_workflow_run(
                StoredWorkflowRun(
                    workflow_run_id=workflow_run_id,
                    config_name=self._config.config_name,
                    workflow_def=workflow_def,
                    is_qe=False,
                )
            )
        return workflow_run_id

    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """
        Gets the status of a workflow run

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            The status of the workflow run
        """
        try:
            return self._client.get_workflow_run(workflow_run_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.WorkflowRunNotFound) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                "Could not get the workflow status for run with id "
                f"`{workflow_run_id}` "
                "- the authorization token was rejected by the remote cluster."
            ) from e

    @_retry.retry(
        attempts=5,
        delay=0.2,
        allowed_exceptions=(exceptions.WorkflowResultsNotReadyError,),
    )
    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> Sequence[WorkflowResult]:
        """Non-blocking version of get_workflow_run_outputs.

        This method raises exceptions if the workflow output artifacts are not available

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            WorkflowRunNotSucceeded: if the workflow has not yet succeeded
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            the outputs associated with the workflow run
        """

        try:
            result_ids = self._client.get_workflow_run_results(workflow_run_id)
        except (
            _exceptions.InvalidWorkflowRunID,
            _exceptions.WorkflowRunNotFound,
        ) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                "Could not get the outputs for workflow run with id "
                f"`{workflow_run_id}` "
                "- the authorization token was rejected by the remote cluster."
            ) from e

        if len(result_ids) == 0:
            wf_run = self.get_workflow_run_status(workflow_run_id)
            if wf_run.status.state == State.SUCCEEDED:
                raise exceptions.WorkflowResultsNotReadyError(
                    f"Workflow run `{workflow_run_id}` has succeeded, but the results "
                    "are not ready yet.\n"
                    "After a workflow completes, there may be a short delay before the "
                    "results are ready to download. Please try again!"
                )
            else:
                raise exceptions.WorkflowRunNotSucceeded(
                    f"Workflow run `{workflow_run_id}` is in state "
                    f"{wf_run.status.state}",
                    wf_run.status.state,
                )

        assert len(result_ids) == 1, "Assuming a single output"

        try:
            result = self._client.get_workflow_run_result(result_ids[0])
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                "Could not get the outputs for workflow run with id "
                f"`{workflow_run_id}` "
                "- the authorization token was rejected by the remote cluster."
            ) from e

        if not isinstance(result, ComputeEngineWorkflowResult):
            # It's a WorkflowResult.
            # We need to match the old way of storing results into the new way
            # this is done by unpacking the deserialised values and re-serialising.
            # This is unfortunate, but should only happen for <0.47.0 workflow runs.
            # Example:
            #   We get JSONResult([100, "json_string"]) from the API
            #   We need (JSONResult(100), JSONResult("json_string"))
            deserialised_results = serde.deserialize(result)
            return tuple(
                serde.result_from_artifact(unpacked, ArtifactFormat.AUTO)
                for unpacked in deserialised_results
            )
        else:
            return result.results

    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> Dict[TaskInvocationId, WorkflowResult]:
        """Returns all available outputs for a workflow

        This method returns all available artifacts. When the workflow fails
        it returns artifacts only for the steps that did success.
        Might raise an exception if runtime doesn't support getting artifacts from
        in-progress workflow

        Careful: This method does NOT return status of a workflow. Verify it beforehand
        to make sure if workflow failed/succeeded/is running. You might get incomplete
        results

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            a mapping between task invocation ID and the available artifacts from the
                matching task run.
        """
        try:
            artifact_map = self._client.get_workflow_run_artifacts(workflow_run_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.WorkflowRunNotFound) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found."
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                "Could not get the outputs for workflow run with id "
                f"`{workflow_run_id}` "
                "- the authorization token was rejected by the remote cluster."
            ) from e

        artifact_vals: Dict[TaskInvocationId, WorkflowResult] = {}

        for task_run_id, artifact_ids in artifact_map.items():
            # No artifact available yet on the workflow driver
            if len(artifact_ids) == 0:
                continue
            inv_id = self._invocation_id_by_task_run_id(workflow_run_id, task_run_id)
            assert (
                len(artifact_ids) == 1
            ), "Expecting a single artifact containing the packed values from the task"
            try:
                artifact_vals[inv_id] = self._client.get_workflow_run_artifact(
                    artifact_ids[0]
                )
            except Exception:
                # If we fail for any reason, this artifact wasn't available yet
                continue

        return artifact_vals

    def _invocation_id_by_task_run_id(
        self, wf_run_id: WorkflowRunId, task_run_id: TaskRunId
    ) -> TaskInvocationId:
        # We shouldn't expect any particular format of the task run ID.
        # TODO: use workflow run -> task runs -> invocation ID.
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-694
        return task_run_id.split("@")[-1]

    def stop_workflow_run(
        self, workflow_run_id: WorkflowRunId, *, force: Optional[bool] = None
    ) -> None:
        """Stops a workflow run.

        Raises:
            WorkflowRunCanNotBeTerminated if workflow run is cannot be terminated.
        """
        try:
            self._client.terminate_workflow_run(workflow_run_id, force)
        except _exceptions.WorkflowRunNotFound:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            )
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                f"Could not stop workflow run with id `{workflow_run_id}` "
                "- the authorization token was rejected by the remote cluster."
            ) from e

    def list_workflow_runs(
        self,
        *,
        limit: Optional[int] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
        workspace: Optional[WorkspaceId] = None,
    ) -> List[WorkflowRunMinimal]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            status: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace.

        Raises:
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
                A list of the workflow runs
        """
        if max_age or state:
            warnings.warn(
                "Filtering CE workflow runs by max age and/or state is not currently "
                "supported. These filters will not be applied."
            )

        # Calculate how many pages of what sizes we need.
        # The max_page_size should be the same as the maximum defined in
        # https://github.com/zapatacomputing/workflow-driver/blob/fc3964d37e05d9421029fe28fa844699e2f99a52/openapi/src/parameters/query/pageSize.yaml#L10 # noqa: E501
        max_page_size: int = 100
        page_sizes: Sequence[Optional[int]] = [None]
        if limit is not None:
            if limit < max_page_size:
                page_sizes = [limit]
            else:
                page_sizes = [max_page_size for _ in range(limit // max_page_size)] + [
                    limit % max_page_size
                ]

        page_token: Optional[str] = None
        runs: List[WorkflowRunMinimal] = []

        for page_size in page_sizes:
            try:
                # TODO(ORQSDK-684): driver client cannot do filtering via API yet
                # https://zapatacomputing.atlassian.net/browse/ORQSDK-684?atlOrigin=eyJpIjoiYmNiZjUyMjZiNzg5NDI2YWJmNGU5NzAxZDI1MmJlNzEiLCJwIjoiaiJ9 # noqa: E501
                paginated_runs = self._client.list_workflow_runs(
                    page_size=page_size,
                    page_token=page_token,
                    workspace=workspace,
                )
            except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
                raise exceptions.UnauthorizedError(
                    "Could not get list of workflow runs "
                    "- the authorization token was rejected by the remote cluster."
                ) from e
            page_token = paginated_runs.next_page_token
            runs += paginated_runs.contents

            if page_size is not None and len(paginated_runs.contents) < page_size:
                # If we got back fewer results than we asked for, then we've exhausted
                # the available runs given our filters and don't want to make any
                # further requests.
                break

        return runs

    def get_workflow_logs(self, wf_run_id: WorkflowRunId) -> WorkflowLogs:
        """
        Get all logs produced during the execution of this workflow run.

        Args:
            wf_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token
            ...
        """
        try:
            messages = self._client.get_workflow_run_logs(wf_run_id)
            sys_messages = self._client.get_system_logs(wf_run_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.WorkflowRunNotFound) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{wf_run_id}` not found"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                f"Could not access logs for workflow run with id `{wf_run_id}`. "
                "- the authorization token was rejected by the remote cluster."
            ) from e
        except _exceptions.WorkflowRunLogsNotReadable as e:
            raise exceptions.InvalidWorkflowRunLogsError(
                f"Failed to decode logs for workflow run with id `{wf_run_id}`. "
                "Please report this as a bug."
            ) from e

        task_logs = []
        env_logs = []
        other_logs = []

        for m in messages:
            path = Path(m.ray_filename)
            if _regrouping.is_worker(path=path):
                task_logs.append(m.log)
            elif _regrouping.is_env_setup(path=path):
                env_logs.append(m.log)
            else:
                # Reasons for the "other" logs: future proofness and empathy. The server
                # might return events from more files in the future. We want to let the
                # user see it even this version of the SDK doesn't know how to
                # categorize it. Noisy data is better than no data when the user is
                # trying to find a bug.

                # TODO: group "other" log lines by original filename. Otherwise we risk
                # interleaved lines from multiple files. This is gonna be much easier
                # to implement after we do
                # https://zapatacomputing.atlassian.net/browse/ORQSDK-840.
                other_logs.append(m.log)

        return WorkflowLogs(
            per_task={"UNKNOWN TASK INV ID": task_logs},
            system=[str(m.log) for m in sys_messages],
            env_setup=env_logs,
            other=other_logs,
        )

    def get_task_logs(self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
        raise NotImplementedError()

    def list_workspaces(self):
        try:
            workspaces = self._client.list_workspaces()
            return [
                Workspace(workspace_id=ws.id, name=ws.displayName) for ws in workspaces
            ]
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                "Could not list workspaces "
                "- the authorization token was rejected by the remote cluster."
            ) from e

    def list_projects(self, workspace_id: str):
        try:
            projects = self._client.list_projects(workspace_id)
            return [
                Project(
                    project_id=project.id,
                    workspace_id=project.resourceGroupId,
                    name=project.displayName,
                )
                for project in projects
            ]
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                "Could not list projects "
                "- the authorization token was rejected by the remote cluster."
            ) from e
        except _exceptions.InvalidWorkspaceZRI as e:
            raise exceptions.NotFoundError(
                f"Could not find workspace with id: {workspace_id}."
            ) from e

    def get_workflow_project(self, wf_run_id: WorkflowRunId) -> ProjectRef:
        return self._client.get_workflow_project(wf_run_id)
