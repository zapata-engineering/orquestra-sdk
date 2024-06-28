################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
"""RuntimeInterface implementation that uses Compute Engine."""
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Protocol, Sequence, Union

from orquestra.workflow_shared import (
    Project,
    ProjectRef,
    Workspace,
    exceptions,
    retry,
    serde,
)
from orquestra.workflow_shared.abc import RuntimeInterface
from orquestra.workflow_shared.exceptions import (
    ExpiredTokenError,
    IgnoredFieldWarning,
    InvalidTokenError,
    UnauthorizedError,
)
from orquestra.workflow_shared.kubernetes.quantity import parse_quantity
from orquestra.workflow_shared.logs import (
    LogAccumulator,
    LogOutput,
    LogStreamType,
    WorkflowLogs,
    is_env_setup,
    is_worker,
)
from orquestra.workflow_shared.schema.configs import RuntimeConfiguration
from orquestra.workflow_shared.schema.ir import (
    ArtifactFormat,
    TaskInvocationId,
    WorkflowDef,
)
from orquestra.workflow_shared.schema.responses import (
    ComputeEngineWorkflowResult,
    WorkflowResult,
)
from orquestra.workflow_shared.schema.workflow_run import (
    State,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
    WorkflowRunSummary,
    WorkspaceId,
)

from orquestra.sdk._client._base._jwt import check_jwt_without_signature_verification

from . import _client, _exceptions, _models


class PaginatedListFunc(Protocol):
    """Protocol for functions passed to CERuntime._list_wf_runs()."""

    def __call__(
        self,
        workflow_def_id: Optional[_models.WorkflowDefID] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        workspace: Optional[WorkspaceId] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
    ) -> _client.Paginated:
        ...


def _check_token_validity(token: str):
    try:
        check_jwt_without_signature_verification(token)
    except (ExpiredTokenError, InvalidTokenError):
        raise UnauthorizedError


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
    """A runtime for communicating with the Compute Engine API endpoints."""

    def __init__(
        self,
        config: RuntimeConfiguration,
        client: _client.DriverClient,
        token: str,
        verbose: bool = False,
    ):
        """Initialiser for the CERuntime interface.

        Args:
            config: contains the runtime configuration, including the name of the
                config being used and the associated runtime options.
                These options control how to connect to a CE cluster.
            client: The DriverClient through which the runtime should communicate.
            verbose: if `True`, CERuntime may print debug information about
                its inner working to stderr.
            token: bearer's token used to authenticate with the cluster

        Raises:
            RuntimeConfigError: when the config is invalid.
        """
        self._config = config
        self._verbose = verbose

        self._client = client
        self._token = token

    def create_workflow_run(
        self, workflow_def: WorkflowDef, project: Optional[ProjectRef], dry_run: bool
    ) -> WorkflowRunId:
        """Schedules a workflow definition for execution.

        Args:
            workflow_def: the IR of the workflow to run.
            project: Project dir (workspace and project ID) on which the workflow
                will be run.
            dry_run: If True, code of the tasks will not be executed.

        Raises:
            WorkflowSyntaxError: when the workflow definition was rejected by the remote
                cluster.
            WorkflowRunNotStarted: for all other errors that prevented the workflow run
                from being scheduled.
            UnauthorizedError: if the current token was rejected by the remote cluster.

        Returns:
            the workflow run ID.
        """
        if project is None or project.workspace_id is None:
            warnings.warn(
                "Please specify workspace ID directly for submitting CE workflows."
                " Support for default workspace will be removed in the next release",
                category=PendingDeprecationWarning,
            )
        _check_token_validity(self._token)

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

        if (
            workflow_def.data_aggregation is not None
            and workflow_def.data_aggregation.resources is not None
        ):
            head_node_resources = _models.HeadNodeResources(
                cpu=workflow_def.data_aggregation.resources.cpu,
                memory=workflow_def.data_aggregation.resources.memory,
            )
            if workflow_def.data_aggregation.resources.gpu:
                warnings.warn(
                    "Head node resources will ignore GPU settings",
                    category=IgnoredFieldWarning,
                )
            if workflow_def.data_aggregation.resources.nodes:
                warnings.warn(
                    'Head node resources will ignore "nodes" settings',
                    category=IgnoredFieldWarning,
                )
        else:
            head_node_resources = None

        try:
            workflow_def_id = self._client.create_workflow_def(workflow_def, project)

            workflow_run_id = self._client.create_workflow_run(
                workflow_def_id, resources, dry_run, head_node_resources
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

        return workflow_run_id

    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """Gets the status of a workflow run.

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            The status of the workflow run
        """
        _check_token_validity(self._token)

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

    @retry(
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
        _check_token_validity(self._token)

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
        """Returns all available outputs for a workflow.

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
        _check_token_validity(self._token)

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

        Args:
            workflow_run_id: ID of the workflow run to be terminated.
            force: Asks the runtime to terminate the workflow without waiting for the
                workflow to gracefully exit.
                By default, this behavior is up to the runtime, but can be overridden
                with True/False.

        Raises:
            WorkflowRunCanNotBeTerminated: if workflow run is cannot be terminated.
        """
        _check_token_validity(self._token)

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

    def get_output(
        self, workflow_run_id: WorkflowRunId, task_invocation_id: TaskInvocationId
    ) -> WorkflowResult:
        raise exceptions.UnsupportedRuntimeFeature(
            "CE runtime" "does not support get_output()" "function yet."
        )

    @staticmethod
    def _list_wf_runs(
        func: PaginatedListFunc,
        limit: Optional[int],
        max_age: Optional[timedelta],
        state: Optional[Union[State, List[State]]],
        workspace: Optional[WorkspaceId],
    ) -> list:
        """Iterate through pages listing workflow runs.

        This method encompasses the logic the drives `list_workflow_runs` and
        `list_workflow_run_summaries`.

        Args:
            func: the paginated listing function over which to iterate.
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace.

        Raises:
            exceptions.UnauthorizedError: if the remote cluster rejects the token.

        Returns:
            A list of workflow runs, expressed either as WorkflowRun or
                WorkflowRunSummary objects.
        """
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
        runs = []

        for page_size in page_sizes:
            try:
                paginated_runs = func(
                    page_size=page_size,
                    page_token=page_token,
                    workspace=workspace,
                    max_age=max_age,
                    state=state,
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

    def list_workflow_run_summaries(
        self,
        *,
        limit: Optional[int] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
        workspace: Optional[WorkspaceId] = None,
    ) -> List[WorkflowRunSummary]:
        """List summaries of the workflow runs, with some filters.

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace.

        Raises:
            UnauthorizedError: if the remote cluster rejects the token
        """
        _check_token_validity(self._token)

        func = self._client.list_workflow_run_summaries
        try:
            return self._list_wf_runs(
                func,
                limit=limit,
                max_age=max_age,
                state=state,
                workspace=workspace,
            )
        except exceptions.UnauthorizedError:
            raise

    def list_workflow_runs(
        self,
        *,
        limit: Optional[int] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
        workspace: Optional[WorkspaceId] = None,
    ) -> List[WorkflowRunMinimal]:
        """List the workflow runs, with some filters.

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            state: Only return runs of runs with the specified status.
            workspace: Only return runs from the specified workspace.

        Raises:
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
                A list of the workflow runs
        """
        _check_token_validity(self._token)

        func = self._client.list_workflow_runs
        try:
            return self._list_wf_runs(
                func,
                limit=limit,
                max_age=max_age,
                state=state,
                workspace=workspace,
            )
        except exceptions.UnauthorizedError:
            raise

    def get_workflow_logs(self, wf_run_id: WorkflowRunId) -> WorkflowLogs:
        """Get all logs produced during the execution of this workflow run.

        Args:
            wf_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token
        """
        _check_token_validity(self._token)

        try:
            messages = self._client.get_workflow_run_logs(wf_run_id)
            sys_messages = self._client.get_system_logs(wf_run_id)
            wf_def = self._client.get_workflow_run(wf_run_id).workflow_def
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

        task_logs = {}
        env_logs = LogAccumulator()
        other_logs = LogAccumulator()
        system_logs = LogAccumulator()

        for task_inv_id in wf_def.task_invocations.keys():
            try:
                task_logs[task_inv_id] = self.get_task_logs(wf_run_id, task_inv_id)
            except exceptions.TaskRunLogsNotFound:
                # If a task's logs aren't available, keep going
                pass

        for m in messages:
            # Default to "log.out" if no log filenames
            path = Path(m.ray_filename)
            stream = LogStreamType.by_file(path)
            if is_worker(path=path):
                # We previously added Ray worker logs under task logs with
                # "UNKNOWN TASK INV".
                # Now, we get task logs from the CE API directly and add the
                # worker logs to "other".
                other_logs.add_line_by_stream(stream, m.log)
            elif is_env_setup(path=path):
                env_logs.add_line_by_stream(stream, m.log)
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
                other_logs.add_line_by_stream(stream, m.log)

        for sys_m in sys_messages:
            system_logs.add_line_by_stream(LogStreamType.STDOUT, str(sys_m.log))

        return WorkflowLogs(
            per_task=task_logs,
            system=LogOutput(out=system_logs.out, err=system_logs.err),
            env_setup=LogOutput(out=env_logs.out, err=env_logs.err),
            other=LogOutput(out=other_logs.out, err=other_logs.err),
        )

    def get_task_logs(self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
        """Get the logs produced by a task invocation during a workflow run.

        Args:
            wf_run_id: the ID of a workflow run.
            task_inv_id: the ID of a specific task invocation in the workflow.

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found.
            InvalidWorkflowRunLogsError: if the logs could not be decoded.
            UnauthorizedError: if the remote cluster rejects the token.
        """
        _check_token_validity(self._token)

        try:
            messages = self._client.get_task_run_logs(wf_run_id, task_inv_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.TaskRunLogsNotFound) as e:
            raise exceptions.TaskRunLogsNotFound(wf_run_id, task_inv_id) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(
                f"Could not access logs for workflow run with id `{wf_run_id}`. "
                "- the authorization token was rejected by the remote cluster."
            ) from e
        except _exceptions.WorkflowRunLogsNotReadable as e:
            raise exceptions.InvalidWorkflowRunLogsError(
                f"Failed to decode logs for workflow run with id `{wf_run_id}` "
                f"and task invocation ID `{task_inv_id}`. "
                "Please report this as a bug."
            ) from e

        task_logs = LogAccumulator()
        for m in messages:
            path = Path(m.log_filename)
            stream = LogStreamType.by_file(path)
            task_logs.add_line_by_stream(stream, m.log)
        return LogOutput(out=task_logs.out, err=task_logs.err)

    def list_workspaces(self):
        _check_token_validity(self._token)

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
        _check_token_validity(self._token)

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
        _check_token_validity(self._token)

        return self._client.get_workflow_project(wf_run_id)
