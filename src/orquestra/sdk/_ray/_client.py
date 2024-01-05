################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""Facade module for Ray API."""
import typing as t

from orquestra.sdk.exceptions import (
    NotFoundError,
    UserTaskFailedError,
    WorkflowRunNotFoundError,
)

try:
    import ray
    import ray._private.ray_constants
    import ray.runtime_env
    import ray.workflow
    from ray import exceptions  # noqa: F401
    from ray.actor import ActorClass
    from ray.remote_function import RemoteFunction
    from ray.workflow import exceptions as workflow_exceptions  # noqa: F401
    from ray.workflow import workflow_context
    from ray.workflow.common import WorkflowStatus as RayWorkflowStatus
    from ray.workflow.storage import Storage as RayStorage
    from ray.workflow.workflow_storage import WorkflowStorage
except ModuleNotFoundError:
    if not t.TYPE_CHECKING:
        WorkflowStatus = None

        class RayClient:
            def __init__(self, *args, **kwargs):
                raise ModuleNotFoundError(
                    "In order to run workflows locally using Ray, "
                    "please make sure you install the optional dependencies with:\n"
                    "`pip install 'orquestra-sdk[all]'`",
                    name="ray",
                )

else:
    TaskError = ray.exceptions.RayTaskError
    ObjectRef = ray.ObjectRef
    WorkflowStatus = RayWorkflowStatus
    WorkflowStorage = WorkflowStorage
    Storage = RayStorage
    RuntimeEnv = ray.runtime_env.RuntimeEnv
    FunctionNode = ray.dag.FunctionNode
    LogPrefixActorName = ray._private.ray_constants.LOG_PREFIX_ACTOR_NAME
    LogPrefixTaskName = ray._private.ray_constants.LOG_PREFIX_TASK_NAME

    def save_task_postrun_metadata(
        wf_run_id: str,
        task_inv_id: str,
        metadata: t.Dict[str, t.Any],
    ):
        WorkflowStorage(wf_run_id).save_task_postrun_metadata(task_inv_id, metadata)

    class RayClient:
        """Abstraction layer between our Orquestra-specific RayRuntime and Ray's API.

        We should never use Ray's API directly; rather access it via this object's
        methods.

        This class should be as close as possible to Ray's domain, e.g. it should
        use the same names as Ray does.
        """

        # ----- Ray Core -----

        def init(
            self,
            address: t.Optional[str],
            log_to_driver: bool,
            storage: t.Union[None, str, Storage],
            _temp_dir: t.Optional[str],
            configure_logging: bool,
        ):
            ray.init(
                address=address,
                log_to_driver=log_to_driver,
                storage=storage,
                _temp_dir=_temp_dir,
                ignore_reinit_error=True,
                configure_logging=configure_logging,
            )

        def shutdown(self):
            ray.shutdown()

        def get(
            self,
            obj_refs: t.Union[ray.ObjectRef, t.List[ray.ObjectRef]],
            timeout: t.Optional[float] = None,
        ):
            try:
                return ray.get(obj_refs, timeout=timeout)
            except (UserTaskFailedError, exceptions.GetTimeoutError, ValueError):
                raise NotFoundError

        def remote(self, fn) -> t.Union[RemoteFunction, ActorClass]:
            return ray.remote(fn)

        def add_options(
            self,
            ray_remote_fn,
            *,
            name: str,
            metadata: t.Dict[str, t.Any],
            runtime_env: t.Optional[RuntimeEnv],
            catch_exceptions: t.Optional[bool],
            max_retries: int,
            resources: t.Optional[t.Mapping[str, float]] = None,
            num_cpus: t.Optional[t.Union[int, float]] = None,
            num_gpus: t.Optional[t.Union[int, float]] = None,
            memory: t.Optional[t.Union[int, float]] = None,
        ):
            # The type hint for 'ray.workflow.options' kwargs is invalid. We can
            # work it around by Any.
            workflow_opts: t.Any = {
                "task_id": name,
                "metadata": metadata,
                "catch_exceptions": catch_exceptions,
            }

            ray_optional_kwargs: t.Dict[str, t.Any] = {}
            if num_cpus is not None:
                ray_optional_kwargs["num_cpus"] = num_cpus
            if num_gpus is not None:
                ray_optional_kwargs["num_gpus"] = num_gpus
            if memory is not None:
                ray_optional_kwargs["memory"] = memory
            if resources is not None:
                ray_optional_kwargs["resources"] = resources

            return ray_remote_fn.options(
                **ray.workflow.options(**workflow_opts),
                runtime_env=runtime_env,
                max_retries=max_retries,
                **ray_optional_kwargs,
            )

        # ----- Ray Workflow -----

        def workflow_init(self):
            ray.workflow.init()

        def run_dag_async(
            self,
            dag_node: ray.dag.DAGNode,
            workflow_id: str,
            metadata: t.Dict[str, t.Any],
        ):
            ray.workflow.run_async(dag_node, workflow_id=workflow_id, metadata=metadata)

        def get_workflow_metadata(self, workflow_id: str) -> t.Dict[str, t.Any]:
            """Get the metadata for the workflow run, using Ray Workflow API.

            Args:
                workflow_id: ID of the workflow run, used to identify the correct run
                    in Ray.

            Raises:
                ValueError: if there's no workflow with this ID.
            """
            try:
                return ray.workflow.get_metadata(workflow_id)
            except ValueError:
                raise

        def get_workflow_status(self, workflow_id: str):
            """Get the current status of the workflow run, using Ray Workflow API.

            Args:
                workflow_id: ID of the workflow run, used to identify the correct run
                    in Ray.

            Raises:
                ray.workflow.exceptions.WorkflowNotFoundError: if there's no
                    workflow with this ID.
            """
            try:
                return ray.workflow.get_status(workflow_id)
            except WorkflowRunNotFoundError:
                raise

        def get_task_metadata(self, workflow_id: str, name: str):
            return ray.workflow.get_metadata(workflow_id, name)

        def get_workflow_output(self, workflow_id: str) -> t.Any:
            """Get values computed by the the whole workflow, using Ray Workflow API.

            Blocks until the workflow is completed.

            Args:
                workflow_id: ID of the workflow run, used to identify the correct run
                    in Ray.

            Returns:
                Deserialized values produced by the workflow's last node.

            Raises:
                ValueError: if there's no workflow with this ID.
            """
            try:
                return ray.workflow.get_output(workflow_id)
            except ValueError:
                raise

        def get_task_output_async(self, workflow_id: str, task_id: str) -> ObjectRef:
            """Get values computed by a single task node in a workflow.

            Uses the Ray Workflow API.
            Blocks until the workflow is completed.

            The "async" in the name refers to the returned type – an ObjectRef
            instead of a deserialized value.

            It won't raise errors if the 'workflow_id'-'task_id' combination wasn't
            found.
            A ValueError will be raised at ray.get() time.
            """
            return ray.workflow.get_output_async(workflow_id, task_id=task_id)

        def list_all(self) -> t.List[t.Tuple[str, WorkflowStatus]]:
            return ray.workflow.list_all()

        def cancel(self, workflow_id: str):
            ray.workflow.cancel(workflow_id)

        def get_current_workflow_id(self):
            return workflow_context.get_current_workflow_id()

        def get_current_task_id(self):
            return workflow_context.get_current_task_id()
