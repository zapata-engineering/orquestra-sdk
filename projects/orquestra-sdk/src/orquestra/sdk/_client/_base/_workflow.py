################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
import ast
import functools
import inspect
import warnings
from pathlib import Path
from types import FunctionType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

import orquestra.workflow_shared.schema.ir as ir
from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.exceptions import WorkflowSyntaxError
from orquestra.workflow_shared.schema.workflow_run import ProjectId, WorkspaceId
from orquestra.workflow_shared.secrets import Secret
from typing_extensions import ParamSpec

from orquestra.sdk import secrets

from . import _api, _dsl, loader
from ._ast import CallVisitor, NodeReference, NodeReferenceType, normalize_indents
from ._config import RuntimeConfig
from ._dsl import (
    DataAggregation,
    FunctionRef,
    Import,
    ImportTypes,
    TaskDef,
    UnknownPlaceholderInCustomNameWarning,
    get_fn_ref,
    parse_custom_name,
)


# ----- Workflow exceptions  -----
class NotATaskWarning(Warning):
    pass


# ----- data structures -----

_P = ParamSpec("_P")
_R = TypeVar("_R", covariant=True)


class WorkflowDef(Generic[_R]):
    """An Orquestra Workflow Definition. The result of a call to a WorkflowTemplate.

    We do some magic to transform Python code in the decorated function to a
    computational workflow graph. Use the `.model` property to get the serializable
    form.
    """

    def __init__(
        self,
        name: str,
        workflow_fn: Callable[..., _R],
        fn_ref: FunctionRef,
        resources: _dsl.Resources,
        head_node_image: Optional[str],
        data_aggregation: Optional[DataAggregation] = None,
        workflow_args: Optional[Tuple[Any, ...]] = None,
        workflow_kwargs: Optional[Dict[str, Any]] = None,
        default_source_import: Optional[Import] = None,
        default_dependency_imports: Optional[Iterable[Import]] = None,
    ):
        self._name = name
        self._fn = workflow_fn
        self._fn_ref = fn_ref
        self._resources = resources
        self._data_aggregation = data_aggregation
        self._workflow_args = workflow_args or ()
        self._workflow_kwargs = workflow_kwargs or {}
        self._head_node_image = head_node_image
        self.default_source_import = default_source_import
        self.default_dependency_imports = default_dependency_imports

    @property
    def name(self) -> str:
        return self._name

    @property
    def fn_ref(self) -> FunctionRef:
        return self._fn_ref

    @property
    def data_aggregation(self) -> Optional[DataAggregation]:
        return self._data_aggregation

    @property
    def model(self) -> ir.WorkflowDef:
        """Serializable form of a workflow def (intermediate representation).

        Returns:
            Serializable Pydantic model.

        Raises:
            orquestra.sdk.exceptions.DirtyGitRepo: (warning) when a task def used by
                this workflow def has a "GitImport" and the git repo that contains it
                has uncommitted changes.
            orquestra.sdk.exceptions.WorkflowSyntaxError: when there are no tasks
                defined for this workflow.
            orquestra.sdk.exceptions.InvalidTaskDefinitionError: when there is invalid
                task used inside this workflow
        """
        from orquestra.sdk._client._base import _traversal

        futures = _traversal.extract_root_futures(self)
        model = _traversal.flatten_graph(self, futures)

        if len(model.task_invocations) < 1:
            helpstr = f"The workflow '{model.name}' "
            if not isinstance(model.fn_ref, ir.InlineFunctionRef):
                helpstr += f"(defined at {model.fn_ref.file_path}"
                helpstr += f" line {model.fn_ref.line_number}"
                helpstr += ") "
            helpstr += (
                "cannot be submitted as it does not define any tasks to be executed. "
                "Please modify the workflow definition to define at least one task "
                "and retry submitting the workflow."
            )
            raise WorkflowSyntaxError(helpstr)
        return model

    @property
    def graph(self):
        """Builds a graphviz visualization of the workflow graph.

        Jupyter renders it natively.

        Note: rendering an image of the result graph requires a system-wide installation
        of graphviz. For installation instructions see:
        https://graphviz.readthedocs.io/en/stable/manual.html#installation
        """
        # Defer import to avoid 120ms latency toll for everything.
        import orquestra.sdk._client._base._viz

        return orquestra.sdk._client._base._viz.wf_def_to_graphviz(self.model)

    def run(
        self,
        config: Union[RuntimeConfig, str],
        project_dir: Optional[Union[str, Path]] = None,
        workspace_id: Optional[WorkspaceId] = None,
        project_id: Optional[ProjectId] = None,
        dry_run: bool = False,
    ) -> _api.WorkflowRun:
        """Schedules workflow for execution.

        Args:
            config: SDK needs to know where to execute the workflow. The config
                contains the required details. This can be a RuntimeConfig object, or
                the name of a saved configuration.
            project_dir: DEPRECATED
            workspace_id: ID of the workspace for workflow - supported only on CE
            project_id: ID of the project for workflow - supported only on CE
            dry_run: Run the workflow without actually executing any task code.
                Useful for testing infrastructure, dependency imports, etc.

        Raises:
            orquestra.sdk.exceptions.DirtyGitRepo: (warning) when a task def used by
                this workflow def has a "GitImport" and the git repo that contains it
                has uncommitted changes.
            orquestra.sdk.exceptions.ProjectInvalidError: when only 1 out of project and
                workspace is passed.
        """
        if project_dir:
            warnings.warn(
                "project_dir argument is deprecated and will be removed"
                "in upcoming versions of orquestra-sdk"
            )

        try:
            wf_def_model = self.model
        except exceptions.DirtyGitRepo:
            raise

        try:
            return _api.WorkflowRun.start_from_ir(
                wf_def=wf_def_model,
                config=config,
                workspace_id=workspace_id,
                project_id=project_id,
                dry_run=dry_run,
            )
        except exceptions.ProjectInvalidError:
            raise

    def with_resources(
        self,
        *,
        cpu: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        memory: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        disk: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        gpu: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        nodes: Optional[Union[int, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
    ) -> "WorkflowDef":
        """Assigns optional metadata related to this workflow definition object.

        Doesn't modify the existing workflow definition, returns a new one.

        Example usage::

            wf_run = my_workflow().with_resources(
                cpu="10", memory="10Gi"
            ).run("my_cluster")

        Args:
            cpu: amount of cpu requested for the workflow
            memory: amount of memory requested for the workflow
            disk: amount of disk requested for the workflow
            gpu: amount of gpu requested for the workflow
            nodes: the number of nodes requested for the workflow
        """
        # Only use the new properties if they have not been changed.
        # None is a valid option, so we are using the Sentinel object pattern:
        # https://python-patterns.guide/python/sentinel-object/

        resources = self._resources
        new_resources = _dsl.Resources(
            cpu=resources.cpu if cpu is _dsl.Sentinel.NO_UPDATE else cpu,
            gpu=resources.gpu if gpu is _dsl.Sentinel.NO_UPDATE else gpu,
            memory=resources.memory if memory is _dsl.Sentinel.NO_UPDATE else memory,
            disk=resources.disk if disk is _dsl.Sentinel.NO_UPDATE else disk,
            nodes=resources.nodes if nodes is _dsl.Sentinel.NO_UPDATE else nodes,
        )
        return WorkflowDef(
            name=self._name,
            workflow_fn=self._fn,
            fn_ref=self._fn_ref,
            resources=new_resources,
            data_aggregation=self._data_aggregation,
            workflow_args=self._workflow_args,
            workflow_kwargs=self._workflow_kwargs,
            default_source_import=self.default_source_import,
            default_dependency_imports=self.default_dependency_imports,
            head_node_image=self._head_node_image,
        )

    def with_head_node_resources(
        self,
        *,
        cpu: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        memory: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        disk: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
    ) -> "WorkflowDef":
        """Assigns optional metadata related to this workflow definition object.

        Doesn't modify the existing workflow definition, returns a new one.

        Example usage::

            wf_run = my_workflow().with_head_node_resources(
                cpu="10", memory="10Gi"
            ).run("my_cluster")

        Args:
            cpu: amount of cpu requested for the head node
            memory: amount of memory requested for the head node
            disk: amount of disk requested for the head node
        """
        # Only use the new properties if they have not been changed.
        # None is a valid option, so we are using the Sentinel object pattern:
        # https://python-patterns.guide/python/sentinel-object/

        resources = (
            self._data_aggregation.resources
            if self._data_aggregation
            else _dsl.Resources()
        )

        new_resources = _dsl.Resources(
            cpu=resources.cpu if cpu is _dsl.Sentinel.NO_UPDATE else cpu,
            memory=resources.memory if memory is _dsl.Sentinel.NO_UPDATE else memory,
            disk=resources.disk if disk is _dsl.Sentinel.NO_UPDATE else disk,
        )

        new_data_aggregation = _dsl.DataAggregation(resources=new_resources)
        return WorkflowDef(
            name=self._name,
            workflow_fn=self._fn,
            fn_ref=self._fn_ref,
            resources=self._resources,
            data_aggregation=new_data_aggregation,
            workflow_args=self._workflow_args,
            workflow_kwargs=self._workflow_kwargs,
            default_source_import=self.default_source_import,
            default_dependency_imports=self.default_dependency_imports,
            head_node_image=self._head_node_image,
        )

    def with_head_node_image(
        self,
        *,
        image: Optional[str],
    ) -> "WorkflowDef":
        """Assigns optional metadata related to this workflow definition object.

        Doesn't modify the existing workflow definition, returns a new one.

        Example usage::

            wf_run = my_workflow().with_head_node_image(
                image="abc"
            ).run("my_cluster")

        Args:
            image: docker image to be used as head node image,
                Image should be full path to docker image with the tag, example:
                "hub.stage.nexus.orquestra.io/zapatacomputing/workflow-driver-ray:orquestra-head-image-v1.0.0"
        """
        return WorkflowDef(
            name=self._name,
            workflow_fn=self._fn,
            fn_ref=self._fn_ref,
            resources=self._resources,
            data_aggregation=self._data_aggregation,
            workflow_args=self._workflow_args,
            workflow_kwargs=self._workflow_kwargs,
            default_source_import=self.default_source_import,
            default_dependency_imports=self.default_dependency_imports,
            head_node_image=image,
        )


class WorkflowTemplate(Generic[_P, _R]):
    """Result of applying the `@workflow` decorator to a function."""

    def __init__(
        self,
        custom_name: Optional[str],
        workflow_fn: Callable[_P, _R],
        fn_ref: FunctionRef,
        is_parametrized: bool,
        resources: _dsl.Resources,
        data_aggregation: Optional[Union[DataAggregation, bool]] = None,
        default_source_import: Optional[Import] = None,
        default_dependency_imports: Optional[Iterable[Import]] = None,
        head_node_image: Optional[str] = None,
    ):
        self._custom_name = custom_name
        self._fn = workflow_fn
        self._fn_ref = fn_ref
        self._is_parametrized = is_parametrized
        self._resources = resources
        self._data_aggregation = data_aggregation
        self._default_source_import = default_source_import
        self._default_dependency_imports = default_dependency_imports
        self._head_node_image = head_node_image

    # flake8: ignore=DOC101
    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> WorkflowDef[_R]:
        """When called like a function, construct and return the WorkflowDef.

        Args:
            *args: Variable length argument list to be passed to the workflow.
            **kwargs: Arbitrary keyword arguments to be passed to the workflow.

        Raises:
            RuntimeError: if the workflow calls a non-task function.
            orquestra.sdk.exceptions.WorkflowSyntaxError: if the arguments don't match
                the workflow def's parameters.
        """
        allowed_function_calls = (
            Secret,
            secrets.get,
            secrets.delete,
            secrets.list,
            secrets.set,
        )
        # First we check the contents of the workflow function and warn the user if we
        # find any function calls to non-tasks.
        fn_calls = _get_function_calls(self._fn)
        non_task_functions = []
        for called_fn in fn_calls:
            if isinstance(called_fn.function, loader.FakeImportedAttribute):
                raise RuntimeError(
                    f'"{called_fn.name}" is currently loaded from a faked import. '
                    'Try adding it to "workflow_defs.py"'
                )
            elif not (
                isinstance(called_fn.function, TaskDef)
                or called_fn.function in allowed_function_calls
            ):
                non_task_functions.append(called_fn.name)

        if non_task_functions:
            warnings.warn(
                f'Functions: "{", ".join(non_task_functions)}" called in the workflow '
                f"are not tasks. Did you mean to decorate them with @task?",
                NotATaskWarning,
            )

        data_aggregation: Optional[DataAggregation]
        if self._data_aggregation is False:
            data_aggregation = DataAggregation(run=False)
        elif self._data_aggregation is True:
            data_aggregation = None  # fall back to default
        elif (
            isinstance(self._data_aggregation, _dsl.DataAggregation)
            and self._data_aggregation.resources.gpu is not None
        ):
            warnings.warn(
                "GPU setting for Data Aggregation will be ignored. Falling back to 0"
            )
            new_resources = self._data_aggregation.resources._replace(gpu="0")
            data_aggregation = self._data_aggregation._replace(resources=new_resources)
        else:
            data_aggregation = self._data_aggregation

        signature = inspect.signature(self._fn)
        try:
            bound_args = signature.bind(*args, **kwargs)
        except TypeError as e:
            raise WorkflowSyntaxError(
                "Workflow arguments must be known at submission time. "
            ) from e
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "error", category=UnknownPlaceholderInCustomNameWarning
            )
            try:
                name = (
                    parse_custom_name(self._custom_name, bound_args)
                    or self._fn.__name__
                )
            except UnknownPlaceholderInCustomNameWarning:
                raise WorkflowSyntaxError(
                    "Workflow arguments must be known at submission time. "
                )
        return WorkflowDef(
            name=name,
            workflow_fn=self._fn,
            fn_ref=self._fn_ref,
            resources=self._resources,
            data_aggregation=data_aggregation,
            workflow_args=args,
            workflow_kwargs=kwargs,
            default_source_import=self._default_source_import,
            default_dependency_imports=self._default_dependency_imports,
            head_node_image=self._head_node_image,
        )

    @property
    def is_parametrized(self) -> bool:
        """A workflow is parametrized if the decorated function has parameters.

        If the function had no parameters, the workflow is not considered parametrized.
        """
        return self._is_parametrized

    @property
    def model(self) -> ir.WorkflowDef:
        """Serializable form of a workflow template (intermediate representation).

        Raises:
            NotImplementedError: when this method is called with a parametrized
                workflow.

        Returns:
            Serializable Pydantic model.
        """
        # First we check if we're a parametrized workflow
        # If not, we return the model of the underlying workflow def
        # Otherwise we raise an error
        if self.is_parametrized:
            raise NotImplementedError(
                "Parametrized WorkflowTemplates cannot be serialised yet"
            )
        else:
            # `WorkflowTemplate.__call__` accepts arguments if and only if the
            # underlying function has parameters.
            # The `self.is_parametrized` property tells us if the workflow function
            # is parametrized.
            # However, `self.__call__()` accepts a Paramspec `_P` as the input
            # parameter type (`_P.args` and `_P.kwargs`).
            # This causes a type error in Pyright because Pyright does not have the
            # information that `is_parametrized` tells us if `_P` has any members.
            # When `is_parametrized` is false, we know that `_P.args` and `_P.kwargs`
            # are empty.
            # This means, that when we are in this branch, we can be sure that
            # `__call__` does not accept any args or kwargs.
            return self().model  # type: ignore


# ----- decorator helpers -----
class _CalledFunction(NamedTuple):
    """Call made inside the workflow definition.

    Attributes:
        function: the function called inside the workflow.
        name: name of the function called.
        module_name: name of the module from where the function is imported from.
        source_file: file where the function is defined.
        line_no: line of the workflow definition where the function is called.
    """

    function: Callable
    name: str
    module_name: Optional[str] = None
    source_file: Optional[str] = None
    line_no: Optional[int] = None


NO_MODULE_SENTINEL = object()


def _get_callable(  # noqa: DOC103 - pydoclint doesn't like fn: Callable for some reason
    fn: Callable, call_statement: List[NodeReference]
) -> Tuple[Union[Callable, None], Union[str, None]]:
    """Find the callable and the callable's module name.

    If the callable cannot be find then return None.

    Args:
        fn: workflow function
        call_statement: List of NodeReferences with information about the call history.

    Returns:
        _fn: Callable corresponding to the call_statement
        module_name: Name of the callable's module
    """
    found_module: Any = NO_MODULE_SENTINEL
    _fn = None
    module_name = None
    for node in call_statement:
        if node.node_type is NodeReferenceType.CALL:
            # For CALL nodes we try to retrieve the callable object
            if found_module is not NO_MODULE_SENTINEL:
                # This raises:
                #    AttributeError if the function isn't found
                try:
                    _fn = getattr(found_module, node.name)
                    module_name = found_module.__name__
                    break
                except AttributeError:
                    # We cannot find the function, so break
                    _fn = None
                    module_name = None
                    break
            else:
                # This raises:
                #    KeyError if the function isn't found
                try:
                    _fn = fn.__globals__[node.name]
                    module_name = None
                    break
                except KeyError:
                    # We cannot find the function, so break
                    _fn = None
                    module_name = None
                    break
        else:
            # try to retrieve the module from where the callable is imported
            try:
                if found_module is not NO_MODULE_SENTINEL:
                    found_module = getattr(found_module, node.name)
                else:
                    found_module = fn.__globals__[node.name]
            except (KeyError, AttributeError):
                # This "module" is not a module we can find.
                # For now, let's continue.
                break
    if not callable(_fn):
        # The "callable" is not a callable, then set it to None
        _fn = None
    return _fn, module_name


def _get_function_calls(fn: Callable) -> List[_CalledFunction]:  # noqa: DOC103
    """Get the functions that are called inside the workflow definition.

    This function uses some heuristics to find function calls using the workflow
    function's AST.
    If the function call cannot be identified, then is not returned in list of called
    functions.

    Arguments:
        fn : Workflow function

    Returns:
        List of functions that are called inside the
            workflow function

    """
    assert isinstance(fn, FunctionType)
    try:
        source = inspect.getsource(fn)
        _, base_lineno = inspect.getsourcelines(fn)
    except OSError:
        return []
    source_file = inspect.getabsfile(fn)
    fn_body = ast.parse(normalize_indents(source))
    visitor = CallVisitor()
    visitor.visit(fn_body)

    called_fns = []
    for call in visitor.calls:
        # Get the callable information
        _fn, _module_name = _get_callable(fn, call.call_statement)
        if _fn is None:
            # If we cannot find the callable then continue
            continue

        if call.line_no is not None:
            # We have an off-by-one because line numbers always start from 1, not 0.
            line_no = (base_lineno + call.line_no) - 1
        else:
            line_no = None
        called_fn = _CalledFunction(
            function=_fn,
            name=_fn.__name__,
            module_name=_module_name,
            source_file=source_file,
            line_no=line_no,
        )
        called_fns.append(called_fn)
    return called_fns


# ----- decorator -----


@overload
def workflow(fn: Callable[_P, _R]) -> WorkflowTemplate[_P, _R]:
    ...


@overload
def workflow(
    *,
    resources: Optional[_dsl.Resources] = None,
    head_node_resources: Optional[_dsl.Resources] = None,
    data_aggregation: Optional[Union[DataAggregation, bool]] = None,
    custom_name: Optional[str] = None,
    default_source_import: Optional[Import] = None,
    default_dependency_imports: Optional[Iterable[Import]] = None,
    head_node_image: Optional[str] = None,
) -> Callable[[Callable[_P, _R]], WorkflowTemplate[_P, _R]]:
    ...


@overload
def workflow(
    fn: Callable[_P, _R],
    *,
    resources: Optional[_dsl.Resources] = None,
    head_node_resources: Optional[_dsl.Resources] = None,
    data_aggregation: Optional[Union[DataAggregation, bool]] = None,
    custom_name: Optional[str] = None,
    default_source_import: Optional[Import] = None,
    default_dependency_imports: Optional[Iterable[Import]] = None,
    head_node_image: Optional[str] = None,
) -> WorkflowTemplate[_P, _R]:
    ...


def workflow(
    fn: Optional[Callable[_P, _R]] = None,
    *,
    resources: Optional[_dsl.Resources] = None,
    head_node_resources: Optional[_dsl.Resources] = None,
    data_aggregation: Optional[Union[DataAggregation, bool]] = None,
    custom_name: Optional[str] = None,
    default_source_import: Optional[Import] = None,
    default_dependency_imports: Union[Iterable[Import], Import, None] = None,
    head_node_image: Optional[str] = None,
) -> Union[
    WorkflowTemplate[_P, _R],
    Callable[[Callable[_P, _R]], WorkflowTemplate[_P, _R]],
]:
    """Decorator that produces a workflow definition.

    Args:
        fn: the function to be wrapped.
        resources: !Unstable API! The resources that this workflow requires.
            The exact behaviour depends on the runtime, based on a Compute Engine
            workflow, you can set the cluster your workflow will use:
            10 nodes with 20 CPUs and a GPU each would be:
            resources=sdk.Resources(cpu="20", gpu="1", nodes=10)
            If omitted, the cluster's default resources will be used.
        head_node_resources: !Unstable API! The resources that the head node requires.
            Only used on Compute Engine. If omitted, the cluster's default head node
            resources will be used.
        data_aggregation: Deprecated.
        custom_name: custom name for the workflow
        default_source_import: Set the default source import for all tasks inside
           this workflow.
           Important: if a task defines its own individual source import, the workflow
           scoped default_source_import will be ignored.
        default_dependency_imports: Set the default dependency imports for all tasks
           inside this workflow.
           Important: if a task defines its own individual dependency imports, the
           workflow scoped default_dependency_imports will be ignored for that
           particular task
        head_node_image: Path to docker image that will be used as head node image.
            Image should be full path to docker image with the tag, example:
            "hub.stage.nexus.orquestra.io/zapatacomputing/workflow-driver-ray:orquestra-head-image-v1.0.0"

    You can use the Python API to submit workflows for execution::

        @sdk.workflow
        def my_wf():
            ...

        wf_run = my_wf().run([config])


    Simple workflows (without any parameters) can also be submitted via the CLI.

    A single workflow def can be submitted to be ran multiple times.

    Whatever is returned from the `fn` will be treated as "workflow results" and will be
    accessible by both the Python API and the CLI after the workflow has completed.

    This decorator will raise a NotATaskWarning if any of the "calls" inside the
    workflow function are not tasks. For example::

        @sdk.task
        def my_task(data):
            return transform(data)

        @sdk.workflow
        def wf():
            data = np.ones(2)
            return [my_task(data)]

    will raise a NotATaskWarning on the `np.ones` call, but not for the `my_task` call.
    """
    workflow_default_dependency_imports: Optional[Tuple[Import, ...]]

    if default_dependency_imports is None:
        workflow_default_dependency_imports = None
    elif isinstance(default_dependency_imports, ImportTypes):
        workflow_default_dependency_imports = (default_dependency_imports,)
    elif default_dependency_imports is not None:
        workflow_default_dependency_imports = tuple(default_dependency_imports)

    if data_aggregation is not None:
        warnings.warn(
            "data_aggregation argument is deprecated and will be removed "
            "in upcoming versions of orquestra-sdk. It will be ignored "
            "for this workflow.",
            category=FutureWarning,
        )

    if head_node_resources is not None:
        _data_aggregation = DataAggregation(resources=head_node_resources)
    else:
        _data_aggregation = None

    def _inner(fn: Callable[_P, _R]):
        signature = inspect.signature(fn)
        name = custom_name
        fn_ref = get_fn_ref(fn)
        template = WorkflowTemplate(
            custom_name=name,
            resources=resources or _dsl.Resources(),
            workflow_fn=fn,
            fn_ref=fn_ref,
            is_parametrized=len(signature.parameters) > 0,
            data_aggregation=_data_aggregation,
            default_source_import=default_source_import,
            default_dependency_imports=workflow_default_dependency_imports,
            head_node_image=head_node_image,
        )
        functools.update_wrapper(template, fn)
        return template

    if fn is None:
        return _inner
    else:
        return _inner(fn)
