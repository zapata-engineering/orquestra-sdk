################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import ast
import functools
import inspect
import warnings
from enum import Enum
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

from typing_extensions import ParamSpec

import orquestra.sdk.schema.ir as ir
from orquestra.sdk.exceptions import (
    ConfigNameNotFoundError,
    ProjectInvalidError,
    WorkflowSyntaxError,
)
from orquestra.sdk.schema.workflow_run import ProjectId, WorkspaceId

from .. import secrets
from . import _api, _dsl, loader
from ._ast import CallVisitor, NodeReference, NodeReferenceType, normalize_indents
from ._dsl import (
    DataAggregation,
    FunctionRef,
    Import,
    ImportTypes,
    Secret,
    TaskDef,
    UnknownPlaceholderInCustomNameWarning,
    get_fn_ref,
    parse_custom_name,
)
from ._in_process_runtime import InProcessRuntime
from ._spaces._structs import ProjectRef
from .abc import RuntimeInterface


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
        """
        from orquestra.sdk._base import _traversal

        futures = _traversal.extract_root_futures(self)
        model = _traversal.flatten_graph(self, futures)

        if len(model.task_invocations) < 1:
            helpstr = f"The workflow '{model.name}' "
            if hasattr(model.fn_ref, "file_path"):  # If possible add the file and line.
                helpstr += f"(defined at {model.fn_ref.file_path}"
                if hasattr(model.fn_ref, "line_number"):
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
        """
        Builds a graphviz visualization of the workflow graph. Jupyter renders it
        natively.

        Note: rendering an image of the result graph requires a system-wide installation
        of graphviz. For installation instructions see:
        https://graphviz.readthedocs.io/en/stable/manual.html#installation
        """
        # Defer import to avoid 120ms latency toll for everything.
        import orquestra.sdk._base._viz

        return orquestra.sdk._base._viz.wf_def_to_graphviz(self.model)

    def local_run(self) -> _R:
        """Executes workflow as a script in a local environment."""
        _dsl.DIRECT_EXECUTION = True
        result = self._fn(*self._workflow_args, **self._workflow_kwargs)
        _dsl.DIRECT_EXECUTION = False
        return result

    def prepare(
        self,
        config: Union[_api.RuntimeConfig, str],
        project_dir: Optional[Union[str, Path]] = None,
        workspace_id: Optional[WorkspaceId] = None,
        project_id: Optional[ProjectId] = None,
    ) -> _api.WorkflowRun:
        """
        "Prepares" workflow for running. Call ".start()" on the result to
        schedule the workflow for execution.

        Args:
            config: SDK needs to know where to execute the workflow. The config
                contains the required details. This can be a RuntimeConfig object, or
                the name of a saved configuration.
            project_dir: the path to the project directory. If omitted, the current
                working directory is used.
            workspace_id: ID of the workspace for workflow - supported only on CE
            project_id: ID of the project for workflow - supported only on CE

        Raises:
            ConfigNameNotFoundError: when the configuration has not been saved prior to
                this point.
            orquestra.sdk.exceptions.DirtyGitRepo: (warning) when a task def used by
                this workflow def has a "GitImport" and the git repo that contains it
                has uncommitted changes.
            ProjectInvalidError: when only 1 out of project and workspace is passed
        """
        _config: _api.RuntimeConfig
        if isinstance(config, _api.RuntimeConfig):
            _config = config
        elif isinstance(config, str):
            _config = _api.RuntimeConfig.load(config)
        else:
            raise TypeError(
                f"'config' argument to `prepare()` has unsupported type {type(config)}."
            )
        runtime: RuntimeInterface
        if _config._runtime_name == "IN_PROCESS":
            runtime = InProcessRuntime()
        else:
            runtime = _config._get_runtime(project_dir=project_dir)

        # In close future there will be multiple ways of figuring out the
        # appropriate runtime to use, based on `config`. Regardless of this
        # logic, the runtime should always be resolved.
        assert runtime is not None

        _project: Optional[ProjectRef]
        if project_id is not None and workspace_id is not None:
            _project = ProjectRef(project_id=project_id, workspace_id=workspace_id)
        elif project_id is None and workspace_id is None:
            _project = None
        else:
            raise ProjectInvalidError(
                "Invalid project ID. Either explicitly pass workspace_id "
                "and project_id, or omit both"
            )

        # The DirtyGitRepo warning can be raised here.
        wf_def_model = self.model

        return _api.WorkflowRun(
            run_id=None,
            wf_def=wf_def_model,
            runtime=runtime,
            config=_config,
            project=_project,
        )

    def run(
        self,
        config: Optional[Union[_api.RuntimeConfig, str]] = None,
        project_dir: Optional[Union[str, Path]] = None,
        workspace_id: Optional[WorkspaceId] = None,
        project_id: Optional[ProjectId] = None,
    ) -> _api.WorkflowRun:
        """
        Schedules workflow for execution. Shorthand for
        `workflow.prepare().start()`.

        Args:
            config: SDK needs to know where to execute the workflow. This
                objects contains the required details.
            project_dir: the path to the project directory. If omitted, the current
                working directory is used.
            workspace_id: ID of the workspace for workflow - supported only on CE
            project_id: ID of the project for workflow - supported only on CE

        Raises:
            orquestra.sdk.exceptions.DirtyGitRepo: (warning) when a task def used by
                this workflow def has a "GitImport" and the git repo that contains it
                has uncommitted changes.
        """
        # This exists for users who have gotten used to doing `run()`. Once this has
        # been released, the following release should make config a required argument
        # and remove this check.
        if config is None:
            raise FutureWarning(
                "Please specify the runtime configuration for this run. "
                "The built in `local` and `in_process` configurations can be used by "
                'calling `run.("local")` and `run("in_process")` respectively. '
                "User defined configurations can be specified by providing the name "
                "under which they are saved, or passing in the RuntimeConfig object "
                "directly. "
            )
        run = self.prepare(
            config,
            project_dir=project_dir,
            workspace_id=workspace_id,
            project_id=project_id,
        )
        run.start()
        return run

    def with_resources(
        self,
        *,
        cpu: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        memory: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        disk: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        gpu: Optional[Union[str, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
        nodes: Optional[Union[int, _dsl.Sentinel]] = _dsl.Sentinel.NO_UPDATE,
    ) -> "WorkflowDef":
        """
        Assigns optional metadata related to this workflow definition object.

        Doesn't modify the existing workflow definition, returns a new one.

        Example usage:
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
        )


class WorkflowTemplate(Generic[_P, _R]):
    """
    Result of applying the `@workflow` decorator to a function.
    """

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
    ):
        self._custom_name = custom_name
        self._fn = workflow_fn
        self._fn_ref = fn_ref
        self._is_parametrized = is_parametrized
        self._resources = resources
        self._data_aggregation = data_aggregation
        self._default_source_import = default_source_import
        self._default_dependency_imports = default_dependency_imports

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> WorkflowDef[_R]:
        """
        Raises:
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
                warnings.warn_explicit(
                    f'"{called_fn.name}" is not a task. Did you mean to decorate '
                    "it with @task?",
                    NotATaskWarning,
                    called_fn.source_file or "<unknown>",
                    called_fn.line_no or 0,
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
        )

    @property
    def is_parametrized(self) -> bool:
        """
        A workflow is parametrized if the decorated function has parameters.
        If the function had no parameters, the workflow is not considered parametrized.
        """
        return self._is_parametrized

    @property
    def model(self) -> ir.WorkflowDef:
        """Serializable form of a workflow template (intermediate representation).

        returns:
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
            return self().model


# ----- decorator helpers -----
class _CalledFunction(NamedTuple):
    """Call made inside the workflow definition.
    Attributes:
        function : the function called inside the workflow
        name : name of the function called
        module_name : name of the module from where the function is
            imported from
        source_file : file where the function is defined
        line_no : line of the workflow definition where the function
            is called
    """

    function: Callable
    name: str
    module_name: Optional[str] = None
    source_file: Optional[str] = None
    line_no: Optional[int] = None


class Sentinel(Enum):
    NO_MODULE = object()


def _get_callable(
    fn: Callable, call_statement: List[NodeReference]
) -> Tuple[Union[Callable, None], Union[str, None]]:
    """Find the callable and the callable's module name.
    If the callable cannot be find then return None
    Args:
        fn : workflow function
        call_statement : List of NodeReferences with information
            about the call history
    Returns:
        _fn : Callable corresponding to the call_statement
        module_name : Name of the callable's module
    """
    found_module = Sentinel.NO_MODULE
    _fn = None
    module_name = None
    for node in call_statement:
        if node.node_type is NodeReferenceType.CALL:
            # For CALL nodes we try to retrieve the callable object
            if found_module is not Sentinel.NO_MODULE:
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
                if found_module is not Sentinel.NO_MODULE:
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


def _get_function_calls(fn: Callable) -> List[_CalledFunction]:
    """Get the functions that are called inside the workflow definition.
    This function uses some heuristics to find function calls using the
    workflow function's AST.
    If the function call cannot be identified, then is not returned in
    list of called functions.

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
    data_aggregation: Optional[Union[DataAggregation, bool]] = None,
    custom_name: Optional[str] = None,
    default_source_import: Optional[Import] = None,
    default_dependency_imports: Optional[Iterable[Import]] = None,
) -> Callable[[Callable[_P, _R]], WorkflowTemplate[_P, _R]]:
    ...


def workflow(
    fn: Optional[Callable[_P, _R]] = None,
    *,
    resources: Optional[_dsl.Resources] = None,
    data_aggregation: Optional[Union[DataAggregation, bool]] = None,
    custom_name: Optional[str] = None,
    default_source_import: Optional[Import] = None,
    default_dependency_imports: Union[Iterable[Import], Import, None] = None,
) -> Union[
    WorkflowTemplate[_P, _R],
    Callable[[Callable[_P, _R]], WorkflowTemplate[_P, _R]],
]:
    """Decorator that produces a workflow definition.

    Args:
        resources: !Unstable API! The resources that this workflow requires.
            The exact behaviour depends on the runtime, based on a Compute Engine
            workflow, you can set the cluster your workflow will use:
            10 nodes with 20 CPUs and a GPU each would be:
            resources=sdk.Resources(cpu="20", gpu="1", nodes=10)
            If omitted, the cluster's default resources will be used.
        data_aggregation: Used to set up resources used during data step. If skipped,
            or assigned True default values will be used. If assigned False
            data aggregation step will not run.
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
            data_aggregation=data_aggregation,
            default_source_import=default_source_import,
            default_dependency_imports=workflow_default_dependency_imports,
        )
        functools.update_wrapper(template, fn)
        return template

    if fn is None:
        return _inner
    else:
        return _inner(fn)
