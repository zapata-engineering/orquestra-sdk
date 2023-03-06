################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Transforms a DSL-based workflow into Intermediate Representation format.

The main highlight is `flatten_graph()`.
"""

import collections.abc
import hashlib
import re
import typing as t
from collections import OrderedDict, defaultdict
from functools import singledispatch

from pip_api import Requirement

import orquestra.sdk.schema.ir as model
from orquestra.sdk.schema import responses

from .. import exceptions
from . import _dsl, _git_url_utils, _workflow, serde

N_BYTES_IN_HASH = 8


def _make_key(obj: t.Any):
    """Returns a hashable key for all types

    BIG SCARY WARNING: Only use when all elements stay in scope
                       id() can return the same value for different objects if the
                       first has been de-allocated.
    """
    try:
        # We don't care about the result of hash()
        # just that we can successfully call it
        _ = hash(obj)
    except TypeError:
        # We use the id() of an object as the unhashable's key
        # This only makes sense because we know the lifetime of the objects we're
        # using. ids returned by id() can be reused during a program's lifetime
        return id(obj)
    else:
        # We use a tuple of the object itself and the type, if the object is hashable
        # The reason we do this is that it is possible to have multiple objects that
        # equal each other, but are different types.
        # We care about this because we end up serialising the objects for future
        # deserialisation.
        # If we have a hash collision between things that equal each other, but
        # serialise to different things, we will run into errors at deserialisation
        # time.
        # An example of this is:
        #   Python: 1 == 1.0 == True
        #           |     |      |
        #           JSON serialise
        #           v     v      v
        #   JSON:   1 != 1.0 != true
        return obj, type(obj)


def _hash_or_repr_bytes(obj: t.Any) -> bytes:
    return repr(obj).encode("utf-8")


def _gen_id_hash(*args):
    bytes = b"".join(map(_hash_or_repr_bytes, args))
    shake = hashlib.shake_256(bytes)
    return shake.hexdigest(5)


def _make_artifact_id(source_task: model.TaskDef, future_index: int):
    return _qe_compliant_name(
        f"artifact-{future_index}-{source_task.fn_ref.function_name}"
    )


GraphNode = t.Union[_dsl.ArtifactFuture, _dsl.Constant, _dsl.Secret]


class GraphTraversal:
    def __init__(self):
        self._artifacts = {}
        self._invocations = {}
        self._secrets = {}
        self._constants = {}

    def traverse(self, root_futures: t.Sequence[GraphNode]):
        """
        Traverse the workflow graph.

        We iterate over the futures returned from the workflow find the artifacts,
        constants, and secrets.
        """
        artifact_counter = 0
        secret_counter = 0
        constant_counter = 0
        for n in _iter_nodes(root_futures):
            if isinstance(n, _dsl.ArtifactFuture):
                self._artifacts[_make_key(n)] = _make_artifact_node(artifact_counter, n)
                artifact_counter += 1
                # Map the invocation to the future.
                # Note: Each unique future has one invocation, but each invocation
                #       can have many Futures.
                #       We're mapping `invocation: set(futures from invocation)`
                self._invocations.setdefault(n.invocation, set()).add(n)
            elif isinstance(n, _dsl.Secret):
                self._secrets[_make_key(n)] = model.SecretNode(
                    id=f"secret-{secret_counter}",
                    secret_name=n.name,
                    secret_config=n.config_name,
                )
                secret_counter += 1
            else:
                self._constants[_make_key(n)] = _make_constant_node(constant_counter, n)
                constant_counter += 1

    @property
    def artifacts(self):
        return self._artifacts

    @property
    def constants(self):
        return self._constants

    @property
    def invocations(self):
        return self._invocations

    @property
    def secrets(self):
        return self._secrets

    def get_argument_id(self, node: GraphNode):
        return self[node].id

    def __getitem__(self, node: GraphNode):
        key = _make_key(node)
        if key in self._artifacts:
            return self._artifacts[key]
        elif key in self._secrets:
            return self._secrets[key]
        elif key in self._constants:
            return self._constants[key]
        else:
            # In normal circumstances, this should never happen
            raise KeyError(node)  # pragma: no cover


def _iter_nodes(
    root_futures: t.Sequence[GraphNode],
) -> t.Iterator[t.Union[_dsl.ArtifactFuture, _dsl.Constant]]:
    traversal_list = [*root_futures]
    traversed_nodes = set()
    while traversal_list:
        current_node = traversal_list.pop()
        # _make_key ensures that we traverse each node only once
        traversed_nodes.add(_make_key(current_node))
        if isinstance(current_node, _dsl.ArtifactFuture):
            invocation = current_node.invocation
            for arg in [*invocation.args, *[arg for _, arg in invocation.kwargs]]:
                if _make_key(arg) not in traversed_nodes:
                    traversal_list.append(arg)
        yield current_node


@singledispatch
def _make_import_id(imp: _dsl.Import, import_hash: str):
    raise TypeError(f"Unknown import: {type(imp)}")


@_make_import_id.register
def _(imp: _dsl.LocalImport, import_hash: str):
    return f"local-{import_hash}"


@_make_import_id.register(_dsl.GitImport)
@_make_import_id.register(_dsl.GitImportWithAuth)
def _(imp: t.Union[_dsl.GitImport, _dsl.GitImportWithAuth], import_hash: str):
    # Remove git@, https://, and .git from repo_url
    proj_name = re.sub("https://|.git|git@", "", imp.repo_url)
    # Replace all non-alphanumeric characters with an underscore.
    # multiple special characters are grouped together with a single underscore
    # For example: github_com_zapatacomputing_orquestra_sdk
    proj_name = re.sub("[^A-Za-z0-9]+", "_", proj_name)
    return f"git-{import_hash}_{proj_name}"


@_make_import_id.register
def _(imp: _dsl.PythonImports, import_hash: str):
    # Imports might be too long to include in the ID
    return f"python-import-{import_hash}"


# Hashing inline imports is useless since it gives the same result each time. To ensure
# uniqueness in the system, use a global counter.
_global_inline_import_counter: int = 0


@_make_import_id.register
def _(imp: _dsl.InlineImport, import_hash: str):
    global _global_inline_import_counter
    _global_inline_import_counter += 1
    return f"inline-import-{_global_inline_import_counter}"


def _make_import_model(imp: _dsl.Import):
    # We should resolve the deferred git import before hashing
    if isinstance(imp, _dsl.DeferredGitImport):
        imp = imp.resolved()

    import_hash = _gen_id_hash(imp)
    id_ = _make_import_id(imp, import_hash)

    if isinstance(imp, _dsl.LocalImport):
        return model.LocalImport(
            id=id_,
        )
    elif isinstance(imp, _dsl.GitImport):
        return model.GitImport(
            id=id_,
            repo_url=imp.repo_url,
            git_ref=imp.git_ref,
        )
    elif isinstance(imp, _dsl.GitImportWithAuth):
        url = _git_url_utils.parse_git_url(imp.repo_url)
        url.user = imp.username
        if imp.auth_secret is not None:
            url.password = model.SecretNode(
                id=f"secret-{id_}",
                secret_name=imp.auth_secret.name,
                secret_config=imp.auth_secret.config_name,
            )
        return model.GitImport(
            id=id_,
            repo_url=url,
            git_ref=imp.git_ref,
        )
    elif isinstance(imp, _dsl.InlineImport):
        return model.InlineImport(id=id_)

    elif isinstance(imp, _dsl.PythonImports):
        reqs: t.List[Requirement] = imp.resolved()
        deps = []
        for req in reqs:
            x = model.PackageSpec(
                name=req.name,
                extras=sorted(list(req.extras)),
                version_constraints=sorted([str(spec) for spec in req.specifier]),
                environment_markers=str(req.marker) if req.marker else "",
            )
            deps.append(x)
        return model.PythonImports(id=id_, packages=deps, pip_options=[])

    else:
        raise ValueError(f"Invalid DSL import type: {type(imp)}")


def _make_resources_model(resources: _dsl.Resources):
    """Create a resources object of the IR based on a resources
    of the DSL. If no resources are allocated then returns None.
    Args:
        resources: resources object from the DSL
    Returns:
        resources object from the IR
    """
    return (
        model.Resources(
            cpu=resources.cpu,
            memory=resources.memory,
            disk=resources.disk,
            gpu=resources.gpu,
        )
        if not resources.is_empty()
        else None
    )


def _make_data_aggregation_model(data_aggregation: _dsl.DataAggregation):
    return model.DataAggregation(
        run=data_aggregation.run,
        resources=_make_resources_model(data_aggregation.resources),
    )


def _qe_compliant_name(name: str) -> str:
    """Make a guess of a name that's compliant with QE.

    Running a workflow remotely means submitting it to Quantum Engine. QE uses
    Kubernetes under the hood, and hence some of the IDs have constraints. See also:
        - https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
        - https://zapatacomputing.atlassian.net/browse/ORQSDK-367
        - https://docs.orquestra.io/quantum-engine/steps/#name

    Based on the requirements from QE we:
    Make sure there is nothing else then [a-z0-9] and dash (-)
    Make sure that string is no longer than 128 characters long
    """
    return re.sub(r"[^a-z\d\-]", "-", name.lower())[:128]


def _make_task_id(fn_name, suffix):
    return _qe_compliant_name(f"task-{fn_name}-{suffix}")


def _make_parameters(parameters: t.Optional[OrderedDict]):
    if parameters is not None:
        return [
            model.TaskParameter(name=p.name, kind=model.ParameterKind[p.kind.value])
            for p in parameters.values()
        ]
    else:
        return None


def _make_fn_ref(fn_ref: _dsl.FunctionRef) -> model.FunctionRef:
    if isinstance(fn_ref, _dsl.ModuleFunctionRef):
        return model.ModuleFunctionRef(
            module=fn_ref.module,
            function_name=fn_ref.function_name,
            file_path=fn_ref.file_path,
            line_number=fn_ref.line_number,
        )
    elif isinstance(fn_ref, _dsl.FileFunctionRef):
        return model.FileFunctionRef(
            file_path=fn_ref.file_path,
            function_name=fn_ref.function_name,
            line_number=fn_ref.line_number,
        )
    elif isinstance(fn_ref, _dsl.InlineFunctionRef):
        encoded_fn = serde.serialize_pickle(fn_ref.fn)
        return model.InlineFunctionRef(
            function_name=fn_ref.function_name,
            encoded_function=encoded_fn,
        )
    else:
        raise NotImplementedError(f"Unknown FunctionRef {type(fn_ref)}")


def _make_task_model(
    task: _dsl.TaskDef,
    imports_dict: t.Dict[_dsl.Import, model.Import],
) -> model.TaskDef:
    fn_ref_model = _make_fn_ref(task.fn_ref)

    source_import = imports_dict[task.source_import]

    dependency_import_ids: t.Optional[t.List[model.ImportId]]
    if task.dependency_imports is not None:
        # We need to keep track of the seen dependencies so we don't include duplicates.
        # Why don't we use a set? We currently treat the source_import separately and
        # we need to preserve the ordering of the dependency IDs.
        seen_ids = set([source_import.id])
        dependency_import_ids = []
        for imp in task.dependency_imports:
            dep_id = imports_dict[imp].id
            if dep_id not in seen_ids:
                seen_ids.add(dep_id)
                dependency_import_ids.append(dep_id)
    else:
        dependency_import_ids = None

    resources = _make_resources_model(task.resources)
    parameters = _make_parameters(task.parameters)

    task_contents_hash = _gen_id_hash(
        fn_ref_model,
        source_import.id,
        dependency_import_ids,
        resources,
        parameters,
    )

    return model.TaskDef(
        id=_make_task_id(
            task.__name__,
            task_contents_hash,
        ),
        fn_ref=fn_ref_model,
        source_import_id=source_import.id,
        dependency_import_ids=dependency_import_ids,
        resources=resources,
        parameters=parameters,
        custom_image=task.custom_image,
    )


def _make_artifact_node(
    future_index: int, future: _dsl.ArtifactFuture
) -> model.ArtifactNode:
    return model.ArtifactNode(
        id=_make_artifact_id(
            source_task=future.invocation.task,
            future_index=future_index,
        ),
        custom_name=future.custom_name,
        serialization_format=model.ArtifactFormat(future.serialization_format.value),
        artifact_index=future.output_index,
    )


def _get_nested_objects(obj) -> t.Iterable:
    """
    Figure out an object's neighbors in the reference graph using best-effort
    heuristics.
    """
    try:
        vs = vars(obj)
        return vs.values()
    except TypeError:
        # Some types' fields can't be inspected using `vars()` and
        # `__dict__()`. We workaround this below using a set of well-known
        # types.
        pass

    if isinstance(obj, collections.abc.Mapping):
        return [*obj.keys(), *obj.values()]
    elif isinstance(obj, str):
        return []
    elif isinstance(obj, collections.abc.Collection):
        return list(obj)
    else:
        return []


def _find_nested_objs_in_fields(root_obj, predicate) -> t.Sequence:
    """
    Traverses the `root_obj`'s reference graph, applies `predicate` over each
    node, and collects a list of nodes where the predicate was truthy.

    A "reference graph" means that each object holds references to its fields.
    Each field is another object with fields.
    """
    stack = [root_obj]
    collected = []
    # Required for robustness against cycles.
    seen_keys = set()

    while stack:
        focus = stack.pop()

        focus_key = _make_key(focus)
        if focus_key in seen_keys:
            continue

        seen_keys.add(focus_key)

        if predicate(focus):
            collected.append(focus)

        stack.extend(_get_nested_objects(focus))

    return collected


def _find_futures_in_container(
    constant_value: _dsl.Constant,
) -> t.Sequence[_dsl.ArtifactFuture]:
    return _find_nested_objs_in_fields(
        constant_value, lambda o: isinstance(o, _dsl.ArtifactFuture)
    )


def _preview_constant(constant: _dsl.Constant):
    return repr(constant)[:12]


def _make_constant_node(
    constant_index: int, constant_value: _dsl.Constant
) -> model.ConstantNode:
    if isinstance(constant_value, _dsl.TaskDef):
        raise exceptions.WorkflowSyntaxError(
            f"`{constant_value.__name__}` is a task definition and should be called "
            "before returning.\n Did you mean to call it inside a workflow?"
        )
    try:
        # Piggyback on the serialization we already implemented for artifacts.
        # Adding support for pickle constants would require adding a separate
        # schema for constants with chunked values. For more info, see and
        # compare:
        # - orquestra.sdk.schema.ir.ConstantNode
        # - orquestra.sdk.schema.responses.JSONResult
        # - orquestra.sdk.schema.responses.PickleResult
        result = serde.result_from_artifact(constant_value, model.ArtifactFormat.AUTO)
    except (TypeError, ValueError, NotImplementedError):
        futures = _find_futures_in_container(constant_value)
        task_fn_names = ", ".join(
            {f"`{fut.invocation.task.fn_ref.function_name}()`" for fut in futures}
        )
        if task_fn_names:
            raise exceptions.WorkflowSyntaxError(
                "We couldn't serialize part of the workflow related to the outputs "
                f"of {task_fn_names}. Looks like you may have put a task output "
                "inside a list, dict, or some other container. We don't support that "
                "yet. As a workaround, please create a separate task to wrap the "
                "value, or consider changing your previous task to return a wrapped "
                "value in the first place."
            )
        else:
            raise exceptions.WorkflowSyntaxError(
                "We couldn't serialize part of the workflow. Looks like you may have "
                f"used a non-serializable object: {constant_value}"
            )

    if isinstance(result, responses.JSONResult):
        return model.ConstantNodeJSON(
            id=f"constant-{constant_index}",
            value=result.value,
            value_preview=_preview_constant(constant_value),
            serialization_format=result.serialization_format,
        )
    elif isinstance(result, responses.PickleResult):
        return model.ConstantNodePickle(
            id=f"constant-{constant_index}",
            chunks=result.chunks,
            value_preview=_preview_constant(constant_value),
            serialization_format=result.serialization_format,
        )
    else:
        raise ValueError(
            "Only JSON-serializable and PICKLE-serializable workflow constants "
            f"are supported. {constant_value} ({type(constant_value)}) is not."
        )


def _make_invocation_id(task_name, invocation_i, custom_name):
    if custom_name is None:
        return _qe_compliant_name(f"invocation-{invocation_i}-task-{task_name}")
    else:
        return _qe_compliant_name(
            f"name-{custom_name}-invocation-{invocation_i}-task-{task_name}"
        )


def _sort_artifact_futures(artifact: _dsl.ArtifactFuture) -> int:
    if artifact.output_index is None:
        return -1
    else:
        return artifact.output_index


def _make_invocation_model(
    invocation: _dsl.TaskInvocation,
    invocation_index: int,
    task_models_dict: t.Dict[_dsl.TaskDef, model.TaskDef],
    graph: GraphTraversal,
):
    args_ids = [graph.get_argument_id(arg) for arg in invocation.args]

    kwargs_ids = {
        arg_name: graph.get_argument_id(arg_val)
        for arg_name, arg_val in invocation.kwargs
    }

    sorted_outputs = sorted(graph.invocations[invocation], key=_sort_artifact_futures)

    return model.TaskInvocation(
        id=_make_invocation_id(
            task_models_dict[invocation.task].fn_ref.function_name,
            invocation_index,
            invocation.custom_name,
        ),
        task_id=task_models_dict[invocation.task].id,
        args_ids=args_ids,
        kwargs_ids=kwargs_ids,
        output_ids=[graph[output_future].id for output_future in sorted_outputs],
        resources=_make_resources_model(invocation.resources),
        custom_image=invocation.custom_image,
    )


def _get_imports_from_task_def(
    task_def: _dsl.TaskDef,
) -> t.Dict[_dsl.Import, model.Import]:
    return {
        imp: _make_import_model(imp)
        for imp in [task_def.source_import, *(task_def.dependency_imports or [])]
    }


def get_model_from_task_def(task_def: _dsl.TaskDef) -> model.TaskDef:
    """Returns an IR TaskDef from an SDK TaskDef"""
    imports_dict = _get_imports_from_task_def(task_def)
    return _make_task_model(task_def, imports_dict)


def get_model_imports_from_task_def(task_def: _dsl.TaskDef) -> t.List[model.Import]:
    """Returns the IR Imports a SDK TaskDef requires
    Args:
        task_def: dsl.TaskDef
    Returns:
        A list of model.Import objects the task requires
    """
    return list(_get_imports_from_task_def(task_def).values())


def flatten_graph(
    workflow_def: _workflow.WorkflowDef,
    futures: t.Sequence[t.Union[_dsl.ArtifactFuture, _dsl.Constant]],
) -> model.WorkflowDef:
    """Traverse the nested linked list of futures and produce a flat graph.

    Each `dsl.ArtifactFuture` is mapped to a single `model.ArtifactNode`.
    Each `dsl.Constant` is mapped to a single `model.ConstantNode`.
    Each `dsl.TaskInvocation` is mapped to a single `model.TaskInvocation`.

    Unique task references from `dsl.TaskInvocation`s are mapped to `model.Task`s.

    Each `model.TaskInvocation` refers to nodes from `tasks`, `artifact_nodes` &
    `constant_nodes` by their ids. This allows deduplication of metadata if a single
    node is referenced by multiple invocations.
    """
    root_futures = futures

    graph = GraphTraversal()
    graph.traverse(root_futures)

    import_models_dict: t.Dict[_dsl.Import, model.Import] = {}

    # this dict is used to store already processed deferred git imports in the WF
    # As deferred git imports are fetching repos inside model creation, this is used
    # to avoid git fetch spam for the same repos over and over.
    cached_git_import_dict: t.Dict[t.Tuple, model.Import] = {}
    for invocation in graph.invocations.keys():
        for imp in [
            invocation.task.source_import,
            *(invocation.task.dependency_imports or []),
        ]:
            if imp not in import_models_dict:
                if isinstance(imp, _dsl.DeferredGitImport):
                    cashe_key = (imp.local_repo_path, imp.git_ref)
                    if cashe_key in cached_git_import_dict:
                        import_models_dict[imp] = cached_git_import_dict[cashe_key]
                    else:
                        model_import = _make_import_model(imp)
                        import_models_dict[imp] = model_import
                        cached_git_import_dict[cashe_key] = model_import
                else:
                    import_models_dict[imp] = _make_import_model(imp)

    task_models_dict: t.Dict[_dsl.TaskDef, model.TaskDef] = {
        invocation.task: _make_task_model(invocation.task, import_models_dict)
        for invocation in graph.invocations.keys()
    }

    dsl_invocations = list(graph.invocations.keys())

    invocation_models_dict: t.Dict[_dsl.TaskInvocation, model.TaskInvocation] = {
        dsl_invocation: _make_invocation_model(
            invocation=dsl_invocation,
            invocation_index=dsl_invocation_i,
            task_models_dict=task_models_dict,
            graph=graph,
        )
        for dsl_invocation_i, dsl_invocation in enumerate(dsl_invocations)
    }

    output_ids: t.List[t.Union[model.ConstantNodeId, model.ArtifactNodeId]] = []
    for output_future in futures:
        output_id = graph.get_argument_id(output_future)
        output_ids.append(output_id)
    return model.WorkflowDef(
        # At the moment 'orq submit workflow-def <name>' assumes that the <name> is
        # the same as the underlying function. Orquestra Studio seems to get it from
        # the 'orq get workflow-def', so for now we have to keep .name attribute same
        # as the function name.
        name=workflow_def.name,
        fn_ref=_make_fn_ref(workflow_def.fn_ref),
        imports={
            import_node.id: import_node for import_node in import_models_dict.values()
        },
        tasks={task_model.id: task_model for task_model in task_models_dict.values()},
        artifact_nodes={
            artifact_node.id: artifact_node
            for artifact_node in graph.artifacts.values()
        },
        secret_nodes={
            secret_node.id: secret_node for secret_node in graph.secrets.values()
        },
        constant_nodes={
            constant_node.id: constant_node
            for constant_node in graph.constants.values()
        },
        task_invocations={
            invocation.id: invocation for invocation in invocation_models_dict.values()
        },
        output_ids=output_ids,
        data_aggregation=_make_data_aggregation_model(workflow_def.data_aggregation)
        if workflow_def.data_aggregation
        else None,
    )
