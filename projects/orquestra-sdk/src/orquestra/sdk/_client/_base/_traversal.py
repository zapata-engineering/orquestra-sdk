################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
"""Transforms a DSL-based workflow into Intermediate Representation format.

The main highlight is `flatten_graph()`.
"""

import collections.abc
import hashlib
import inspect
import os
import re
import typing as t
import warnings
from collections import OrderedDict
from functools import singledispatch

from orquestra.workflow_shared import exceptions, parse_git_url, serde
from orquestra.workflow_shared.exec_ctx import workflow_build
from orquestra.workflow_shared.packaging import (
    get_current_python_version,
    get_current_sdk_version,
)
from orquestra.workflow_shared.schema import ir, responses
from orquestra.workflow_shared.secrets import Secret
from packaging import version
from pip_api._parse_requirements import Requirement

from . import _docker_images, _dsl, _workflow

N_BYTES_IN_HASH = 8


def _get_default_image(num_gpus: t.Optional[str]):
    image = _docker_images.DEFAULT_WORKER_IMAGE
    if num_gpus:
        image = f"{image}-cuda"
    return image


def _make_key(obj: t.Any):
    """Returns a hashable key for all types.

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


def _make_artifact_id(source_task: _dsl.TaskDef, wf_scoped_artifact_index: int):
    return _k8s_compliant_name(
        f"artifact-{wf_scoped_artifact_index}-{source_task._fn_name}"
    )


# DSL object that represents a data node in the workflow graph. Data nodes are
# constants, secrets, artifact futures, but not task invocations.
DSLDataNode = _dsl.Argument


class GraphTraversal:
    def __init__(self):
        self._secrets: t.MutableMapping[t.Hashable, ir.SecretNode] = {}
        self._constants: t.MutableMapping[t.Hashable, ir.ConstantNode] = {}

        # Running a task invocation results in values. The values are stored in
        # artifacts. Some of the values can be subscripted. Some of the values
        # can be left unused.
        #
        # If the task output is substritable, an invocation of an n-output task produces
        # n+1 artifacts: one for each subscripted output and one for non-subscripted
        # output.
        #
        # If the task output is not substriptable, an invocation produces one artifact.
        #
        # Use case example:
        # foo = task()
        # bar, baz, _ = foo
        # qux = task2(bar)
        # bla = task2(bar)
        #
        # In the example above, there are:
        # - 3 task invocations ("task()", "task2(bar)", and "task2(bar)")
        # - 5 futures ("foo", "bar", "baz", "qux", "bla")
        # - 6 artifacts:
        #     - 3 unpacked outputs "bar", "baz", "_"
        #     - 3 non-unpacked outputs "foo", "qux", "bla"

        # Each future points to an artifact. This field to powers "self.get_node_id()".
        self._future_artifacts: t.MutableMapping[
            _dsl.ArtifactFuture, ir.ArtifactNode
        ] = {}

        # Artifacts for "bar" and "baz" in the example above. This field powers
        # "self.artifacts". We need it in addition to "self._future_artifacts" because
        # some artifact nodes can be not referenced in the workflow function ("_" in
        # the example above).
        self._invocation_unpacked_artifacts: t.MutableMapping[
            _dsl.TaskInvocation, t.MutableSequence[ir.ArtifactNode]
        ] = {}

        # Artifacts for "foo" in the example above. This field powers "self.artifacts".
        # We need it in addition to "self._invocation_unpacked_artifacts" because of
        # "foo" in the example above.
        self._invocation_non_unpacked_artifacts: t.MutableMapping[
            _dsl.TaskInvocation, ir.ArtifactNode
        ] = {}

        # Needed to generate workflow-def-scoped artifact IDs.
        self._wf_artifact_counter = 0

    def _point_future_to_artifact(self, future: _dsl.ArtifactFuture):
        """Helper subroutine used in ``traverse()``.

        Mutates ``self._future_artifacts`` to point ``future`` to an already generated
        artifact node.
        """
        if future.output_index is not None:
            # This future is a result of unpacking another future ("bar"
            # and "baz" in the example above).
            self._future_artifacts[future] = self._invocation_unpacked_artifacts[
                future.invocation
            ][future.output_index]
        else:
            # This future is non-unpacked output ("foo" in the example
            # above).
            self._future_artifacts[future] = self._invocation_non_unpacked_artifacts[
                future.invocation
            ]

    def _gen_artifact(
        self,
        task_def: _dsl.TaskDef,
        custom_name: t.Optional[str],
        format: ir.ArtifactFormat,
        invocation_output_index: t.Optional[int],
    ) -> ir.ArtifactNode:
        """Creates artifact node models with workflow-def-scoped IDs."""
        artifact = ir.ArtifactNode(
            id=_make_artifact_id(
                source_task=task_def,
                wf_scoped_artifact_index=self._wf_artifact_counter,
            ),
            custom_name=custom_name,
            serialization_format=format,
            artifact_index=invocation_output_index,
        )
        self._wf_artifact_counter += 1

        return artifact

    def traverse(self, output_nodes: t.Sequence[DSLDataNode]):
        """Traverse the DSL workflow graph and collect the IR models.

        We iterate over the futures returned from the workflow find the artifacts,
        constants, and secrets.
        """
        secret_counter = 0
        constant_counter = 0

        seen_futures: t.MutableSet[_dsl.ArtifactFuture] = set()
        seen_invocations: t.MutableSet[_dsl.TaskInvocation] = set()

        for n in _iter_nodes(output_nodes):
            if isinstance(n, _dsl.ArtifactFuture):
                # The main point of handling artifact futures is to populate:
                # - self._future_artifacts
                # - self._invocation_unpacked_artifacts
                # - self._invocation_non_unpacked_artifacts

                # A single future can be used in multiple places in the workflow. We
                # only want to handle it once.
                if n in seen_futures:
                    continue

                out_meta = n.invocation.task._output_metadata

                # There can be many futures requiring to run the same task invocation.
                if n.invocation in seen_invocations:
                    # We've already handled the invocation, but not this future. We're
                    # supposed to have artifact nodes already generated for it. We just
                    # need to point "self._future_artifacts" to it.

                    if out_meta.is_subscriptable:
                        assert n.invocation in self._invocation_unpacked_artifacts

                    assert n.invocation in self._invocation_non_unpacked_artifacts

                    self._point_future_to_artifact(n)
                else:
                    # We haven't seen the invocation and haven't seen the future. We'll
                    # have to generate artifact nodes before we can point
                    # "self._future_artifacts" to it.

                    if out_meta.is_subscriptable:
                        # Generate artifact nodes for unpacking ("bar" and "baz" in the
                        # example above).
                        unpacked_artifacts = []
                        for output_i in range(out_meta.n_outputs):
                            default_format = ir.ArtifactFormat(
                                _dsl.ArtifactFuture.DEFAULT_SERIALIZATION_FORMAT.value
                            )
                            artifact = self._gen_artifact(
                                task_def=n.invocation.task,
                                custom_name=_dsl.ArtifactFuture.DEFAULT_CUSTOM_NAME,
                                format=default_format,
                                invocation_output_index=output_i,
                            )
                            unpacked_artifacts.append(artifact)
                        self._invocation_unpacked_artifacts[
                            n.invocation
                        ] = unpacked_artifacts

                    # Generate artifact node for non-unpacked use ("foo", "qux", "bla"
                    # in the example above).
                    artifact = self._gen_artifact(
                        task_def=n.invocation.task,
                        custom_name=n.custom_name,
                        format=ir.ArtifactFormat(n.serialization_format.value),
                        invocation_output_index=None,
                    )

                    self._invocation_non_unpacked_artifacts[n.invocation] = artifact

                    self._point_future_to_artifact(n)

                # due diligence
                seen_futures.add(n)
                seen_invocations.add(n.invocation)

            elif isinstance(n, Secret):
                self._secrets[_make_key(n)] = ir.SecretNode(
                    id=f"secret-{secret_counter}",
                    secret_name=n.name,
                    secret_config=n.config_name,
                    workspace_id=n.workspace_id,
                )
                secret_counter += 1
            else:
                self._constants[_make_key(n)] = _make_constant_node(constant_counter, n)
                constant_counter += 1

    @property
    def artifacts(self) -> t.Iterable[ir.ArtifactNode]:
        # Collect unpacked artifacts
        unpacked = (
            artifact
            for artifacts in self._invocation_unpacked_artifacts.values()
            for artifact in artifacts
        )
        # Collect non-unpacked artifacts
        non_unpacked = self._invocation_non_unpacked_artifacts.values()

        return (*unpacked, *non_unpacked)

    @property
    def constants(self) -> t.Iterable[ir.ConstantNode]:
        return self._constants.values()

    @property
    def invocations(self) -> t.Iterable[_dsl.TaskInvocation]:
        # Every seen invocation has a non-unpacked artifact so we can use this field.
        return self._invocation_non_unpacked_artifacts.keys()

    def output_ids_for_invocation(
        self, invocation: _dsl.TaskInvocation
    ) -> t.List[ir.ArtifactNodeId]:
        # Collect unpacked artifacts (a sequence)
        unpacked: t.Iterable[ir.ArtifactNode]
        if invocation.task._output_metadata.is_subscriptable:
            unpacked = (
                artifact for artifact in self._invocation_unpacked_artifacts[invocation]
            )
        else:
            unpacked = []

        # Get the non-unpacked artifact (a single one)
        non_unpacked = self._invocation_non_unpacked_artifacts[invocation]

        return [artifact.id for artifact in (*unpacked, non_unpacked)]

    @property
    def secrets(self) -> t.Iterable[ir.SecretNode]:
        return self._secrets.values()

    def get_node_id(self, node: DSLDataNode) -> ir.ArgumentId:
        key = _make_key(node)

        if isinstance(node, _dsl.ArtifactFuture):
            return self._future_artifacts[node].id

        elif isinstance(node, Secret):
            return self._secrets[key].id
        else:
            return self._constants[key].id


def _iter_nodes(
    root_futures: t.Sequence[DSLDataNode],
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
def _make_import_id(imp: _dsl.Import, import_hash: str) -> str:
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
    # For example: github_com_zapata_engineering_orquestra_sdk
    proj_name = re.sub("[^A-Za-z0-9]+", "_", proj_name)
    return f"git-{import_hash}_{proj_name}"


@_make_import_id.register
def _(imp: _dsl.PythonImports, import_hash: str):
    # Imports might be too long to include in the ID
    return f"python-import-{import_hash}"


# Hashing inline imports is useless since it gives the same result each time. To ensure
# uniqueness in the system, use a global counter.
_global_inline_import_counter: int = 0


def _global_inline_import_identifier():
    global _global_inline_import_counter
    _global_inline_import_counter += 1
    return _global_inline_import_counter


@_make_import_id.register
def inline_import(imp: _dsl.InlineImport, import_hash: str):
    id = _global_inline_import_identifier()
    return f"inline-import-{id}"


def _make_import_model(imp: _dsl.Import):
    # We should resolve the deferred git import before hashing
    if isinstance(imp, _dsl.DeferredGitImport):
        imp = imp.resolved()

    import_hash = _gen_id_hash(imp)
    id_ = _make_import_id(imp, import_hash)

    if isinstance(imp, _dsl.LocalImport):
        return ir.LocalImport(
            id=id_,
        )
    elif isinstance(imp, _dsl.GitImport):
        repo_url = parse_git_url(imp.repo_url)
        git_ref = (
            imp.git_ref.resolve()
            if isinstance(imp.git_ref, _dsl.DeferredGitRef)
            else imp.git_ref
        )
        return ir.GitImport(
            id=id_,
            repo_url=repo_url,
            git_ref=git_ref,
        )
    elif isinstance(imp, _dsl.GitImportWithAuth):
        url = parse_git_url(imp.repo_url)
        url.user = imp.username
        git_ref = (
            imp.git_ref.resolve()
            if isinstance(imp.git_ref, _dsl.DeferredGitRef)
            else imp.git_ref
        )
        if imp.auth_secret is not None:
            url.password = ir.SecretNode(
                id=f"secret-{id_}",
                secret_name=imp.auth_secret.name,
                secret_config=imp.auth_secret.config_name,
                workspace_id=imp.auth_secret.workspace_id,
            )
        return ir.GitImport(
            id=id_,
            repo_url=url,
            git_ref=git_ref,
            package_name=imp.package_name,
            extras=imp.extras,
        )
    elif isinstance(imp, _dsl.InlineImport):
        return ir.InlineImport(id=id_)

    elif isinstance(imp, _dsl.PythonImports):
        reqs: t.List[Requirement] = imp.resolved()
        deps = []
        for req in reqs:
            x = ir.PackageSpec(
                name=req.name,
                extras=sorted(list(req.extras)),
                version_constraints=sorted([str(spec) for spec in req.specifier]),
                environment_markers=str(req.marker) if req.marker else "",
            )
            deps.append(x)
        return ir.PythonImports(id=id_, packages=deps, pip_options=[])

    else:
        raise ValueError(f"Invalid DSL import type: {type(imp)}")


def _make_resources_model(
    resources: _dsl.Resources, is_task: bool = True
) -> t.Optional[ir.Resources]:
    """Create a resources object of the IR based on a resources of the DSL.

    If no resources are allocated then returns None.

    Args:
        resources: resources object from the DSL.
        is_task: toggle indicating whether the resources are for the workflow, or an
            individual task.

    Returns:
        resources object from the IR.
    """
    if resources.is_empty():
        return None

    if is_task and resources.nodes is not None:
        warnings.warn(
            exceptions.NodesInTaskResourcesWarning(
                "Tasks currently do not support the nodes resource."
            )
        )
        nodes = None
    else:
        nodes = resources.nodes

    return ir.Resources(
        cpu=resources.cpu,
        memory=resources.memory,
        disk=resources.disk,
        gpu=resources.gpu,
        nodes=nodes,
    )


def _make_data_aggregation_model(data_aggregation: _dsl.DataAggregation):
    return ir.DataAggregation(
        run=data_aggregation.run,
        resources=_make_resources_model(data_aggregation.resources),
    )


def _k8s_compliant_name(name: str) -> str:
    """Make a guess of a name that's compliant with k8s.

    Running a workflow remotely means submitting it to a k8s cluster.
    We have some constraints on the naming of tasks. See also:

        - https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
        - https://zapatacomputing.atlassian.net/browse/ORQSDK-367

    Based on the requirements we:

        - Make sure there is nothing else then [a-z0-9] and dash (-)
        - Make sure that string is no longer than 128 characters long
    """
    return re.sub(r"[^a-z\d\-]", "-", name.lower())[:128]


def _make_task_id(fn_name, suffix):
    return _k8s_compliant_name(f"task-{fn_name}-{suffix}")


def _make_parameters(parameters: t.Optional[OrderedDict]):
    if parameters is not None:
        return [
            ir.TaskParameter(name=p.name, kind=ir.ParameterKind[p.kind.value])
            for p in parameters.values()
        ]
    else:
        return None


def _make_fn_ref(fn_ref: _dsl.FunctionRef) -> ir.FunctionRef:
    if isinstance(fn_ref, _dsl.ModuleFunctionRef):
        return ir.ModuleFunctionRef(
            module=fn_ref.module,
            function_name=fn_ref.function_name,
            file_path=fn_ref.file_path,
            line_number=fn_ref.line_number,
        )
    elif isinstance(fn_ref, _dsl.FileFunctionRef):
        return ir.FileFunctionRef(
            file_path=fn_ref.file_path,
            function_name=fn_ref.function_name,
            line_number=fn_ref.line_number,
        )
    elif isinstance(fn_ref, _dsl.InlineFunctionRef):
        module = inspect.getmodule(fn_ref.fn)
        with serde.registered_module(module):
            encoded_fn = serde.serialize_pickle(fn_ref.fn)
        return ir.InlineFunctionRef(
            function_name=fn_ref.function_name,
            encoded_function=encoded_fn,
        )
    else:
        raise NotImplementedError(f"Unknown FunctionRef {type(fn_ref)}")


def _make_task_model(
    task: _dsl.TaskDef,
    imports_dict: t.Dict[_dsl.Import, ir.Import],
    mandatory_imports: t.Optional[t.List[ir.Import]] = None,
) -> ir.TaskDef:
    # fn_ref and source_imports are completely resolved during
    # creation of import_models_dict. At this point every task should have
    # those variables set - and they are needed to make task model
    # mandatory_imports are imports that will be added additionally to every task
    assert task._fn_ref is not None
    assert task._source_import is not None

    fn_ref_model = _make_fn_ref(task._fn_ref)

    source_import = imports_dict[task._source_import]

    dependency_import_ids: t.Optional[t.List[ir.ImportId]]
    if task._dependency_imports is not None:
        # We need to keep track of the seen dependencies so we don't include duplicates.
        # Why don't we use a set? We currently treat the source_import separately and
        # we need to preserve the ordering of the dependency IDs.
        seen_ids = set([source_import.id])
        dependency_import_ids = []
        for imp in task._dependency_imports:
            dep_id = imports_dict[imp].id
            if dep_id not in seen_ids:
                seen_ids.add(dep_id)
                dependency_import_ids.append(dep_id)
    else:
        dependency_import_ids = None

    # we are adding mandatory imports at the end of the list on purpose -
    # we want to make sure those are actually installed last on the worker as we treat
    # them as mandatory
    if mandatory_imports:
        mandatory_import_ids = [imp.id for imp in mandatory_imports]
        if not dependency_import_ids:
            dependency_import_ids = mandatory_import_ids
        else:
            dependency_import_ids.extend(mandatory_import_ids)

    resources = _make_resources_model(task._resources)
    parameters = _make_parameters(task._parameters)

    task_contents_hash = _gen_id_hash(
        fn_ref_model,
        source_import.id,
        dependency_import_ids,
        resources,
        parameters,
    )

    return ir.TaskDef(
        id=_make_task_id(
            task.__name__,
            task_contents_hash,
        ),
        fn_ref=fn_ref_model,
        output_metadata=ir.TaskOutputMetadata(
            is_subscriptable=task._output_metadata.is_subscriptable,
            n_outputs=task._output_metadata.n_outputs,
        ),
        source_import_id=source_import.id,
        dependency_import_ids=dependency_import_ids,
        resources=resources,
        parameters=parameters,
        custom_image=task._custom_image,
        max_retries=task._max_retries,
    )


def _get_nested_objects(obj) -> t.Iterable:
    """Figure out an object's neighbors in the reference graph.

    Note: This function uses best-effort heuristics.
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
    """Traverses the `root_obj`'s reference graph, applies `predicate` over each
    node, and collects a list of nodes where the predicate was truthy.

    A "reference graph" means that each object holds references to its fields.
    Each field is another object with fields.
    """  # noqa: D205
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
) -> ir.ConstantNode:
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
        result = serde.result_from_artifact(constant_value, ir.ArtifactFormat.AUTO)
    except (TypeError, ValueError, NotImplementedError):
        futures = _find_futures_in_container(constant_value)
        task_fn_names = ", ".join(
            {f"`{fut.invocation.task._fn_name}()`" for fut in futures}
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
        return ir.ConstantNodeJSON(
            id=f"constant-{constant_index}",
            value=result.value,
            value_preview=_preview_constant(constant_value),
            serialization_format=result.serialization_format,
        )
    elif isinstance(result, responses.PickleResult):
        return ir.ConstantNodePickle(
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
        return _k8s_compliant_name(f"invocation-{invocation_i}-task-{task_name}")
    else:
        return _k8s_compliant_name(
            f"name-{custom_name}-invocation-{invocation_i}-task-{task_name}"
        )


def _make_invocation_model(
    invocation: _dsl.TaskInvocation,
    invocation_index: int,
    task_models_dict: t.Dict[_dsl.TaskDef, ir.TaskDef],
    graph: GraphTraversal,
):
    args_ids = [graph.get_node_id(arg) for arg in invocation.args]

    kwargs_ids = {
        arg_name: graph.get_node_id(arg_val) for arg_name, arg_val in invocation.kwargs
    }

    gpu_used: t.Optional[str]
    if invocation.resources.gpu:
        gpu_used = invocation.resources.gpu
    elif task_models_dict[invocation.task].resources is not None:
        task_model = task_models_dict[invocation.task]
        # this is just to silence pyright which doesn't believe elif check
        # we dont multiprocess that variables, so we dont have race conditions here
        assert task_model.resources is not None
        gpu_used = task_model.resources.gpu
    else:
        gpu_used = None

    custom_image = (
        invocation.custom_image
        or task_models_dict[invocation.task].custom_image
        or _get_default_image(gpu_used)
    )
    return ir.TaskInvocation(
        id=_make_invocation_id(
            task_models_dict[invocation.task].fn_ref.function_name,
            invocation_index,
            invocation.custom_name,
        ),
        task_id=task_models_dict[invocation.task].id,
        args_ids=args_ids,
        kwargs_ids=kwargs_ids,
        output_ids=graph.output_ids_for_invocation(invocation),
        resources=_make_resources_model(invocation.resources),
        custom_image=custom_image,
        env_vars=invocation.env_vars,
    )


def extract_root_futures(wf_def: _workflow.WorkflowDef) -> t.Sequence[_dsl.Argument]:
    """Executes the ``wf_def`` function to get the workflow output futures."""
    with workflow_build():
        futures = wf_def._fn(*wf_def._workflow_args, **wf_def._workflow_kwargs)

    if not isinstance(futures, collections.abc.Sequence) or isinstance(futures, str):
        return (futures,)
    else:
        return futures


def flatten_graph(
    workflow_def: _workflow.WorkflowDef,
    futures: t.Sequence[t.Union[_dsl.ArtifactFuture, _dsl.Constant]],
) -> ir.WorkflowDef:
    """Traverse the nested linked list of futures and produce a flat graph.

    Each ``dsl.ArtifactFuture`` is mapped to a single ``model.ArtifactNode``.
    Each ``dsl.Constant`` is mapped to a single ``model.ConstantNode``.
    Each ``dsl.TaskInvocation`` is mapped to a single ``model.TaskInvocation``.

    Unique task references from ``dsl.TaskInvocation`` s are mapped to ``model.Task`` s.

    Each ``model.TaskInvocation`` refers to nodes from ``tasks``, ``artifact_nodes`` &
    ``constant_nodes`` by their ids. This allows deduplication of metadata if a single
    node is referenced by multiple invocations.
    """
    root_futures = futures

    graph = GraphTraversal()
    graph.traverse(root_futures)

    import_models_dict: t.Dict[_dsl.Import, ir.Import] = {}

    # this dict is used to store already processed deferred git imports in the WF
    # As deferred git imports are fetching repos inside model creation, this is used
    # to avoid git fetch spam for the same repos over and over.
    cached_git_import_dict: t.Dict[t.Tuple, ir.Import] = {}
    for invocation in graph.invocations:
        invocation.task._resolve_task_source_data(workflow_def.default_source_import)
        default_deps = (
            tuple(workflow_def.default_dependency_imports)
            if workflow_def.default_dependency_imports
            else None
        )
        invocation.task._resolve_task_dependencies(default_deps)
        for imp in [
            invocation.task._source_import,
            *(invocation.task._dependency_imports or []),
        ]:
            if imp not in import_models_dict:
                if isinstance(imp, _dsl.DeferredGitImport):
                    cache_key = (imp.local_repo_path, imp.git_ref)
                    if cache_key in cached_git_import_dict:
                        import_models_dict[imp] = cached_git_import_dict[cache_key]
                    else:
                        model_import = _make_import_model(imp)
                        import_models_dict[imp] = model_import
                        cached_git_import_dict[cache_key] = model_import
                else:
                    import_models_dict[imp] = _make_import_model(imp)

    sdk_version = get_current_sdk_version()
    sdk_version_parsed = version.parse(sdk_version.original)

    if not (sdk_version_parsed.is_devrelease or sdk_version_parsed.is_prerelease):
        sdk_python_import = _dsl.PythonImports(
            f"orquestra-sdk[all]=={sdk_version.original}"
        )
    else:
        # this only happens on dev environment, should not happen on users' machines
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="You're working on detached HEAD")
            warnings.filterwarnings("ignore", message="You have uncommitted changes")
            # this is a workaround where .model is called outside of SDK repo
            # its save to do as this should only happen if SDK is installed as editable
            # install (or from git repository)
            path_to_sdk = os.path.realpath(__file__)
            git_ref = _dsl.infer_git_ref(path_to_sdk).resolve()
            sdk_python_import = _dsl.GithubImport(
                git_ref=git_ref,
                repo="zapata-engineering/orquestra-sdk",
                package_name="orquestra-sdk",
                extras="all",
            )

    ir_sdk_import = _make_import_model(sdk_python_import)
    import_models_dict[sdk_python_import] = ir_sdk_import

    task_models_dict: t.Dict[_dsl.TaskDef, ir.TaskDef] = {
        invocation.task: _make_task_model(
            invocation.task, import_models_dict, [ir_sdk_import]
        )
        for invocation in graph.invocations
    }
    # make sure we can execute tasks
    for task in task_models_dict:
        task.validate_task()

    dsl_invocations = list(graph.invocations)

    invocation_models_dict: t.Dict[_dsl.TaskInvocation, ir.TaskInvocation] = {
        dsl_invocation: _make_invocation_model(
            invocation=dsl_invocation,
            invocation_index=dsl_invocation_i,
            task_models_dict=task_models_dict,
            graph=graph,
        )
        for dsl_invocation_i, dsl_invocation in enumerate(dsl_invocations)
    }

    sdk_version = get_current_sdk_version()

    python_version = get_current_python_version()

    return ir.WorkflowDef(
        metadata=ir.WorkflowMetadata(
            sdk_version=sdk_version,
            python_version=python_version,
            head_node_image=workflow_def._head_node_image
            if workflow_def._head_node_image is not None
            else _docker_images.HEAD_NODE_IMAGE,
        ),
        resources=_make_resources_model(workflow_def._resources, is_task=False),
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
            artifact_node.id: artifact_node for artifact_node in graph.artifacts
        },
        secret_nodes={secret_node.id: secret_node for secret_node in graph.secrets},
        constant_nodes={
            constant_node.id: constant_node for constant_node in graph.constants
        },
        task_invocations={
            invocation.id: invocation for invocation in invocation_models_dict.values()
        },
        output_ids=[graph.get_node_id(output_future) for output_future in futures],
        data_aggregation=_make_data_aggregation_model(workflow_def.data_aggregation)
        if workflow_def.data_aggregation
        else None,
    )
