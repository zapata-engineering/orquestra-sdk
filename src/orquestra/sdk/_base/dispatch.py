################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import copy
import importlib
import importlib.util
import json
import os
import sys
import typing as t
from functools import singledispatch
from pathlib import Path

from orquestra.sdk._base import _exec_ctx, serde
from orquestra.sdk.schema import ir


def _locate_callable(fn_ref_dict) -> t.Callable:
    fn_ref: ir.FunctionRef
    if fn_ref_dict["type"] == "MODULE_FUNCTION_REF":
        fn_ref = ir.ModuleFunctionRef.parse_obj(fn_ref_dict)
        return locate_fn_ref(fn_ref)

    elif fn_ref_dict["type"] == "FILE_FUNCTION_REF":
        fn_ref = ir.FileFunctionRef.parse_obj(fn_ref_dict)
        # There's some kerkuffle with prepending components to the script file path.
        # Here, we assume that the fn_ref already contains a valid filepath relative to
        # the pwd. Hence, we don't need to pass a custom search path to
        # `locate_fn_ref()`.
        return locate_fn_ref(fn_ref)
    elif fn_ref_dict["type"] == "INLINE_FUNCTION_REF":
        fn_ref = ir.InlineFunctionRef.parse_obj(fn_ref_dict)
        # We need to deserialize the pickled function
        return locate_fn_ref(fn_ref)
    else:
        raise ValueError(
            f"fn_ref_dict needs to be consistent with {ir.FunctionRef}. "
            f"Got: {fn_ref_dict}"
        )


@singledispatch
def locate_fn_ref(fn_ref: ir.FunctionRef, search_path=".") -> t.Callable:
    """Loads appropriate module and returns the function callable.

    In case of `ModuleFunctionRef`, the function will be loaded from
    `fn_ref.module_name`.

    In case of `FileFunctionRef` it's sometimes important to look for the scripts in
    directories other than cwd's subdirs. Use `search_path` for this.

    If your file fn ref links to a function in "lib/tasks.py" and `search_path` is set
    to "." (default), the function will be loaded from "./lib/tasks.py".

    If your file fn ref links to a function in "lib/tasks.py" and `search_path` is set
    to "/app/steps", the function will be loaded from "/app/steps/lib/tasks.py".
    """
    raise ValueError(f"Can't locate {fn_ref}")


def load_module_or_help_debugging(module_name):
    if module_name in sys.modules:
        try:
            # Fix when we have cached module in Ray Worker, but we already changed
            # system directories. Make sure we have proper function definitions now.
            return importlib.reload(sys.modules[module_name])
        except ModuleNotFoundError:
            # The module is already loaded, but we cannot reload it. Can happen
            # for example on case of __main__ module dereference. Return loaded module
            return sys.modules[module_name]
    else:
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as e:
            non_dirs_on_sys_path = [p for p in sys.path if not os.path.isdir(p)]
            raise ModuleNotFoundError(
                f"No module named {e.name}. "
                f"Pwd: '{os.getcwd()}'. "
                f"Full sys.path: {sys.path}.",
                f"Non-directories on sys.path: {non_dirs_on_sys_path}",
                name=e.name,
            ) from e


@locate_fn_ref.register
def _locate_module_fn_ref(fn_ref: ir.ModuleFunctionRef) -> t.Callable:
    module = load_module_or_help_debugging(fn_ref.module)
    return getattr(module, fn_ref.function_name)


WORKLFOW_DEFS_MODULE_NAME = "__sdk_workflow_defs_module"


@locate_fn_ref.register
def _locate_file_fn_ref(fn_ref: ir.FileFunctionRef, search_path=".") -> t.Callable:
    spec = importlib.util.spec_from_file_location(
        WORKLFOW_DEFS_MODULE_NAME,
        os.path.join(search_path, fn_ref.file_path),
    )
    if spec is None:
        raise ModuleNotFoundError(f"Can't find module referenced by {fn_ref}")

    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    loader: t.Any = spec.loader
    loader.exec_module(module)

    # We need to cache the module. Otherwise, pickle loader would re-run the
    # file each time we do `pickle.load()`. This would cause errors in
    # `pickle.load()`, because the class definitions would be re-ran and their
    # pointers would change. Pickle doesn't like that.
    sys.modules[WORKLFOW_DEFS_MODULE_NAME] = module

    return getattr(module, fn_ref.function_name)


@locate_fn_ref.register
def _locate_inline_fn_ref(fn_ref: ir.InlineFunctionRef) -> t.Callable:
    return serde.deserialize_pickle(fn_ref.encoded_function)


SUPPORTED_FORMATS = {
    ir.ArtifactFormat.AUTO,
    ir.ArtifactFormat.JSON,
    ir.ArtifactFormat.ENCODED_PICKLE,
}


def _make_path(artifact: ir.ArtifactNode, artifacts_dir):
    assert (
        artifact.serialization_format in SUPPORTED_FORMATS
    ), f"{artifact.serialization_format} is unsupported"

    if artifacts_dir:
        return os.path.join(artifacts_dir, f"{artifact.id}.json")
    else:
        return f"{artifact.id}.json"


def ensure_sys_paths(additional_paths: t.Sequence[str]):
    # TODO: export a yaml and put it inside smoke tests so we get early alarm
    # when the PWD on QE changes.
    for new_path in reversed(additional_paths):
        if new_path in sys.path:
            sys.path.remove(new_path)
        sys.path.insert(0, new_path)


SERIALIZATION_FORMAT_MAGIC_KEY = "serialization_format"


def exec_task_fn(
    *fn_args,
    __sdk_fn_ref_dict: t.Dict,
    __sdk_output_node_dicts: t.Iterable[t.Dict],
    __sdk_positional_args_ids: t.Iterable[str],
    __sdk_additional_sys_paths: t.Optional[t.Sequence[str]] = None,
    __sdk_artifacts_dir: t.Optional[str] = None,
    **fn_kwargs,
):
    """Huge workaround for backwards compatibility with Orquestra v1.

    With the new SDK/Orquestra v2 we want to handle serialization of the
    outputs automatically. Orquestra v1 doesn't do that, it assumes that the steps
    would make the file writing themselves.

    The workaround is that whenever we export a workflow to v1, we use this function as
    the step function, and pass the actual function the user wanted to execute
    as reference.

    Args:
        *fn_args: these are the positional arguments passed to the dispatcher.
            These are not used and this will be removed soon.

        __sdk_fn_ref_dict: reference that can be used to locate the task body function.
            This is a dict that should be consistent with
            `orquestra.sdk.schema.ir.FunctionRef`

        __sdk_output_node_dicts: Workflow graph related nodes. These are used for
            automatic serialization of task outputs.
            Each dict should be consistent with `orquestra.sdk.schema.ir.ArtifactNode`
            We're using dicts because this needs to be embedded inside v1 yaml workflow
            as constants.

        __sdk_positional_args_ids: list of node IDs of in the workflow graph used as
            positional arguments.
            If you have built a workflow with the new SDK with positional arguments in
            your task invocations, this allows you to run your workflow on QE.

        __sdk_additional_sys_paths: optional paths to be added to `sys.path` before
            executing task function.

        __sdk_artifcats_dir: optional path to a base directory where the artifacts will
            be stored

        **fn_kwargs: these are the keyword arguments passed to the dispatcher. All
            other inputs in the YAML are passed as keyword arguments to the dispatcher.
            This includes: - keyword arguments for the task body function
                           - artifact/constant nodes from the workflow graph
            Collisions are not explicitly avoided, but our artifact/constant nodes
            have `-` in their names, which is not a valid identifier and Python
            functions should not have parameters with names including `-`.
    """
    # --- Phase 1: get the callable ---
    # --- Phase 1.1: make sure Python interpreter knows where to look for modules ---
    if __sdk_additional_sys_paths:
        ensure_sys_paths(__sdk_additional_sys_paths)

    # --- Phase 1.2: dereferencing. Load the appropriate module and get the function ---
    callable: t.Any = _locate_callable(__sdk_fn_ref_dict)

    # dsl.task() wraps a callable in a Task object. We need to call the
    # underlying function, not the Task object.
    try:
        fn = callable._TaskDef__sdk_task_body
    except AttributeError:
        fn = callable

    # --- Phase 2: acquire the arguments ---
    # --- Phase 2.1: transform the arguments that need it ---
    keyword_args = copy.deepcopy(fn_kwargs)

    for yaml_input_name, yaml_value in keyword_args.items():
        # If the input is a string and matches a path that exists then this is a value
        # passed from another step so we have to load the file.
        if isinstance(yaml_value, str) and Path(yaml_value).exists():
            with open(yaml_value) as f:
                yaml_value = json.load(f)

        # If the value embedded in the YAML contains the magic key we deserialize it.
        # Otherwise we assume that it should be passed as-is to the dispatched function.
        #
        # Note that there are two levels of serialization – one is turning an argument
        # into a string using one of ways specified in the ArtifactFormat enum. Apart
        # from this we also need to store metadata information, e.g. whether we used
        # JSON or pickle serialization.
        if (
            isinstance(yaml_value, t.Mapping)
            and SERIALIZATION_FORMAT_MAGIC_KEY in yaml_value
        ):
            embedded_value = serde.value_from_result_dict(yaml_value)
            keyword_args[yaml_input_name] = embedded_value

    # --- Phase 2.2: make some **kwargs work as *args ---
    positional_args = []
    for positional_arg_id in __sdk_positional_args_ids:
        positional_args.append(keyword_args[positional_arg_id])

    # cleanup keyword args from positional arguments
    for positional_arg_id in __sdk_positional_args_ids:
        try:
            del keyword_args[positional_arg_id]
        except KeyError:
            # Positional argument already removed from keyword args
            pass

    # --- Phase 3: execute! ---
    # --- Phase 3.1: set execution context flag ---
    with _exec_ctx.platform_qe():
        # --- Phase 3.2: call user's function ---
        ret_obj = fn(*positional_args, *fn_args, **keyword_args)

    # --- Phase 4: save the outputs ---
    ret_nodes = [
        ir.ArtifactNode.parse_obj(node_dict) for node_dict in __sdk_output_node_dicts
    ]

    for ret_node in ret_nodes:
        if ret_node.artifact_index is None:
            ret_val = ret_obj
        else:
            ret_val = ret_obj[ret_node.artifact_index]
        result = serde.result_from_artifact(
            artifact_value=ret_val, artifact_format=ret_node.serialization_format
        )
        with open(_make_path(ret_node, __sdk_artifacts_dir), "w") as f:
            f.write(result.json())
