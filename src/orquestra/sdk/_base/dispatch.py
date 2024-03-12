################################################################################
# © Copyright 2021 - 2024 Zapata Computing Inc.
################################################################################
import importlib
import importlib.util
import os
import sys
import typing as t
from functools import singledispatch

from orquestra.sdk._base import serde
from orquestra.sdk.schema import ir


def _locate_callable(fn_ref_dict) -> t.Callable:
    fn_ref: ir.FunctionRef
    if fn_ref_dict["type"] == "MODULE_FUNCTION_REF":
        fn_ref = ir.ModuleFunctionRef.model_validate(fn_ref_dict)
        return locate_fn_ref(fn_ref)

    elif fn_ref_dict["type"] == "FILE_FUNCTION_REF":
        fn_ref = ir.FileFunctionRef.model_validate(fn_ref_dict)
        # There's some kerkuffle with prepending components to the script file path.
        # Here, we assume that the fn_ref already contains a valid filepath relative to
        # the pwd. Hence, we don't need to pass a custom search path to
        # `locate_fn_ref()`.
        return locate_fn_ref(fn_ref)
    elif fn_ref_dict["type"] == "INLINE_FUNCTION_REF":
        fn_ref = ir.InlineFunctionRef.model_validate(fn_ref_dict)
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


def ensure_sys_paths(additional_paths: t.Sequence[str]):
    for new_path in reversed(additional_paths):
        if new_path in sys.path:
            sys.path.remove(new_path)
        sys.path.insert(0, new_path)
