################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""This module exposes some machinery for loading workflow_defs.

It is not intended to be used as a Public API and is not exposed by the orquestra.sdk
namespace.
The only reason this is not a private module is that it is used by downstream projects,
such as orquestra-lsp.
These internals are subject to change.
"""

import importlib
import importlib.machinery
import sys
import types
import typing as t
from importlib import abc

from orquestra.workflow_shared import dispatch


class FakeImportedAttribute:
    MSG = "This object has been faked by the import loader"

    def __getitem__(self, _):
        raise RuntimeError(self.MSG)

    def __getattr__(self, _):
        raise RuntimeError(self.MSG)

    def __call__(self, *_, **__):
        raise RuntimeError(self.MSG)


class ImportFaker(abc.MetaPathFinder):
    """ImportFaker implements a 'meta path finder'.

    This is used by Python to find out where a module should be loaded from and, in
    this specific case, returns a 'module spec' for a faked module.
    """

    def __init__(self, block_list: t.List[str] = []):
        """Inits the ImportFaker with a block list.

        Args:
            block_list: a list of module names to block.
                The format of the module names should be the dotted module name, for
                example: 'orquestra.sdk'.
                The block list will be used to check that a requested module does not
                start any of the entries in the list.
                For example, if 'orq' is in the block list, 'orquestra.sdk',
                'orquestra.quantum', etc. will be loaded as fake modules.
        """
        self._loader = FakeLoader()
        self.block_list = block_list

    def find_spec(self, fullname: str, path: t.Any, target: t.Any = None):
        """Implements the interface for a meta path finder.

        Args:
            fullname: the 'dotted' syntax of the module, for example 'orquestra.sdk'.
            path: not used - the __path__ from a subpackage or module's parent.
            target: not used - meta path finders can use a module object as a hint.
        """
        if any(fullname.startswith(blocked) for blocked in self.block_list):
            return self._gen_spec(fullname)
        return None

    def _gen_spec(self, fullname):
        """Returns a spec for a faked module."""
        spec = importlib.machinery.ModuleSpec(fullname, self._loader)
        return spec

    def unload_fakes(self):
        """Unloads all fakes the ImportFaker has created."""
        self._loader.unload_fakes()


class DummyModule(types.ModuleType):
    """A module that always returns fake objects on attribute access."""

    __path__ = []

    def __getattr__(self, name: str):
        return FakeImportedAttribute()


class FakeLoader(abc.Loader):
    """Implements a module loader for faked modules."""

    def __init__(self):
        self._fake_modules = {}

    def create_module(self, spec):
        """Creates a fake module from a given module spec."""
        dummy = DummyModule(spec.name)
        self._fake_modules[spec.name] = dummy
        return dummy

    def exec_module(self, module):
        """Called when a module is imported or reloaded."""
        pass

    def unload_fakes(self):
        """Unloads all modules that this loader has loaded."""
        for name in self._fake_modules.keys():
            del sys.modules[name]
        self._fake_modules = {}


WORKFLOW_DEFS_MODULE = "workflow_defs"
# In the future, we may add time-consuming imports to this list.
# Any addition here must be communicated to SDK users
DEFAULT_IMPORT_BLOCK_LIST: t.List[str] = []


def get_workflow_defs_module(
    directory: str, import_block_list: t.Optional[t.List[str]] = None
) -> types.ModuleType:
    """Return the module containing the current project's workflow definitions.

    Args:
        directory: path to a directory with a workflow_defs.py file. The directory is
            added to the interpreter's `sys.path`.
        import_block_list: a list of module names to block.

    Raises:
        FileNotFoundError: if the workflow defs module wasn't found.

    Returns:
        The loaded workflow_defs module.
    """
    dispatch.ensure_sys_paths([directory])

    # Create a fake loader to stop Python importing blocked libraries
    # Note: any libraries already imported are not faked!
    # But this should not cause a slow down
    block_list = import_block_list or DEFAULT_IMPORT_BLOCK_LIST
    finder = ImportFaker(block_list=block_list)
    needle: t.Any = importlib.machinery.PathFinder
    sys.meta_path.insert(sys.meta_path.index(needle), finder)  # typing: ignore

    try:
        module = dispatch.load_module_or_help_debugging(WORKFLOW_DEFS_MODULE)
    except FileNotFoundError:
        raise
    finally:
        # Unload all fake libraries and remove the fake importer, even if an
        # exception happened.
        finder.unload_fakes()
        if finder in sys.meta_path:
            sys.meta_path.remove(finder)

    # Without this the module is cached and any future "import ..." would
    # return the cached module object instead of re-running the module code
    # again. This is bad, because right above we've faked the imports and this
    # should only be in effect at the time of running "orq submit workflow-def"
    # to make it quicker.
    del sys.modules[module.__name__]

    return module


def get_attributes_of_type(target_object: object, target_type: type) -> t.List:
    """Get all attributes of an object that are instances of a given type.

    Args:
        target_object: The object whose attributes are to be extracted.
        target_type: The type of attributes to be extracted.

    Returns:
        A list containing all attributes of target_object that are instances of
            target_type.
    """
    return [
        attribute
        for attribute in target_object.__dict__.values()
        if isinstance(attribute, target_type)
    ]
