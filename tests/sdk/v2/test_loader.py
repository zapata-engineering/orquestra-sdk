################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import sys
from importlib.machinery import PathFinder

import pytest

import orquestra.sdk as dsl
import orquestra.sdk._base.loader as loader

WORKFLOW_DEFS = """import orquestra.sdk as sdk
@sdk.task
def hello():
    return 1
@sdk.workflow
def wf():
    return [hello()]
"""


def test_get_attributes_of_type():
    class DummyClass:
        def __init__(self, x: int, y: int, z: str):
            self.x = x
            self.y = y
            self.z = z

    target_object = DummyClass(2, 1, "Hello World!")

    result = loader.get_attributes_of_type(target_object, int)

    assert len(result) == 2
    assert target_object.x in result
    assert target_object.y in result


def test_get_workflow_defs_module(tmp_path):
    with open(tmp_path / "workflow_defs.py", "w") as f:
        f.write(WORKFLOW_DEFS)
    module = loader.get_workflow_defs_module(str(tmp_path))
    wfs = loader.get_attributes_of_type(module, dsl.WorkflowTemplate)
    assert len(wfs) == 1
    assert wfs[0]().name == "wf"


@pytest.fixture
def import_modifier():
    """
    This fixture saves the state of the interpreter's session's modules before a test
    and restores the state once the test has completed.
    """
    orig_modules = set(sys.modules.keys())
    orig_path = list(sys.path)
    orig_meta_path = list(sys.meta_path)
    orig_path_hooks = list(sys.path_hooks)
    orig_path_importer_cache = sys.path_importer_cache
    try:
        yield
    finally:
        remove = [key for key in sys.modules if key not in orig_modules]
        for key in remove:
            del sys.modules[key]
        sys.path[:] = orig_path
        sys.meta_path[:] = orig_meta_path
        sys.path_hooks[:] = orig_path_hooks
        sys.path_importer_cache.clear()
        sys.path_importer_cache.update(orig_path_importer_cache)


def test_import_faker(import_modifier):
    """An integration test for the ImportFaker meta path finder"""
    finder = loader.ImportFaker(block_list=["orquestra.fake"])
    sys.meta_path.insert(sys.meta_path.index(PathFinder), finder)
    import orquestra.fake.hellothere  # type: ignore

    assert orquestra.fake.hellothere is not None
    assert isinstance(orquestra.fake.hellothere.not_exist, loader.FakeImportedAttribute)


def test_import_faker_block_list(import_modifier):
    """Unit test for the ImportFaker spec finder"""
    finder = loader.ImportFaker(block_list=["orquestra.fake"])
    # A module that is not in the block list should not be found
    assert finder.find_spec("orquestra.sdk", None) is None
    # A module that is in the block list should be found
    assert finder.find_spec("orquestra.fake", None) is not None


def test_import_faker_unload(import_modifier):
    """Tests that faked modules are unloaded correctly"""
    finder = loader.ImportFaker(block_list=["orquestra.fake"])
    sys.meta_path.insert(sys.meta_path.index(PathFinder), finder)
    original_imports = set(sys.modules.keys())
    assert "orquestra.fake.hellothere" not in original_imports
    import orquestra.fake.hellothere  # type: ignore

    assert "orquestra.fake.hellothere" in sys.modules.keys()
    finder.unload_fakes()
    assert set(sys.modules.keys()) == original_imports


def test_fake_imported_attribute_call():
    attr = loader.FakeImportedAttribute()
    with pytest.raises(RuntimeError):
        attr()


def test_fake_imported_attribute_subscript():
    attr = loader.FakeImportedAttribute()
    with pytest.raises(RuntimeError):
        attr[0]


def test_fake_imported_attribute_attr():
    attr = loader.FakeImportedAttribute()
    with pytest.raises(RuntimeError):
        attr.hello
