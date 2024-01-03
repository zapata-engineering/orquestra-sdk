################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
################################################################################
import importlib
import sys
import traceback
import typing as t

import pytest

from orquestra.sdk._client import dispatch
from orquestra.sdk.schema import ir


# Those tests are loading the example workflows from weird directories.
# Cleanup so the rest of the tests are not affected by it
@pytest.fixture(autouse=True, scope="module")
def cleanup():
    yield
    importlib.invalidate_caches()
    importlib.reload(sys.modules["orquestra.sdk.examples.exportable_wf"])


class TestLocateFnRef:
    def test_default_search_path(self):
        fn = dispatch.locate_fn_ref(
            ir.FileFunctionRef(
                file_path="src/orquestra/sdk/examples/workflow_defs.py",
                function_name="hello",
            )
        )
        assert callable(fn)

    def test_invalid_object(self):
        fn_ref: t.Any = object()

        with pytest.raises(ValueError):
            dispatch.locate_fn_ref(fn_ref)


@pytest.mark.parametrize(
    "example_sys_paths",
    [
        ["/app/abc", "/app/def"],
        ["/app/abc", "/app/abc"],
        [],
    ],
)
class TestSysPaths:
    def test_empty_list_does_not_change_sys(self, monkeypatch, example_sys_paths):
        monkeypatch.setattr(sys, "path", list(example_sys_paths))

        dispatch.ensure_sys_paths([])

        assert sys.path == example_sys_paths

    def test_new_paths_are_prepended(self, monkeypatch, example_sys_paths):
        monkeypatch.setattr(sys, "path", list(example_sys_paths))
        new_paths = ["/added/1", "/added/2"]

        dispatch.ensure_sys_paths(new_paths)

        assert sys.path == [*new_paths, *example_sys_paths]

    def test_duplicated_additions_are_deduplicated(
        self, monkeypatch, example_sys_paths
    ):
        monkeypatch.setattr(sys, "path", list(example_sys_paths))
        new_paths = ["/added/1", "/added/2"]

        dispatch.ensure_sys_paths([*new_paths, *new_paths])

        assert sys.path == [*new_paths, *example_sys_paths]

    def test_new_path_already_in_sys_path(self, monkeypatch, example_sys_paths):
        new_path = "/added/1"
        starting_sys_paths = [*example_sys_paths, new_path]
        monkeypatch.setattr(sys, "path", starting_sys_paths)

        dispatch.ensure_sys_paths([new_path])

        assert sys.path == starting_sys_paths


def test_load_module_exceptions(monkeypatch):
    module_name = "i_hope_this_does_not_exist"

    with pytest.raises(ModuleNotFoundError) as e_info:
        dispatch.load_module_or_help_debugging(module_name)

    msg = "\n".join(traceback.format_exception_only(e_info.type, e_info.value))

    assert "ModuleNotFoundError: " in msg
    assert '("No module named i_hope_this_does_not_exist. Pwd: ' in msg
    assert "Full sys.path: [" in msg
    assert "Non-directories on sys.path: [" in msg
