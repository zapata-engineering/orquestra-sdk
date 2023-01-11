################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
################################################################################
import codecs
import importlib
import json
import os
import sys
import traceback

import dill  # type: ignore
import pytest

from orquestra.sdk._base import dispatch
from orquestra.sdk.examples import exportable_wf
from orquestra.sdk.schema import ir

from .dirs import ch_temp_dir


# Those tests are loading the example workflows from weird directories.
# Cleanup so the rest of the tests are not affected by it
@pytest.fixture(autouse=True, scope="module")
def cleanup():
    yield
    importlib.invalidate_caches()
    importlib.reload(sys.modules["orquestra.sdk.examples.exportable_wf"])


def test_executing_task_by_module_ref():
    with ch_temp_dir() as dir_path:
        dispatch.exec_task_fn(
            "hello",
            __sdk_fn_ref_dict={
                "module": "orquestra.sdk.examples.exportable_wf",
                "function_name": "capitalize",
                "line_number": 15,
                "type": "MODULE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
        )

        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")

        with open(expected_file_path) as f:
            assert json.load(f) == {"value": '"Hello"', "serialization_format": "JSON"}


ENCODED_FN = codecs.encode(
    dill.dumps(exportable_wf.capitalize_no_decorator, protocol=3, recurse=True),
    "base64",
).decode()


def test_executing_task_by_inline_ref():
    with ch_temp_dir() as dir_path:
        dispatch.exec_task_fn(
            "hello",
            __sdk_fn_ref_dict={
                "function_name": "capitalize_inline",
                "encoded_function": [ENCODED_FN],
                "type": "INLINE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
        )

        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")

        with open(expected_file_path) as f:
            assert json.load(f) == {"value": '"Hello"', "serialization_format": "JSON"}


def test_executing_task_by_file_function_ref():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task()
def capitalize(text: str):
    return text.capitalize()
"""
            )
        dispatch.exec_task_fn(
            "hello",
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "capitalize",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
        )
        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"Hello"',
                "serialization_format": "JSON",
            }


def test_executing_task_by_module_ref_on_qe():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        repo_path = os.path.join(dir_path, "step", "my_repo")
        os.makedirs(repo_path, exist_ok=True)

        # Save a test script
        file_path = "my_tasks.py"
        with open(os.path.join(repo_path, file_path), "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task()
def capitalize(text: str):
    return text.capitalize()
"""
            )

        dispatch.exec_task_fn(
            "hello",
            __sdk_fn_ref_dict={
                "module": "my_tasks",
                "function_name": "capitalize",
                "file_path": file_path,
                "line_number": 3,
                "type": "MODULE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
            __sdk_additional_sys_paths=["step/my_repo"],
        )
        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"Hello"',
                "serialization_format": "JSON",
            }


def test_executing_function_instead_of_task():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
def capitalize(text: str):
    return text.capitalize()
"""
            )
        dispatch.exec_task_fn(
            "hello",
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "capitalize",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
        )
        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"Hello"',
                "serialization_format": "JSON",
            }


def test_executing_task_without_artifacts_dir():
    with ch_temp_dir():
        dispatch.exec_task_fn(
            "hello",
            __sdk_fn_ref_dict={
                "module": "orquestra.sdk.examples.exportable_wf",
                "function_name": "capitalize",
                "line_number": 15,
                "type": "MODULE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
        )
        expected_file_path = "artifact-0_hello.json"
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"Hello"',
                "serialization_format": "JSON",
            }


def test_executing_task_with_positional_args():
    with ch_temp_dir() as dir_path:
        kwargs = {
            "constant-0": "hello",
        }
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "module": "orquestra.sdk.examples.exportable_wf",
                "function_name": "capitalize",
                "line_number": 15,
                "type": "MODULE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=["constant-0"],
            __sdk_artifacts_dir=dir_path,
            **kwargs,
        )

        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")

        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"Hello"',
                "serialization_format": "JSON",
            }


def test_executing_task_with_kwargs():
    with ch_temp_dir() as dir_path:
        kwargs = {
            "text": "hello",
        }
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "module": "orquestra.sdk.examples.exportable_wf",
                "function_name": "capitalize",
                "line_number": 15,
                "type": "MODULE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
            **kwargs,
        )

        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")

        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"Hello"',
                "serialization_format": "JSON",
            }


def test_file_ref_with_invalid_file_type():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.c")
        with open(file_path, "w") as f:
            f.write("")
        with pytest.raises(ModuleNotFoundError):
            dispatch.exec_task_fn(
                __sdk_fn_ref_dict={
                    "file_path": file_path,
                    "function_name": "lowercase",
                    "line_number": 3,
                    "type": "FILE_FUNCTION_REF",
                },
                __sdk_output_node_dicts=[],
                __sdk_positional_args_ids=[],
                __sdk_artifacts_dir=dir_path,
            )


def test_unknown_fn_ref_type():
    with ch_temp_dir() as dir_path:
        with pytest.raises(ValueError):
            dispatch.exec_task_fn(
                __sdk_fn_ref_dict={
                    "type": "MAGIC_REF",
                },
                __sdk_output_node_dicts=[],
                __sdk_positional_args_ids=[],
                __sdk_artifacts_dir=dir_path,
            )


def test_task_with_encoded_pickle_constant():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task()
def identity(value):
    return value
"""
            )
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "identity",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
            value={
                "chunks": ["gASVEQAAAAAAAACMDUknbSBhIHBpY2tsZSGULg==\n"],
                "serialization_format": "ENCODED_PICKLE",
            },
        )
        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"I\'m a pickle!"',
                "serialization_format": "JSON",
            }


def test_task_with_artifact_pickle():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task()
def pickle(value):
    return value
"""
            )
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "pickle",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                    "serialization_format": "ENCODED_PICKLE",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
            value="hello",
        )
        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "chunks": ["gANYBQAAAGhlbGxvcQAu\n"],
                "serialization_format": "ENCODED_PICKLE",
            }


def test_executing_task_multiple_outputs():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task(n_outputs=2)
def multi_output():
    return "hello", "there"
"""
            )
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "multi_output",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                    "artifact_index": 0,
                },
                {
                    "id": "artifact-1_hello",
                    "artifact_index": 1,
                },
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
        )
        expected_file_paths = [
            os.path.join(dir_path, "artifact-0_hello.json"),
            os.path.join(dir_path, "artifact-1_hello.json"),
        ]
        expected_values = ['"hello"', '"there"']
        for value, path in zip(expected_values, expected_file_paths):
            with open(path) as f:
                assert json.load(f) == {
                    "value": value,
                    "serialization_format": "JSON",
                }


def test_executing_task_single_indexed_output():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)

        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task(n_outputs=2)
def multi_output():
    return "hello", "there"
"""
            )
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "multi_output",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-1_hello",
                    "artifact_index": 1,
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
        )
        expected_file_path = os.path.join(dir_path, "artifact-1_hello.json")
        expected_value = '"there"'
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": expected_value,
                "serialization_format": "JSON",
            }


def test_executing_task_constant_used_twice():
    with ch_temp_dir() as dir_path:
        # Mimic the v1 platform environment
        step_path = os.path.join(dir_path, "step")
        os.mkdir(step_path)
        kwargs = {
            "constant-1": "15",
        }
        # Save a test script
        file_path = os.path.join(step_path, "test_task.py")
        with open(file_path, "w") as f:
            f.write(
                """
import orquestra.sdk as sdk
@sdk.task(n_outputs=2)
def multi_output(a ,b):
    return a, b
"""
            )
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "file_path": file_path,
                "function_name": "multi_output",
                "line_number": 3,
                "type": "FILE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-1_hello",
                    "artifact_index": 1,
                }
            ],
            __sdk_positional_args_ids=["constant-1", "constant-1"],
            __sdk_artifacts_dir=dir_path,
            **kwargs,
        )
        expected_file_path = os.path.join(dir_path, "artifact-1_hello.json")
        expected_value = '"15"'
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": expected_value,
                "serialization_format": "JSON",
            }


def test_executing_task_with_json_input():
    with ch_temp_dir() as dir_path:
        # Save the JSON input
        input_file = "text.json"
        with open(input_file, "w") as f:
            f.write('{"serialization_format": "JSON","value": "\\"there\\""}')
        dispatch.exec_task_fn(
            __sdk_fn_ref_dict={
                "module": "orquestra.sdk.examples.exportable_wf",
                "function_name": "capitalize",
                "line_number": 15,
                "type": "MODULE_FUNCTION_REF",
            },
            __sdk_output_node_dicts=[
                {
                    "id": "artifact-0_hello",
                }
            ],
            __sdk_positional_args_ids=[],
            __sdk_artifacts_dir=dir_path,
            text=f"{os.path.join(dir_path, input_file)}",
        )
        expected_file_path = os.path.join(dir_path, "artifact-0_hello.json")
        with open(expected_file_path) as f:
            assert json.load(f) == {
                "value": '"There"',
                "serialization_format": "JSON",
            }


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
        with pytest.raises(ValueError):
            dispatch.locate_fn_ref(object())


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
