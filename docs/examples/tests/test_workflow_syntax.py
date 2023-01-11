################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Snippets and tests used in the "Workflow Syntax" guide.
"""

import subprocess
import sys
from pathlib import Path

import pytest

import orquestra.sdk as sdk

from .parsers import get_snippet_as_str


# Used inside an example snippet, defined to let tests work.
@sdk.task
def task_foo():
    ...


# Used inside an example snippet, defined to let tests work.
@sdk.task
def task_bar(*_):
    ...


class Snippets:
    @staticmethod
    def same_module():
        # file: workflow_defs.py
        import orquestra.sdk as sdk

        @sdk.task
        def make_greeting() -> str:
            return "hello!"

        @sdk.workflow
        def wf():
            text = make_greeting()
            ...
            # </snippet>
            return text

        return wf

    @staticmethod
    def define_task_in_other_module():
        # file: ml/generators.py
        import numpy as np

        import orquestra.sdk as sdk

        @sdk.task
        def random_dataset() -> np.ndarray:
            ...

        # </snippet>

    @staticmethod
    def import_task_from_other_module():
        # file: workflow_defs.py
        import ml.generators

        import orquestra.sdk as sdk

        @sdk.workflow
        def wf():
            X = ml.generators.random_dataset()
            return X

        # </snippet>

    @staticmethod
    def passing_outputs():
        import orquestra.sdk as sdk

        @sdk.task
        def make_greeting() -> str:
            return "hello!"

        @sdk.task
        def reverse_text(text: str) -> str:
            return text[::-1]

        @sdk.workflow
        def wf():
            greeting = make_greeting()
            reversed_greeting = reverse_text(greeting)
            ...
            # </snippet>
            return reversed_greeting

        return wf

    @staticmethod
    def returning_values():
        import orquestra.sdk as sdk

        @sdk.workflow
        def wf():
            constant = 5
            artifact1 = task_foo()
            artifact2 = task_bar(artifact1)
            return artifact2, constant

        # </snippet>
        return wf

    @staticmethod
    def fixed_literals():
        import numpy as np

        import orquestra.sdk as sdk

        @sdk.task
        def normalize(X: np.ndarray) -> np.ndarray:
            ...

        @sdk.task
        def reverse_text(text: str) -> str:
            ...

        @sdk.workflow
        def wf():
            # this will be embedded inside the workflow as a JSON
            text = "hello"
            reversed_greeting = reverse_text(text)

            # this will be serialized using pickle
            X = np.array([[1, 2, 3, 4], [2, -2, -5, 1]])
            normalized_X = normalize(X)
            ...
            # </snippet>
            return normalized_X, reversed_greeting

        return wf

    @staticmethod
    def calling_regular_function():
        import orquestra.sdk as sdk

        def plain_reverse(text: str) -> str:
            """
            Notice this is just a regular Python function, not decorated with `@task`.
            """
            return text[::-1]

        @sdk.workflow
        def wf():
            greeting = "hello"
            reversed_greeting = plain_reverse(greeting)
            # The result workflow will contain an embedded `"olleh"` serialized value.
            ...
            # </snippet>
            return reversed_greeting

        return wf

    @staticmethod
    def print_wf_results():
        from workflow_defs import wf

        run = wf().run("in_process")
        print(run.get_results(wait=True))

    @staticmethod
    def task_in_task_fails():
        import orquestra.sdk as sdk

        @sdk.task
        def capitalize(text: str) -> str:
            return text.capitalize()

        @sdk.task
        def join_texts(text1: str, text2: str) -> str:
            #                  Oops! This is another task!
            text1_capitalized = capitalize(text1)
            return f"{text1_capitalized}{text2}"

        @sdk.workflow
        def wf():
            return join_texts("hello", "there")

        # </snippet>

    @staticmethod
    def task_in_task_workaround():
        import orquestra.sdk as sdk

        def capitalize_fn(text: str) -> str:
            return text.capitalize()

        @sdk.task
        def capitalize_task(text: str) -> str:
            return capitalize_fn(text)

        @sdk.task
        def join_texts(text1: str, text2: str) -> str:
            text1_capitalized = capitalize_fn(text1)
            return f"{text1_capitalized}{text2}"

        @sdk.workflow
        def wf():
            return join_texts("hello", capitalize_task("there"))

        # </snippet>

    @staticmethod
    def mixing_constants_with_outputs():
        import typing as t

        import numpy as np

        import orquestra.sdk as sdk

        @sdk.task
        def fetch_dataset() -> t.Tuple[np.ndarray, np.ndarray]:
            return np.random.random(10), np.random.random((10, 10))

        @sdk.task
        def normalize_X(X: np.ndarray) -> np.ndarray:
            ...

        @sdk.task
        def encode_labels(y: np.ndarray) -> np.ndarray:
            ...

        @sdk.task
        def train(dataset_dict):
            ...

        @sdk.workflow
        def wf():
            raw_X, raw_y = fetch_dataset()

            dataset_dict = {
                "X": normalize_X(raw_X),
                "y": encode_labels(raw_y),
                "name": "A toy dataset",
            }
            model = train(dataset_dict)
            return model

        # </snippet>
        return wf

    @staticmethod
    def mixing_constants_with_outputs_workaround_1():
        import typing as t

        import numpy as np

        import orquestra.sdk as sdk

        @sdk.task
        def fetch_dataset() -> t.Tuple[np.ndarray, np.ndarray]:
            return np.random.random(10), np.random.random((10, 10))

        @sdk.task
        def normalize_X(X: np.ndarray) -> np.ndarray:
            ...

        @sdk.task
        def encode_labels(y: np.ndarray) -> np.ndarray:
            ...

        # notice the different function signature
        @sdk.task
        def train(X: np.ndarray, y: np.ndarray, name: str):
            ...

        @sdk.workflow
        def wf():
            raw_X, raw_y = fetch_dataset()

            # notice the different function signature
            model = train(normalize_X(raw_X), encode_labels(raw_y), "A toy dataset")
            return model

        # </snippet>
        return wf

    @staticmethod
    def mixing_constants_with_outputs_workaround_2():
        import typing as t

        import numpy as np

        import orquestra.sdk as sdk

        @sdk.task
        def fetch_dataset() -> t.Tuple[np.ndarray, np.ndarray]:
            return np.random.random(10), np.random.random((10, 10))

        @sdk.task
        def normalize_X(X: np.ndarray) -> np.ndarray:
            ...

        @sdk.task
        def encode_labels(y: np.ndarray) -> np.ndarray:
            ...

        @sdk.task
        def train(dataset_dict):
            ...

        # notice the problematic code extracted to a task
        @sdk.task
        def make_dataset_dict(X: np.ndarray, y: np.ndarray, name: str) -> t.Dict:
            return {
                "X": X,
                "y": y,
                "name": name,
            }

        @sdk.workflow
        def wf():
            raw_X, raw_y = fetch_dataset()

            # notice the usage of the extracted task
            dataset_dict = make_dataset_dict(
                normalize_X(raw_X), encode_labels(raw_y), "A toy dataset"
            )
            model = train(dataset_dict)
            return model

        # </snippet>
        return wf


class TestSnippets:
    @staticmethod
    def test_same_module():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.same_module()

        # When
        # Create the model from the workflow
        model = wf.model

        # Then
        assert "invocation-0-task-make-greeting" in model.task_invocations.keys()

    @staticmethod
    def test_task_in_other_module(change_test_dir):
        # Given
        # Prepare the project dir with the other module
        src_file = Path(".") / "ml" / "generators.py"
        src_file.parent.mkdir()
        src_file.write_text(get_snippet_as_str(fn=Snippets.define_task_in_other_module))

        src_file = Path(".") / "workflow_defs.py"
        src_file.write_text(
            get_snippet_as_str(fn=Snippets.import_task_from_other_module)
            + "\nprint(wf.model)"
        )

        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(src_file)], capture_output=True)

        # Then
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        # Ensure this task call is in the workflow
        assert "invocation-0-task-random-dataset" in std_out

    @staticmethod
    def test_passing_outputs():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.passing_outputs()

        # When
        model = wf.model

        # Then
        greeting_artifact = "artifact-1-make-greeting"
        reverse_artifact = "artifact-0-reverse-text"
        reverse_invocation = "invocation-0-task-reverse-text"

        # Ensure only two artifacts from the tasks
        assert len(model.artifact_nodes) == 2
        assert greeting_artifact in model.artifact_nodes
        assert reverse_artifact in model.artifact_nodes

        # Ensure 2 task invocations with the greeting artifact as input to the reverse
        # text task.
        assert len(model.task_invocations) == 2
        assert greeting_artifact in model.task_invocations[reverse_invocation].args_ids

        # Ensure only one output from the reverse text task
        assert len(model.output_ids) == 1
        assert reverse_artifact in model.output_ids

    @staticmethod
    def test_returning_values():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.returning_values()

        # When
        model = wf.model

        # Then
        assert len(model.output_ids) == 2

    @staticmethod
    def test_fixed_literals():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.fixed_literals()

        # When
        model = wf.model

        # Then
        # Exactly 2 constants
        assert len(model.constant_nodes) == 2
        assert (
            model.constant_nodes["constant-0"].serialization_format == "ENCODED_PICKLE"
        )
        assert model.constant_nodes["constant-1"].serialization_format == "JSON"

    @staticmethod
    def test_calling_regular_function():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.calling_regular_function()

        # When
        # Create the model from the workflow
        model = wf.model

        # Then
        assert len(model.task_invocations) == 0

    @staticmethod
    def test_task_in_task_fails(change_test_dir):
        # Given
        # Prepare the project dir: 'workflow_defs.py' and 'script.py' files
        src_file = Path("./workflow_defs.py")
        src_file.write_text(get_snippet_as_str(fn=Snippets.task_in_task_fails))
        run_wf = Path("./script.py")
        run_wf.write_text(get_snippet_as_str(fn=Snippets.print_wf_results))
        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(run_wf)], capture_output=True)

        # Then
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        assert "hellothere" not in std_out
        assert "ArtifactFuture" in std_out

    @staticmethod
    def test_task_in_task_workaround(change_test_dir):
        # Given
        # Prepare the project dir: 'workflow_defs.py' and 'script.py' files
        src_file = Path("./workflow_defs.py")
        src_file.write_text(get_snippet_as_str(fn=Snippets.task_in_task_workaround))
        run_wf = Path("./script.py")
        run_wf.write_text(get_snippet_as_str(fn=Snippets.print_wf_results))
        # When
        # Call the snippet
        proc = subprocess.run([sys.executable, str(run_wf)], capture_output=True)

        # Then
        proc.check_returncode()
        std_out = str(proc.stdout, "utf-8")
        assert "HelloThere" in std_out
        assert "ArtifactFuture" not in std_out

    @staticmethod
    def test_mixing_constants_with_outputs():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.mixing_constants_with_outputs()

        # When
        with pytest.raises(sdk.exceptions.WorkflowSyntaxError):
            _ = wf.model

    @staticmethod
    def test_mixing_constants_with_outputs_workaround_1():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.mixing_constants_with_outputs_workaround_1()

        # When
        model = wf.model

        # Then
        assert len(model.task_invocations) == 4
        assert len(model.task_invocations["invocation-0-task-train"].args_ids) == 3

    @staticmethod
    def test_mixing_constants_with_outputs_workaround_2():
        # Given
        # Get the workflow from the test snippets
        wf = Snippets.mixing_constants_with_outputs_workaround_2()

        # When
        model = wf.model

        # Then
        assert len(model.task_invocations) == 5
        assert len(model.task_invocations["invocation-0-task-train"].args_ids) == 1
