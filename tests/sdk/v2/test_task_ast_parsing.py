################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Tests for AST analysis inside @sdk.task extracted to a separate test module.
There's so many test cases!
"""
import os

import pytest

import orquestra.sdk as sdk


@sdk.task
def _a_top_level_task():
    return "hello", "there"


class TestNOutputsEdgeCases:
    def test_top_level(self):
        assert _a_top_level_task.output_metadata.n_outputs == 2
        assert _a_top_level_task.output_metadata.is_subscriptable

    def test_nested_function(self):
        # In the future it might be nice to look only at the outer-most return.
        # For now we just have a fall back.

        with pytest.warns(UserWarning):

            @sdk.task
            def _a_task():
                def _inner():
                    # Return with two outputs
                    return "a", "b"

                _inner()

                # A return with three outputs
                return 1, 2, 3

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_union_signature(self):
        with pytest.warns(UserWarning):

            @sdk.task
            def _a_task(a):
                if a < 10:
                    return 4, 2, 0
                else:
                    return 42, 100

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable


class TestNOutputsByNodeType:
    def test_empty_return(self):
        @sdk.task
        def _a_task():
            return

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_pass(self):
        @sdk.task
        def _a_task():
            pass

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_no_return(self):
        @sdk.task
        def _a_task():
            print("hello!")

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_single_literal(self):
        @sdk.task
        def _a_task():
            return "hello"

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_two_literals(self):
        @sdk.task
        def _a_task():
            return "hello", "there"

        assert _a_task.output_metadata.n_outputs == 2
        assert _a_task.output_metadata.is_subscriptable

    def test_single_variable(self):
        @sdk.task
        def _a_task():
            results = "hello", "there"
            return results

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_overriding(self):
        @sdk.task(n_outputs=2)
        def _a_task():
            results = "hello", "there"
            return results

        assert _a_task.output_metadata.n_outputs == 2
        assert _a_task.output_metadata.is_subscriptable

    def test_module_function_call(self):
        @sdk.task
        def _a_task():
            # choice of the called function here doesn't make a difference
            hello = "hello"
            return os.path.join(hello, "world")

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_module_attr(self):
        @sdk.task
        def _a_task():
            return os.path.sep

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_inner_function_call(self):
        @sdk.task
        def _a_task():
            def _inner():
                return "hello"

            return _inner()

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_method_call(self, caplog):
        @sdk.task
        def _a_task():
            text = "hello"
            return text.capitalize()

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable
        assert "Assuming a single output for node" in caplog.text

    def test_fstring(self):
        @sdk.task
        def _a_task(name: str):
            return f"hello, {name}"

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_starred_tuple(self):
        @sdk.task
        def _a_task():
            names = ["emiliano", "zapata"]
            return ("hello", *names, "world")

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_set(self):
        @sdk.task
        def _a_task(name: str):
            return {"hello", name}

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_starred_set(self):
        @sdk.task
        def _a_task(name: str):
            names = ["emiliano", "zapata"]
            return {"hello", *names, name}

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_set_comp(self):
        @sdk.task
        def _a_task(name: str):
            return {word for word in "hello world".split()}

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_empty_sequences(self):
        @sdk.task
        def _empty_list():
            return []

        @sdk.task
        def _empty_dict():
            return {}

        @sdk.task
        def _empty_return():
            return

        # Let's treat an empty sequences similarly to how we treat empty returns
        assert (
            _empty_list.output_metadata.n_outputs
            == _empty_dict.output_metadata.n_outputs
            == _empty_return.output_metadata.n_outputs
            == 1
        )
        assert (
            _empty_list.output_metadata.is_subscriptable
            == _empty_dict.output_metadata.is_subscriptable
            == _empty_return.output_metadata.is_subscriptable
            is False
        )

    def test_list_two_elements(self):
        @sdk.task
        def _a_task():
            return ["hello", "world"]

        assert _a_task.output_metadata.n_outputs == 2
        assert _a_task.output_metadata.is_subscriptable

    def test_list_a_lot_of_elements(self):
        @sdk.task
        def _a_task():
            return ["hello", "world", "hello", "world", "hello"]

        assert _a_task.output_metadata.n_outputs == 5
        assert _a_task.output_metadata.is_subscriptable

    def test_starred_list(self):
        @sdk.task
        def _a_task():
            names = ["emiliano", "zapata"]
            return ["hello", *names, "world"]

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_list_comp(self):
        @sdk.task
        def _a_task():
            return [word.upper() for word in "hello world".split()]

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_dict(self):
        @sdk.task
        def _a_task():
            val2 = "val2"
            return {"key1": "val1", "key2": val2}

        assert _a_task.output_metadata.n_outputs == 2
        assert _a_task.output_metadata.is_subscriptable

    def test_starred_dict(self):
        @sdk.task
        def _a_task():
            return {**{"hello": "world"}, "hey": "there"}

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_dict_comp(self):
        @sdk.task
        def _a_task():
            return {f"key{i}": f"val{i}" for i in range(3)}

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_generator(self):
        @sdk.task
        def _a_task():
            return (word for word in "hello world".split())

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_subscript(self):
        @sdk.task
        def _a_task():
            words = {"hello": "world"}
            return words["hello"]

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_bin_op(self):
        @sdk.task
        def _a_task():
            a = ["hello"]
            b = ["emiliano", "zapata"]
            return a + b

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_unary_op(self):
        @sdk.task
        def _a_task():
            num1 = 42
            return -num1

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_bool_op(self):
        @sdk.task
        def _a_task():
            num1 = 42
            return num1 < 0 and num1 > -100

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable

    def test_comparison(self):
        @sdk.task
        def _a_task():
            return 42 > 12

        assert _a_task.output_metadata.n_outputs == 1
        assert not _a_task.output_metadata.is_subscriptable
