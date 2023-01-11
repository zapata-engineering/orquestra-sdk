################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import ast
import inspect
from types import FunctionType

import numpy as np
import pytest

from orquestra.sdk._base import _ast

from .test_artifact_future_methods import (
    ObjWithTask,
    _task_without_resources,
    wf_with_mixed_calls,
)


def _a_function():
    return 2100 + 37


def test_normalize_indents():
    global _a_function
    fn1 = _a_function

    # Note: this shadows the top-level function to keep the sources
    # semantically identical.
    def _a_function():
        return 2100 + 37

    fn2 = _a_function

    assert inspect.getsource(fn1) == _ast.normalize_indents(inspect.getsource(fn2))


def function_with_calls():
    np.arange(2)
    simple_obj = ObjWithTask(0)
    simple_obj.get_id()
    artifact = _task_without_resources()
    print(artifact.custom_name)
    artifact.with_resources(cpu="2000m")
    for ii in range(5):
        artifact.with_resources(cpu=f"{ii*5}000m")


# Calls inside the 'function_with_calls' function
function_calls = [
    _ast._Call(
        callable_name="arange",
        call_statement=[
            _ast.NodeReference(name="np", node_type=_ast.NodeReferenceType.NAME),
            _ast.NodeReference(name="arange", node_type=_ast.NodeReferenceType.CALL),
        ],
        line_no=2,
    ),
    _ast._Call(
        callable_name="ObjWithTask",
        call_statement=[
            _ast.NodeReference(
                name="ObjWithTask", node_type=_ast.NodeReferenceType.CALL
            )
        ],
        line_no=3,
    ),
    _ast._Call(
        callable_name="get_id",
        call_statement=[
            _ast.NodeReference(
                name="simple_obj", node_type=_ast.NodeReferenceType.NAME
            ),
            _ast.NodeReference(name="get_id", node_type=_ast.NodeReferenceType.CALL),
        ],
        line_no=4,
    ),
    _ast._Call(
        callable_name="_task_without_resources",
        call_statement=[
            _ast.NodeReference(
                name="_task_without_resources", node_type=_ast.NodeReferenceType.CALL
            )
        ],
        line_no=5,
    ),
    _ast._Call(
        callable_name="print",
        call_statement=[
            _ast.NodeReference(name="print", node_type=_ast.NodeReferenceType.CALL)
        ],
        line_no=6,
    ),
    _ast._Call(
        callable_name="with_resources",
        call_statement=[
            _ast.NodeReference(name="artifact", node_type=_ast.NodeReferenceType.NAME),
            _ast.NodeReference(
                name="with_resources", node_type=_ast.NodeReferenceType.CALL
            ),
        ],
        line_no=7,
    ),
    _ast._Call(
        callable_name="range",
        call_statement=[
            _ast.NodeReference(name="range", node_type=_ast.NodeReferenceType.CALL)
        ],
        line_no=8,
    ),
    _ast._Call(
        callable_name="with_resources",
        call_statement=[
            _ast.NodeReference(name="artifact", node_type=_ast.NodeReferenceType.NAME),
            _ast.NodeReference(
                name="with_resources", node_type=_ast.NodeReferenceType.CALL
            ),
        ],
        line_no=9,
    ),
]

# Calls of the function of the workflow 'wf_with_mixed_calls'
wf_function_calls = [
    _ast._Call(
        callable_name="ObjWithTask",
        call_statement=[
            _ast.NodeReference(
                name="ObjWithTask", node_type=_ast.NodeReferenceType.CALL
            )
        ],
        line_no=3,
    ),
    _ast._Call(
        callable_name="get_id",
        call_statement=[
            _ast.NodeReference(
                name="simple_obj", node_type=_ast.NodeReferenceType.NAME
            ),
            _ast.NodeReference(name="get_id", node_type=_ast.NodeReferenceType.CALL),
        ],
        line_no=4,
    ),
    _ast._Call(
        callable_name="with_invocation_meta",
        call_statement=[
            _ast.NodeReference(
                name="_task_without_resources", node_type=_ast.NodeReferenceType.CALL
            ),
            _ast.NodeReference(
                name="with_invocation_meta", node_type=_ast.NodeReferenceType.CALL
            ),
        ],
        line_no=5,
    ),
]


class TestCallVisitor:
    @staticmethod
    def test_identify_calls_from_function():
        """Test the identification of calls using the AST of functions"""
        assert isinstance(function_with_calls, FunctionType)
        source = inspect.getsource(function_with_calls)
        fn_body = ast.parse(_ast.normalize_indents(source))
        visitor = _ast.CallVisitor()
        visitor.visit(fn_body)
        # Check that all the calls are identified
        assert visitor.calls == function_calls

    @staticmethod
    def test_identify_calls_from_workflow_function():
        """Test the identification of calls using the AST of the workflow function"""
        assert isinstance(wf_with_mixed_calls()._fn, FunctionType)
        source = inspect.getsource(wf_with_mixed_calls()._fn)
        fn_body = ast.parse(_ast.normalize_indents(source))
        visitor = _ast.CallVisitor()
        visitor.visit(fn_body)
        # Check that all the calls are identified
        assert visitor.calls == wf_function_calls


class TestGetCallStatement:
    @pytest.mark.parametrize(
        "call, expected",
        [
            ("task()", [_ast.NodeReference("task", _ast.NodeReferenceType.CALL)]),
            (
                "sdk.task()",
                [
                    _ast.NodeReference("sdk", _ast.NodeReferenceType.NAME),
                    _ast.NodeReference("task", _ast.NodeReferenceType.CALL),
                ],
            ),
            (
                "task().with_resources()",
                [
                    _ast.NodeReference("task", _ast.NodeReferenceType.CALL),
                    _ast.NodeReference("with_resources", _ast.NodeReferenceType.CALL),
                ],
            ),
            (
                "task()()",
                [
                    _ast.NodeReference("task", _ast.NodeReferenceType.CALL),
                    _ast.NodeReference("", _ast.NodeReferenceType.CALL),
                ],
            ),
            (
                "task()()()",
                [
                    _ast.NodeReference("task", _ast.NodeReferenceType.CALL),
                    _ast.NodeReference("", _ast.NodeReferenceType.CALL),
                    _ast.NodeReference("", _ast.NodeReferenceType.CALL),
                ],
            ),
        ],
    )
    def test_function_call(self, call, expected):
        # Given
        ast_module = ast.parse(call)
        assert isinstance(ast_module.body[0], ast.Expr)
        ast_call = ast_module.body[0].value
        assert isinstance(ast_call, ast.Call)
        # When
        call_statement = _ast.get_call_statement(ast_call)
        # Then
        assert call_statement == expected
