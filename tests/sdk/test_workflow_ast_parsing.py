################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Tests for AST analysis inside @sdk.workflow extracted to a separate test module.
"""

import warnings

import numpy as np
import pytest

import orquestra.sdk as sdk
from orquestra.sdk._base import _workflow


def plain_function():
    pass


@sdk.task
def a_task():
    pass


@sdk.workflow
def with_function_call():
    return [plain_function()]


@sdk.workflow
def with_module_call():
    return [np.ones([1])]


@sdk.workflow
def with_dotted_module_call():
    return [np.random.random()]


@sdk.workflow
def with_list_append():
    results = []
    for _ in range(3):
        results.append(a_task())
    return results


class TestNotATaskWarnings:
    @staticmethod
    def test_does_not_warn_at_definition():
        with warnings.catch_warnings():
            warnings.simplefilter("error")

            @sdk.workflow
            def wf():
                return [plain_function()]

    def test_function_call(self):
        with pytest.warns(_workflow.NotATaskWarning):
            _ = with_function_call()

    def test_module_call(self):
        with pytest.warns(_workflow.NotATaskWarning):
            _ = with_module_call()

    def test_dotted_module_call(self):
        with pytest.warns(_workflow.NotATaskWarning):
            _ = with_dotted_module_call()

    def test_append_list(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _ = with_list_append()
