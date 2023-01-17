################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Determines user-friendly captions for showing in interactive prompts.
"""

from functools import singledispatchmethod
from orquestra.sdk.schema import ir


class FNRefFormatter:
    @singledispatchmethod
    def format(self, fn_ref: ir.FunctionRef) -> str:
        raise TypeError(f"Unsupported function ref type: {type(fn_ref)}")

    @format.register
    def _(self, fn_ref: ir.ModuleFunctionRef) -> str:
        return f"{fn_ref.module}:{fn_ref.function_name}"

    @format.register
    def _(self, fn_ref: ir.FileFunctionRef) -> str:
        return f"{fn_ref.file_path}:{fn_ref.function_name}"

    @format.register
    def _(self, fn_ref: ir.InlineFunctionRef) -> str:
        return f"<inline> {fn_ref.function_name}"
