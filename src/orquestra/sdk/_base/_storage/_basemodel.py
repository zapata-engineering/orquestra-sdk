################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################

from typing import Any

from pydantic.main import BaseModel


class OrquestraBaseModel(BaseModel):
    """The pydantic BaseModel changed between V1 and V2.

    As a result, workflow outputs generated prior to the V2 upgrade may not be
    depickled correctly. The culpret is a change in behaviour of `__setstate__`.

    This class adds a new `__setstate__` that wraps the V2 BaseModel `__setstate__` and
    adds the missing behaviour back in.


    """

    def __setstate__(self, state: dict[Any, Any]) -> None:
        state.setdefault("__pydantic_extra__", {})
        state.setdefault("__pydantic_private__", {})

        if "__pydantic_fields_set__" not in state:
            state["__pydantic_fields_set__"] = state.get("__fields_set__")

        super().__setstate__(state)
