################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################

from typing import Any

import pydantic

PYDANTICV1 = pydantic.__version__.startswith("1.")


# TODO (ORQSDK-1025): remove the model base class
class OrquestraBaseModel(pydantic.main.BaseModel):
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

    @classmethod
    def model_validate(cls, *args, **kwargs):
        if PYDANTICV1:
            return super(OrquestraBaseModel, cls).parse_obj(*args, **kwargs)
        else:
            return super(OrquestraBaseModel, cls).model_validate(*args, **kwargs)

    @classmethod
    def model_validate_json(cls, *args, **kwargs):
        if PYDANTICV1:
            return super(OrquestraBaseModel, cls).parse_raw(*args, **kwargs)
        else:
            return super(OrquestraBaseModel, cls).model_validate_json(*args, **kwargs)

    def model_dump(self, *args, **kwargs):
        if PYDANTICV1:
            return super().dict(*args, **kwargs)
        else:
            return super().model_dump(*args, **kwargs)

    def model_dump_json(self, *args, **kwargs):
        if PYDANTICV1:
            return super().json(*args, **kwargs)
        else:
            return super().model_dump_json(*args, **kwargs)

    def model_json_schema(self, *args, **kwargs):
        if PYDANTICV1:
            return super().schema_json(*args, **kwargs)
        else:
            return super().model_json_schema(*args, **kwargs)

    def model_copy(self, *args, **kwargs):
        if PYDANTICV1:
            return super().copy(*args, **kwargs)
        else:
            return super().model_copy(*args, **kwargs)
