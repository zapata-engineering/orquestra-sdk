################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################

from typing import Any

import pydantic

PYDANTICV1 = pydantic.__version__.startswith("1.")

if PYDANTICV1:
    from pydantic.generics import GenericModel

    class OrquestraBaseModel(GenericModel):
        @classmethod
        def model_validate(cls, *args, **kwargs):
            return super(OrquestraBaseModel, cls).parse_obj(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args, **kwargs):
            return super(OrquestraBaseModel, cls).parse_raw(*args, **kwargs)

        def model_dump(self, *args, **kwargs):
            return super().dict(*args, **kwargs)

        def model_dump_json(self, *args, **kwargs):
            return super().json(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args, **kwargs):
            return super(OrquestraBaseModel, cls).schema_json(*args, **kwargs)

        def model_copy(self, *args, **kwargs):
            return super().copy(*args, **kwargs)

else:
    # TODO (ORQSDK-1025): remove the model base class and replace it with an alies to
    # BaseModel
    class OrquestraBaseModel(pydantic.BaseModel):
        """The pydantic BaseModel changed between V1 and V2.

        As a result, workflow outputs generated prior to the V2 upgrade may not be
        depickled correctly. The culpret is a change in behaviour of `__setstate__`.

        This class adds a new `__setstate__` that wraps the V2 BaseModel `__setstate__`
        and adds the missing behaviour back in.
        """

        def __setstate__(self, state: dict[Any, Any]) -> None:
            state.setdefault("__pydantic_extra__", {})
            state.setdefault("__pydantic_private__", {})

            if "__pydantic_fields_set__" not in state:
                state["__pydantic_fields_set__"] = state.get("__fields_set__")

            super().__setstate__(state)
