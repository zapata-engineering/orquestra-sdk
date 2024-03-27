################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################

"""Compatibility layer for pydantic v1 / v2 compatibility."""

from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, Optional

import pydantic
from typing_extensions import Annotated

PYDANTICV1 = pydantic.version.VERSION.startswith("1.")

if PYDANTICV1 and not TYPE_CHECKING:
    from pydantic.generics import GenericModel

    class BaseModel(GenericModel):
        @classmethod
        def model_validate(cls, *args, **kwargs):
            return super(GenericModel, cls).parse_obj(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args, **kwargs):
            return super(GenericModel, cls).parse_raw(*args, **kwargs)

        def model_dump(self, *args, **kwargs):
            return super().dict(*args, **kwargs)

        def model_dump_json(self, *args, **kwargs):
            return super().json(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args, **kwargs):
            return super(GenericModel, cls).schema_json(*args, **kwargs)

        def model_copy(self, *args, **kwargs):
            return super().copy(*args, **kwargs)

else:
    # TODO (ORQSDK-1025): remove the model base class and replace it with an alias to
    # BaseModel
    class BaseModel(pydantic.BaseModel):  # type: ignore[no-redef]
        """The pydantic BaseModel changed between V1 and V2.

        As a result, workflow outputs generated prior to the V2 upgrade may not be
        depickled correctly. The culpret is a change in behaviour of `__setstate__`.

        This class adds a new `__setstate__` that wraps the V2 BaseModel `__setstate__`
        and adds the missing behaviour back in.
        """

        def __setstate__(self, state: Dict[Any, Any]) -> None:
            state.setdefault("__pydantic_extra__", {})
            state.setdefault("__pydantic_private__", {})

            if "__pydantic_fields_set__" not in state:
                state["__pydantic_fields_set__"] = state.get("__fields_set__")

            super().__setstate__(state)


class TypeAdapter:
    """Accessor for Pydantic parsing.

    If Pydantic V2 is installed, this class is a simple wrapper for
    `pydantic.TypeAdapter`.

    If Pydantic V1 is installed, this class acts as a translator between the V1-specific
    `parse_X_as` methods and the V2 TypeAdapter style syntax we use in our code.
    """

    def __init__(self, model, *args, **kwargs):
        if PYDANTICV1:
            self._model = model
        else:
            self._typeadapter = pydantic.TypeAdapter(model, *args, **kwargs)

    def validate_python(self, value, *args, **kwargs):
        if PYDANTICV1:
            return pydantic.parse_obj_as(self._model, value)
        else:
            return self._typeadapter.validate_python(value, *args, **kwargs)

    def validate_json(self, value, *args, **kwargs):
        if PYDANTICV1:
            return pydantic.parse_raw_as(
                self._model, value
            )  # type: ignore[reportCallIssue,operator]
        else:
            return self._typeadapter.validate_json(value, *args, **kwargs)


def field_validator(*fields, **kwargs):
    """Wrapper for pydantic field validators.

    If Pydantic V2 is installed, this operates as a simple wrapper for
    `pydantic.field_validator`.

    If Pydantic V1 is installed, this operates as a wrapper for `pydantic.validator` and
    _tries_ to translate V2-style kwargs. There are not perfect analogues, so this is
    likely to cause problems if we add more validators.
    """
    if PYDANTICV1:

        def translate_kwargs(kwargs: dict) -> dict:
            _kwargs = deepcopy(kwargs)
            if "mode" in _kwargs:
                if _kwargs["mode"] == "before":
                    _kwargs["pre"] = True
                elif _kwargs["mode"] == "after":
                    _kwargs["always"] = True
                _kwargs.pop("mode")
            return _kwargs

        return pydantic.validator(*fields, **translate_kwargs(kwargs))
    else:
        return pydantic.field_validator(*fields, **kwargs)


if TYPE_CHECKING:
    GpuResourceType = Optional[str]
else:
    if PYDANTICV1:
        GpuResourceType = Optional[str]
    else:
        GpuResourceType = Optional[
            Annotated[str, pydantic.BeforeValidator(lambda x: str(x))]
        ]
