################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import codecs
import json
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from functools import singledispatch
from pathlib import Path

import cloudpickle  # type: ignore
import pydantic

from orquestra.sdk.schema import ir, responses

CHUNK_SIZE = 40_000
ENCODING = "base64"
PICKLE_PROTOCOL = 4


class _JSONTupleEncoder(json.JSONEncoder):
    @staticmethod
    def decode_tuple(obj):
        if "__tuple__" in obj:
            return tuple(obj["__values__"])
        else:
            return obj

    @classmethod
    def encode_tuple(cls, obj):
        if isinstance(obj, tuple):
            return {
                "__tuple__": True,
                "__values__": tuple(cls.encode_tuple(o) for o in obj),
            }
        elif isinstance(obj, list):
            return [cls.encode_tuple(v) for v in obj]
        elif isinstance(obj, dict):
            return {k: cls.encode_tuple(v) for k, v in obj.items()}
        else:
            return obj

    def encode(self, obj):
        return super(_JSONTupleEncoder, self).encode(self.encode_tuple(obj))


def _serialize_json(value: t.Any):
    return json.dumps(value, cls=_JSONTupleEncoder)


def _chunkify(s: str) -> t.List[str]:
    """
    Yaml/JSON parsers will fail if string is too large,
    so break it down into a list of strings where each chunk
    is no larger than CHUNK_SIZE
    """
    return [s[i : i + CHUNK_SIZE] for i in range(0, len(s), CHUNK_SIZE)]


def _encoded_pickle_chunks(object: t.Any) -> t.List[str]:
    return _chunkify(
        codecs.encode(
            cloudpickle.dumps(object, protocol=PICKLE_PROTOCOL), ENCODING
        ).decode()
    )


@contextmanager
def registered_module(module):
    """
    Used to temporarily register a module to be pickled by value

    If a module (or its members) are not registered, they will be pickled by reference
    which will require the modules to be installed when unpickling.
    """
    if module is not None:
        register_pickle_by_value(module)
    try:
        yield
    finally:
        if module is not None:
            unregister_pickle_by_value(module)


def register_pickle_by_value(module):
    cloudpickle.register_pickle_by_value(module)


def unregister_pickle_by_value(module):
    cloudpickle.unregister_pickle_by_value(module)


def serialize_pickle(object: t.Any) -> t.List[str]:
    return _encoded_pickle_chunks(object)


@singledispatch
def deserialize(result) -> t.Any:
    raise NotImplementedError(
        f"Deserialization not implemented for {type(result).__name__}"
    )


@deserialize.register
def _(result: responses.JSONResult) -> t.Any:
    return deserialize_json(result.value)


@deserialize.register
def _(result: responses.PickleResult) -> t.Any:
    return deserialize_pickle(result.chunks)


@deserialize.register
def _(result: ir.ConstantNodeJSON) -> t.Any:
    return deserialize_constant(result)


@deserialize.register
def _(result: ir.ConstantNodePickle) -> t.Any:
    return deserialize_constant(result)


def deserialize_json(serialized_value: str) -> t.Any:
    return json.loads(serialized_value, object_hook=_JSONTupleEncoder.decode_tuple)


def deserialize_pickle(chunks: t.List[str]) -> t.Any:
    chunks_str: str = "".join(chunks)
    return cloudpickle.loads(codecs.decode(chunks_str.encode(), ENCODING))


def result_from_artifact(
    artifact_value: t.Any, artifact_format: ir.ArtifactFormat
) -> responses.WorkflowResult:
    if artifact_format == ir.ArtifactFormat.JSON:
        return responses.JSONResult(value=_serialize_json(artifact_value))
    elif artifact_format == ir.ArtifactFormat.ENCODED_PICKLE:
        return responses.PickleResult(chunks=_encoded_pickle_chunks(artifact_value))
    elif artifact_format == ir.ArtifactFormat.AUTO:
        try:
            return result_from_artifact(artifact_value, ir.ArtifactFormat.JSON)
        except (TypeError, ValueError):
            # TypeError - if the value isn't JSON-serializable
            # ValueError - if the value contains nan, inf, or -inf and the encoder is
            # set not to support it. See:
            # https://github.com/python/cpython/blob/d5650a1738fe34f6e1db4af5f4c4edb7cae90a36/Lib/json/encoder.py#L122-L125
            return result_from_artifact(
                artifact_value, ir.ArtifactFormat.ENCODED_PICKLE
            )
    else:
        raise NotImplementedError(
            "We only support AUTO, JSON, and ENCODED_PICKLE artifact serialization at"
            f" the moment, not {artifact_format}"
        )


def value_from_result_dict(result_dict: t.Mapping) -> t.Any:
    # Bug with mypy and Pydantic:
    #   Unions cannot be passed to parse_obj_as: pydantic/pydantic#1847
    result: responses.WorkflowResult = pydantic.parse_obj_as(
        responses.WorkflowResult, result_dict  # type: ignore[arg-type]
    )
    return deserialize(result)


def deserialize_constant(node: ir.ConstantNode):
    # Bug with mypy and Pydantic:
    #   Unions cannot be passed to parse_obj_as: pydantic/pydantic#1847
    return deserialize(
        pydantic.parse_obj_as(
            responses.WorkflowResult, node.dict()  # type: ignore[arg-type]
        )
    )


def stringify_package_spec(package: ir.PackageSpec) -> str:
    parts: t.List[str] = [package.name]

    if package.extras:
        formatted_extras = ",".join(sorted(package.extras))
        parts.append(f"[{formatted_extras}]")

    if package.version_constraints:
        parts.append(f'{",".join(package.version_constraints)}')

    if package.environment_markers:
        parts.append(f"; {package.environment_markers}")

    return "".join(parts)


@dataclass(frozen=True)
class DumpDetails:
    file_path: Path
    format: ir.ArtifactFormat


def dump_to_file(value: t.Any, dir_path: Path, file_name_prefix: str) -> DumpDetails:
    """
    Writes ``value`` to a file. Serialization format is picked depending on the value.
    The result file is created under ``<dir_path>/<file_name_prefix><extension>``,
    where ``extension`` matches the inferred format.

    Returns:
        Metadata about the created file.
    """
    dir_path.mkdir(parents=True, exist_ok=True)

    json_file_path = dir_path / f"{file_name_prefix}.json"

    try:
        with json_file_path.open("w") as f:
            json.dump(value, f)

        return DumpDetails(json_file_path, ir.ArtifactFormat.JSON)
    except (TypeError, ValueError):
        # The file is created even if we don't write anything to it.
        json_file_path.unlink()

    pickle_file_path = dir_path / f"{file_name_prefix}.pickle"
    with pickle_file_path.open("wb") as f:
        cloudpickle.dump(value, f, protocol=PICKLE_PROTOCOL)

    return DumpDetails(
        file_path=pickle_file_path, format=ir.ArtifactFormat.ENCODED_PICKLE
    )
