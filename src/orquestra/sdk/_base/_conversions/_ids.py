################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import re
import typing as t
from functools import singledispatch

from orquestra.sdk.schema import ir

ID_SEPARATOR = "-"
MAX_WORD_LEN = 3
MAX_WORDS_IN_ID = 3


def generate_yaml_ids(ir_objs: t.Iterable[t.Any]) -> t.Mapping[str, t.Any]:
    """
    Generates short, human readable IDs for use inside YAMLs sent to QE.

    The generated IDs are guaranteed to be unique in the scope of this collection,
    but they won't be unique globally.

    Input: IR objects (orquestra.sdk.schema.ir). Output: names/IDs suitable for QE
    (orquestra.sdk.schema.yaml_model).

    Goals:
    - short – this highly impacts workflow size limits on QE.
    - kinda human readable – for ease of debugging
    - suitable for embedding in the QE YAML.
    - local uniqueness – these names/IDs are unique in scope of a single workflow.

    Non-goals:
    - global uniqueness: ID collisions across workflow defs are expected.

    Returns:
        A dict where the key is the generated ID, and the value is the object it
        was generated for.
    """

    # Key: generated short ID.
    # Value: list of IR object with the same, colliding short ID.
    id_objs: t.MutableMapping[str, t.List] = {}
    for ir_obj in ir_objs:
        short_id = _id_for_ir_obj(ir_obj)
        id_objs.setdefault(short_id, []).append(ir_obj)

    # Multiple values in the list <=> ID collision
    disambiguated_id_objs = {}
    for short_id, collided_objs in id_objs.items():
        if len(collided_objs) == 1:
            disambiguated_id_objs[short_id] = collided_objs[0]
        else:
            for collided_obj_i, collided_obj in enumerate(collided_objs):
                new_id = f"{short_id}{ID_SEPARATOR}{collided_obj_i}"
                disambiguated_id_objs[new_id] = collided_obj

    return disambiguated_id_objs


def _shorten_id(tokens: t.Sequence[str]) -> str:
    short_tokens = [token[:MAX_WORD_LEN] for token in tokens[:MAX_WORDS_IN_ID]]
    return ID_SEPARATOR.join(short_tokens)


@singledispatch
def _id_for_ir_obj(obj) -> str:  # pragma: no cover
    raise TypeError(f"Invalid obj type: {obj}")


@_id_for_ir_obj.register
def _id_for_GitImport(imp: ir.GitImport):
    proj_name = imp.repo_url

    # Get the repo name
    # "git@github.com:zapatacomputing/orquestra-core.git" -> "orquestra-core.git"
    proj_name = proj_name.rsplit("/", maxsplit=2)[-1]

    # Remove the trailing '.git'
    # "orquestra-core.git" -> "orquestra-core"
    proj_name = proj_name.rsplit(".git", maxsplit=2)[0]

    # Replace all non-alphanumeric characters with an underscore.
    # Multiple special characters are grouped together with a single underscore.
    # "orquestra--core" -> "orquestra_core"
    proj_name = re.sub("[^A-Za-z0-9]+", "_", proj_name)

    tokens = proj_name.split("_")

    return _shorten_id(tokens)


@_id_for_ir_obj.register
def _id_for_InlineImport(imp: ir.InlineImport):
    # The IR's ID should be good enough for QE. Inline imports generate a lot of
    # embedded data anyway, the base64-encoded pickles are likely to shadow any
    # gains we make with heuristics over IDs.
    return imp.id


@_id_for_ir_obj.register
def _id_for_PythonImports(imp: ir.PythonImports):
    # There's not a lot of nice fields on PythonImports we could use to make the
    # ID user-friendly.
    return "py"
