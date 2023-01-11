################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import json

import numpy as np
import numpy.testing
import pytest
from pydantic.error_wrappers import ValidationError

from orquestra.sdk._base import serde
from orquestra.sdk.schema import ir

ROUNDTRIP_EXAMPLES = [
    None,
    "hello",
    {"foo": "bar"},
    {"foo": "bar", "baz": ["qux", "qux"]},
    {"a_float": 0.123},
]


class TestResultFromArtifact:
    @pytest.mark.parametrize(
        "artifact",
        [
            *ROUNDTRIP_EXAMPLES,
            {"a_nan": float("nan")},
            {"an_inf": float("inf")},
            {"a_minus_inf": float("-inf")},
            set(),
            np.eye(3),
        ],
    )
    def test_model_can_be_dumped(self, artifact):
        model = serde.result_from_artifact(artifact, ir.ArtifactFormat.AUTO)
        _ = model.json()

    @pytest.mark.parametrize("artifact", ROUNDTRIP_EXAMPLES)
    def test_roundtrip_for_small_values(self, artifact):
        model = serde.result_from_artifact(artifact, ir.ArtifactFormat.AUTO)
        json_dict = json.loads(model.json())
        value = serde.value_from_result_dict(json_dict)
        assert value == artifact

    @pytest.mark.parametrize("artifact", [np.eye(100)])
    def test_pickle_roundtrip_for_big_values(self, artifact):
        model = serde.result_from_artifact(artifact, ir.ArtifactFormat.AUTO)
        assert model.serialization_format == ir.ArtifactFormat.ENCODED_PICKLE
        assert len(model.chunks) > 1

        json_dict = json.loads(model.json())
        retrieved = serde.value_from_result_dict(json_dict)
        np.testing.assert_array_equal(retrieved, artifact)

    def test_unknown_format(self):
        result = {"serialization_format": "Made Up Format"}
        with pytest.raises(ValidationError):
            _ = serde.value_from_result_dict(result)


def test_deserialization_fails_for_auto_format():
    json_dict = {
        "serialization_format": ir.ArtifactFormat.AUTO.value,
    }
    with pytest.raises(ValueError):
        _ = serde.value_from_result_dict(json_dict)


def test_deserialize_constant_json():
    val = 2
    constant = ir.ConstantNodeJSON(value=val, id="", value_preview="")
    assert serde.deserialize_constant(constant) == val


def test_deserialize_constant_pickle():
    array = np.eye(10)
    constant = ir.ConstantNodePickle(
        chunks=serde._encoded_pickle_chunks(array), id="", value_preview=""
    )
    np.testing.assert_array_equal(serde.deserialize_constant(constant), array)


def test_roundtrip_function_serialize():
    def fun():
        return "hello there"

    serialized = serde.serialize_pickle(fun)

    assert fun() == (serde.deserialize_pickle(serialized))()


@pytest.mark.parametrize(
    "package_spec, expected",
    [
        (
            ir.PackageSpec(
                name="package",
                extras=[],
                version_constraints=["==1.1"],
                environment_markers="",
            ),
            "package==1.1",
        ),
        (
            ir.PackageSpec(
                name="package",
                extras=[],
                version_constraints=["==1.1", ">= 2.8.1"],
                environment_markers="",
            ),
            "package==1.1,>= 2.8.1",
        ),
        (
            ir.PackageSpec(
                name="package",
                extras=["my_extra_1", "my_extra_2"],
                version_constraints=["==1.1", ">= 2.8.1"],
                environment_markers="",
            ),
            "package[my_extra_1,my_extra_2]==1.1,>= 2.8.1",
        ),
        (
            ir.PackageSpec(
                name="package",
                extras=["my_extra_1", "my_extra_2"],
                version_constraints=["==1.1", ">= 2.8.1"],
                environment_markers='python_version < "2.7"',
            ),
            'package[my_extra_1,my_extra_2]==1.1,>= 2.8.1; python_version < "2.7"',
        ),
    ],
)
def test_stringify_package_spec(package_spec, expected):
    assert serde.stringify_package_spec(package_spec) == expected
