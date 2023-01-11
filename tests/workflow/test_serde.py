################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk.schema import ir

MODELS = [
    ir.WorkflowDef(
        name="single_invocation",
        fn_ref=ir.FileFunctionRef(
            file_path="examples/hello_v2.py",
            function_name="my_workflow",
            line_number=42,
        ),
        imports={"local-0": ir.LocalImport(id="local-0")},
        tasks={
            "task-0_capitalize": ir.TaskDef(
                id="task-0_capitalize",
                fn_ref=ir.ModuleFunctionRef(
                    module="orquestra.examples",
                    function_name="capitalize",
                    line_number=42,
                ),
                source_import_id="local-0",
                parameters=[
                    ir.TaskParameter(
                        name="text",
                        kind=ir.ParameterKind.POSITIONAL_OR_KEYWORD,
                    )
                ],
            )
        },
        artifact_nodes={
            "artifact-0_capitalize": ir.ArtifactNode(
                id="artifact-0_capitalize",
            )
        },
        constant_nodes={
            "constant-0": ir.ConstantNodeJSON(
                id="constant-0",
                value='"emiliano"',
                value_preview='"emiliano"',
            ),
            "constant-1": ir.ConstantNodePickle(
                id="constant-0",
                chunks=['"emiliano"'],
                value_preview='"emiliano"',
            ),
        },
        task_invocations={
            "invocation-0_task-0_capitalize": ir.TaskInvocation(
                id="invocation-0_task-0_capitalize",
                task_id="task-0_capitalize",
                args_ids=[],
                kwargs_ids={"text": "constant-0"},
                output_ids=["artifact-0_capitalize"],
            ),
        },
        output_ids=["artifact-0_capitalize", "constant-0"],
    )
]


@pytest.mark.parametrize("model", MODELS)
def test_model_dict_model_roundtrip(model):
    model_cls = type(model)
    parsed = model_cls.parse_obj(model.dict())
    assert parsed == model
