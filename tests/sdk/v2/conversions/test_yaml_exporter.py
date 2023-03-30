################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

import orquestra.sdk.schema.ir as model
from orquestra.sdk._base._conversions import _imports
from orquestra.sdk._base._conversions import _yaml_exporter as yaml_converter
from orquestra.sdk.examples import exportable_wf


def _model_dict(model_obj):
    return json.loads(model_obj.json(exclude_none=True))


WORKFLOW_WITH_POSITIONAL_ARGS = model.WorkflowDef(
    name="positional",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="positional",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "orquestra-sdk": model.GitImport(
            id="orquestra-sdk",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-0-capitalize": model.TaskDef(
            id="task-0-capitalize",
            fn_ref=model.ModuleFunctionRef(
                module="examples.hello_v2",
                function_name="capitalize",
                file_path="examples/hello_v2.py",
                line_number=11,
            ),
            parameters=[
                model.TaskParameter(
                    name="text", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                ),
            ],
            source_import_id="orquestra-sdk",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-capitalize": model.ArtifactNode(
            id="artifact-0-capitalize",
        ),
    },
    constant_nodes={
        "constant-0": model.ConstantNodeJSON(
            id="constant-0",
            value='"hello"',
            value_preview="'hello'",
        ),
    },
    task_invocations={
        "invocation-0-task-0-capitalize": model.TaskInvocation(
            id="invocation-0-task-0-capitalize",
            task_id="task-0-capitalize",
            args_ids=[
                "constant-0",
            ],
            kwargs_ids={},
            output_ids=["artifact-0-capitalize"],
        ),
    },
    output_ids=["artifact-0-capitalize"],
)


WORKFLOW_WITH_POSITIONAL_ARTIFACT_ARGS = model.WorkflowDef(
    name="positional_artifact",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="positional",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "orquestra-sdk": model.GitImport(
            id="orquestra-sdk",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-0-capitalize": model.TaskDef(
            id="task-0-capitalize",
            fn_ref=model.ModuleFunctionRef(
                module="examples.hello_v2",
                function_name="capitalize",
                file_path="examples/hello_v2.py",
                line_number=11,
            ),
            parameters=[
                model.TaskParameter(
                    name="text", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                ),
            ],
            source_import_id="orquestra-sdk",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-capitalize": model.ArtifactNode(
            id="artifact-0-capitalize",
        ),
        "artifact-1-capitalize": model.ArtifactNode(
            id="artifact-1-capitalize",
        ),
    },
    constant_nodes={
        "constant-0": model.ConstantNodeJSON(
            id="constant-0",
            value='"hello"',
            value_preview="'hello'",
        ),
    },
    task_invocations={
        "invocation-0-task-0-capitalize": model.TaskInvocation(
            id="invocation-0-task-0-capitalize",
            task_id="task-0-capitalize",
            args_ids=[
                "constant-0",
            ],
            kwargs_ids={},
            output_ids=["artifact-0-capitalize"],
        ),
        "invocation-1-task-0-capitalize": model.TaskInvocation(
            id="invocation-1-task-0-capitalize",
            task_id="task-0-capitalize",
            args_ids=[
                "artifact-0-capitalize",
            ],
            kwargs_ids={},
            output_ids=["artifact-1-capitalize"],
        ),
    },
    output_ids=["artifact-1-capitalize"],
)


WORKFLOW_WITH_CUSTOM_JSON_ARGS = model.WorkflowDef(
    name="custom-json",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="custom_json",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "orquestra-sdk": model.GitImport(
            id="orquestra-sdk",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-0-last-element": model.TaskDef(
            id="task-0-last-element",
            fn_ref=model.ModuleFunctionRef(
                module="examples.hello_v2",
                function_name="last_element",
                file_path="examples/hello_v2.py",
                line_number=11,
            ),
            parameters=[
                model.TaskParameter(
                    name="text", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                ),
            ],
            source_import_id="orquestra-sdk",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-last-element": model.ArtifactNode(
            id="artifact-0-last-element",
        ),
    },
    constant_nodes={
        "constant-0": model.ConstantNodeJSON(
            id="constant-0",
            value="[1, 2, 3, 4, 5]",
            value_preview="[1, 2, 3, 4,",
        ),
    },
    task_invocations={
        "invocation-0-task-0-last-element": model.TaskInvocation(
            id="invocation-0-task-0-last-element",
            task_id="task-0-last-element",
            args_ids=[
                "constant-0",
            ],
            kwargs_ids={},
            output_ids=["artifact-0-last-element"],
        ),
    },
    output_ids=["artifact-0-last-element"],
)


WORKFLOW_WITH_PICKLED_ARGS = model.WorkflowDef(
    name="pickled",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="pickled",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "orquestra-sdk": model.GitImport(
            id="orquestra-sdk",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-0-capitalize": model.TaskDef(
            id="task-0-capitalize",
            fn_ref=model.ModuleFunctionRef(
                module="examples.hello_v2",
                function_name="capitalize",
                file_path="examples/hello_v2.py",
                line_number=11,
            ),
            parameters=[
                model.TaskParameter(
                    name="text", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                ),
            ],
            source_import_id="orquestra-sdk",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-capitalize": model.ArtifactNode(
            id="artifact-0-capitalize",
        ),
    },
    constant_nodes={
        "constant-0": model.ConstantNodePickle(
            id="constant-0",
            chunks=[
                "gASVLQAAAAAAAACMCmRpbGwuX2RpbGyUjApfbG9hZF90eXBllJOUjAZvYmplY3SUhZRSlCmBlC4=\\n"  # noqa: E501'
            ],
            value_preview="<object obje",
            serialization_format=model.ArtifactFormat.ENCODED_PICKLE,
        ),
    },
    task_invocations={
        "invocation-0-task-0-capitalize": model.TaskInvocation(
            id="invocation-0-task-0-capitalize",
            task_id="task-0-capitalize",
            args_ids=[
                "constant-0",
            ],
            kwargs_ids={},
            output_ids=["artifact-0-capitalize"],
        ),
    },
    output_ids=["artifact-0-capitalize"],
)


BASICS_WORKFLOW = model.WorkflowDef(
    name="basics",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="basics",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "welcome-to-orquestra": model.GitImport(
            id="welcome-to-orquestra",
            repo_url="git@github.com:zapatacomputing/tutorial-0-welcome",
            git_ref="master",
        ),
    },
    tasks={
        "welcome-welcome": model.TaskDef(
            id="welcome-welcome",
            fn_ref=model.ModuleFunctionRef(
                module="welcome",
                function_name="welcome",
                file_path="welcome.py",
                line_number=7,
            ),
            parameters=[],
            source_import_id="welcome-to-orquestra",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-welcome": model.ArtifactNode(
            id="artifact-0-welcome",
            custom_name="welcome",
        ),
    },
    constant_nodes={},
    task_invocations={
        "step-0-welcome-welcome": model.TaskInvocation(
            id="step-0-welcome-welcome",
            task_id="welcome-welcome",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-welcome"],
        ),
    },
    output_ids=["artifact-0-welcome"],
)

BASICS_WORKFLOW_WITH_GPU = model.WorkflowDef(
    name="basics-with-gpu",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="basics",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "welcome-to-orquestra": model.GitImport(
            id="welcome-to-orquestra",
            repo_url="git@github.com:zapatacomputing/tutorial-0-welcome",
            git_ref="master",
        ),
    },
    tasks={
        "welcome-welcome": model.TaskDef(
            id="welcome-welcome",
            fn_ref=model.ModuleFunctionRef(
                module="welcome",
                function_name="welcome",
                file_path="welcome.py",
                line_number=7,
            ),
            parameters=[],
            source_import_id="welcome-to-orquestra",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
                gpu="1",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-welcome": model.ArtifactNode(
            id="artifact-0-welcome",
            custom_name="welcome",
        ),
    },
    constant_nodes={},
    task_invocations={
        "step-0-welcome-welcome": model.TaskInvocation(
            id="step-0-welcome-welcome",
            task_id="welcome-welcome",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-welcome"],
        ),
    },
    output_ids=["artifact-0-welcome"],
)

BASICS_FILE_REF_WORKFLOW = model.WorkflowDef(
    name="basics-file-ref",
    fn_ref=model.FileFunctionRef(
        function_name="basics",
        file_path="welcome.py",
        line_number=13,
    ),
    imports={
        "welcome-to-orquestra": model.GitImport(
            id="welcome-to-orquestra",
            repo_url="git@github.com:zapatacomputing/tutorial-0-welcome",
            git_ref="master",
        ),
    },
    tasks={
        "welcome-welcome": model.TaskDef(
            id="welcome-welcome",
            fn_ref=model.FileFunctionRef(
                function_name="welcome",
                file_path="welcome.py",
                line_number=7,
            ),
            parameters=[],
            source_import_id="welcome-to-orquestra",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-welcome": model.ArtifactNode(
            id="artifact-0-welcome",
            custom_name="welcome",
        ),
    },
    constant_nodes={},
    task_invocations={
        "step-0-welcome-welcome": model.TaskInvocation(
            id="step-0-welcome-welcome",
            task_id="welcome-welcome",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-welcome"],
        ),
    },
    output_ids=["artifact-0-welcome"],
)


ADDITIONAL_METRICS = model.WorkflowDef(
    name="additional-metrics",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="additional_metrics",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "sklearn-component": model.GitImport(
            id="sklearn-component",
            repo_url=(
                "git@github.com:zapatacomputing" "/tutorial-orquestra-sklearn.git"
            ),
            git_ref="master",
        ),
    },
    tasks={
        ("steps-ml-tutorial-3-exercise-steps-generate-data-step"): model.TaskDef(
            # I know this is mouthful, but the idea was to use the fully
            # qualified python module name. In this example, the module
            # would be "steps.ml_tutorial_3_exercise_steps"
            # https://github.com/zapatacomputing/tutorial-orquestra-sklearn/tree/master/steps
            id="steps-ml-tutorial-3-exercise-steps-generate-data-step",
            fn_ref=model.ModuleFunctionRef(
                module="steps.ml_tutorial_3_exercise_steps",
                function_name="generate_data_step",
                file_path="steps/ml_tutorial_3_exercise_steps.py",
                line_number=4,
            ),
            parameters=[
                model.TaskParameter(
                    name="dataset_name", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                )
            ],
            source_import_id="sklearn-component",
            dependency_import_ids=[],
            resources=None,
        ),
        ("steps-ml-tutorial-3-exercise-steps-preprocess-data-step"): model.TaskDef(
            id="steps-ml-tutorial-3-exercise-steps-preprocess-data-step",
            fn_ref=model.ModuleFunctionRef(
                module="steps.ml_tutorial_3_exercise_steps",
                function_name="preprocess_data_step",
                file_path="steps/ml_tutorial_3_exercise_steps.py",
                line_number=10,
            ),
            parameters=[
                model.TaskParameter(
                    name="data", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                )
            ],
            source_import_id="sklearn-component",
            dependency_import_ids=[],
            resources=None,
        ),
    },
    artifact_nodes={
        "artifact-0-generate-data-step": model.ArtifactNode(
            id="artifact-0-generate-data-step",
            custom_name="data",
        ),
        "artifact-1-preprocess-data": model.ArtifactNode(
            id="artifact-1-preprocess-data",
            custom_name="features",
        ),
        "artifact-2-preprocess-data": model.ArtifactNode(
            id="artifact-2-preprocess-data",
            custom_name="labels",
        ),
    },
    constant_nodes={
        "literal-0": model.ConstantNodeJSON(
            id="literal-0",
            value='"simple_dataset"',
            value_preview="simple_datas",
            serialization_format=model.ArtifactFormat.JSON,
        ),
    },
    task_invocations={
        (
            "step-0-steps-ml-tutorial-3-exercise-steps-generate-data-step"
        ): model.TaskInvocation(
            id=("step-0-steps-ml-tutorial-3-exercise-steps" "-generate-data-step"),
            task_id="steps-ml-tutorial-3-exercise-steps-generate-data-step",
            args_ids=[],
            kwargs_ids={"dataset_name": "literal-0"},
            output_ids=["artifact-0-generate-data-step"],
        ),
        (
            "step-1-steps-ml-tutorial-3-exercise-steps-preprocess-data-step"
        ): model.TaskInvocation(
            id="step-1-steps-ml-tutorial-3-exercise-steps-preprocess-data-step",
            task_id="steps-ml-tutorial-3-exercise-steps-preprocess-data-step",
            args_ids=[],
            kwargs_ids={"data": "artifact-0-generate-data-step"},
            output_ids=[
                "artifact-1-preprocess-data",
                "artifact-2-preprocess-data",
            ],
        ),
    },
    output_ids=["artifact-1-preprocess-data"],
)


MULTIPLE_TASK_OUTPUTS = model.WorkflowDef(
    name="multiple-task-outputs",
    fn_ref=model.ModuleFunctionRef(
        module="test.v2.test_traversal",
        function_name="multiple_task_outputs",
        file_path="tests/sdk/v2/test_traversal.py",
        line_number=0,
        type="MODULE_FUNCTION_REF",
    ),
    imports={
        "my_import": model.GitImport(
            id="my_import",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-multi-output-99daf1a352": model.TaskDef(
            id="task-multi-output-99daf1a352",
            fn_ref=model.ModuleFunctionRef(
                module="orquestra.sdk.examples.exportable_wf",
                function_name="multi_output_test",
                file_path="src/orquestra/sdk/examples/exportable_wf.py",
                line_number=0,
            ),
            parameters=[],
            source_import_id="my_import",
        )
    },
    artifact_nodes={
        "artifact-0-multi-output": model.ArtifactNode(
            id="artifact-0-multi-output",
            artifact_index=0,
        ),
        "artifact-1-multi-output": model.ArtifactNode(
            id="artifact-1-multi-output",
            artifact_index=1,
        ),
        "artifact-2-multi-output": model.ArtifactNode(
            id="artifact-2-multi-output",
            artifact_index=1,
        ),
    },
    constant_nodes={},
    task_invocations={
        "invocation-0-task-multi-output": model.TaskInvocation(
            id="invocation-0-task-multi-output",
            task_id="task-multi-output-99daf1a352",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-multi-output", "artifact-1-multi-output"],
        ),
        "invocation-1-task-multi-output": model.TaskInvocation(
            id="invocation-1-task-multi-output",
            task_id="task-multi-output-99daf1a352",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-2-multi-output"],
        ),
    },
    output_ids=[
        "artifact-0-multi-output",
        "artifact-1-multi-output",
        "artifact-2-multi-output",
    ],
)

MULTIPLE_TASK_OUTPUTS_AS_INPUTS = model.WorkflowDef(
    name="multiple-task-outputs-as-inputs",
    fn_ref=model.ModuleFunctionRef(
        module="test.v2.test_traversal",
        function_name="multiple_task_outputs",
        file_path="tests/sdk/v2/test_traversal.py",
        line_number=0,
        type="MODULE_FUNCTION_REF",
    ),
    imports={
        "my_import": model.GitImport(
            id="my_import",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-multi-output-99daf1a352": model.TaskDef(
            id="task-multi-output-99daf1a352",
            fn_ref=model.ModuleFunctionRef(
                module="orquestra.sdk.examples.exportable_wf",
                function_name="multi_output_test",
                file_path="src/orquestra/sdk/examples/exportable_wf.py",
                line_number=0,
            ),
            parameters=[],
            source_import_id="my_import",
        )
    },
    artifact_nodes={
        "artifact-0-multi-output": model.ArtifactNode(
            id="artifact-0-multi-output",
            artifact_index=0,
        ),
        "artifact-1-multi-output": model.ArtifactNode(
            id="artifact-1-multi-output",
            artifact_index=1,
        ),
        "artifact-2-multi-output": model.ArtifactNode(
            id="artifact-2-multi-output",
            artifact_index=1,
        ),
    },
    constant_nodes={},
    task_invocations={
        "invocation-0-task-multi-output": model.TaskInvocation(
            id="invocation-0-task-multi-output",
            task_id="task-multi-output-99daf1a352",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-multi-output", "artifact-1-multi-output"],
        ),
        "invocation-1-task-multi-output": model.TaskInvocation(
            id="invocation-1-task-multi-output",
            task_id="task-multi-output-99daf1a352",
            args_ids=["artifact-0-multi-output", "artifact-1-multi-output"],
            kwargs_ids={},
            output_ids=["artifact-2-multi-output"],
        ),
    },
    output_ids=[
        "artifact-2-multi-output",
    ],
)


DATA_AGGREGATION_SETTINGS = model.WorkflowDef(
    name="basics-with-data-aggregation",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="basics",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "welcome-to-orquestra": model.GitImport(
            id="welcome-to-orquestra",
            repo_url="git@github.com:zapatacomputing/tutorial-0-welcome",
            git_ref="master",
        ),
    },
    tasks={
        "welcome-welcome": model.TaskDef(
            id="welcome-welcome",
            fn_ref=model.ModuleFunctionRef(
                module="welcome",
                function_name="welcome",
                file_path="welcome.py",
                line_number=7,
            ),
            parameters=[],
            source_import_id="welcome-to-orquestra",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
                gpu="1",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-welcome": model.ArtifactNode(
            id="artifact-0-welcome",
            custom_name="welcome",
        ),
    },
    constant_nodes={},
    task_invocations={
        "step-0-welcome-welcome": model.TaskInvocation(
            id="step-0-welcome-welcome",
            task_id="welcome-welcome",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-welcome"],
        ),
    },
    output_ids=["artifact-0-welcome"],
    data_aggregation=model.DataAggregation(
        run=True, resources=model.Resources(cpu="1", memory="1", disk="1", gpu="1")
    ),
)


INLINE_TASK = model.WorkflowDef(
    name="basics",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="basics",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "welcome-to-orquestra": model.GitImport(
            id="welcome-to-orquestra",
            repo_url="git@github.com:zapatacomputing/tutorial-0-welcome",
            git_ref="master",
        ),
        "inline-import-0": model.InlineImport(
            id="inline-import-0",
        ),
    },
    tasks={
        "welcome-welcome": model.TaskDef(
            id="task-capitalize-inline-8bba29c9af",
            fn_ref=model.InlineFunctionRef(
                function_name="capitalize_inline",
                encoded_function=[
                    "gANjZGlsbC5fZGlsbApfY3JlYXRlX2Z1bmN0aW9uCnEAKGNkaWxsLl9kaWxsCl9jcmVhdGVfY29k\n"  # noqa: E501'
                    "ZQpxAShLAUsASwBLAUsCS0NDCHwAoAChAFMAcQJOhXEDWAoAAABjYXBpdGFsaXplcQSFcQVYBAAA\n"  # noqa: E501'
                    "AHRleHRxBoVxB1hFAAAAL1VzZXJzL3NlYmFzdGlhbm1vcmF3aWVjL2dpdC9vcnF1ZXN0cmEtc2Rr\n"  # noqa: E501'
                    "L3Rlc3RzL3YyL3Rlc3RfdHJhdmVyc2FsLnB5cQhYEQAAAGNhcGl0YWxpemVfaW5saW5lcQlLakMC\n"  # noqa: E501'
                    "AARxCikpdHELUnEMfXENWAgAAABfX25hbWVfX3EOWBcAAAB0ZXN0cy52Mi50ZXN0X3RyYXZlcnNh\n"  # noqa: E501'
                    "bHEPc2gJTk50cRBScRF9cRIoWBcAAABfVGFza0RlZl9fc2RrX3Rhc2tfYm9keXETaBFYBgAAAGZu\n"  # noqa: E501'
                    "X3JlZnEUY2RpbGwuX2RpbGwKX2NyZWF0ZV9uYW1lZHR1cGxlCnEVWBEAAABJbmxpbmVGdW5jdGlv\n"  # noqa: E501'
                    "blJlZnEWWA0AAABmdW5jdGlvbl9uYW1lcRdYAgAAAGZucRiGcRlYFQAAAG9ycXVlc3RyYS5zZGsu\n"  # noqa: E501'
                    "djIuX2RzbHEah3EbUnEcaAloEYZxHYFxHlgNAAAAc291cmNlX2ltcG9ydHEfaBVYDAAAAElubGlu\n"  # noqa: E501'
                    "ZUltcG9ydHEgKWgah3EhUnEiKYFxI1gPAAAAb3V0cHV0X21ldGFkYXRhcSRoFVgSAAAAVGFza091\n"  # noqa: E501'
                    "dHB1dE1ldGFkYXRhcSVYEAAAAGlzX3N1YnNjcmlwdGFibGVxJlgJAAAAbl9vdXRwdXRzcSeGcSho\n"  # noqa: E501'
                    "GodxKVJxKolLAYZxK4FxLFgKAAAAcGFyYW1ldGVyc3EtY2NvbGxlY3Rpb25zCk9yZGVyZWREaWN0\n"  # noqa: E501'
                    "CnEuKVJxL2gGaBVYDQAAAFRhc2tQYXJhbWV0ZXJxMFgEAAAAbmFtZXExWAQAAABraW5kcTKGcTNo\n"  # noqa: E501'
                    "GodxNFJxNWgGY29ycXVlc3RyYS5zZGsudjIuX2RzbApQYXJhbWV0ZXJLaW5kCnE2WBUAAABQT1NJ\n"  # noqa: E501'
                    "VElPTkFMX09SX0tFWVdPUkRxN4VxOFJxOYZxOoFxO3NYEgAAAGRlcGVuZGVuY3lfaW1wb3J0c3E8\n"  # noqa: E501'
                    "TlgJAAAAcmVzb3VyY2VzcT1OWAwAAABjdXN0b21faW1hZ2VxPlgoAAAAemFwYXRhY29tcHV0aW5n\n"  # noqa: E501'
                    "L29ycXVlc3RyYS1kZWZhdWx0OnYwLjAuMHE/WAsAAABjdXN0b21fbmFtZXFATnV9cUFYDwAAAF9f\n"  # noqa: E501'
                    "YW5ub3RhdGlvbnNfX3FCfXFDaAZjZGlsbC5fZGlsbApfbG9hZF90eXBlCnFEWAMAAABzdHJxRYVx\n"  # noqa: E501'
                    "RlJxR3NzhnFIYi4=\n"
                ],
            ),
            parameters=[
                model.TaskParameter(
                    name="text", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                )
            ],
            source_import_id="inline-import-0",
        )
    },
    artifact_nodes={
        "artifact-0-welcome": model.ArtifactNode(
            id="artifact-0-welcome",
            custom_name="welcome",
        ),
    },
    constant_nodes={},
    task_invocations={
        "step-0-welcome-welcome": model.TaskInvocation(
            id="step-0-welcome-welcome",
            task_id="welcome-welcome",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-welcome"],
        ),
    },
    output_ids=["artifact-0-welcome"],
    data_aggregation=model.DataAggregation(
        run=True, resources=model.Resources(cpu="1", memory="1", disk="1", gpu="1")
    ),
)


PYTHON_REQUIREMENTS = model.WorkflowDef(
    name="python-import",
    fn_ref=model.ModuleFunctionRef(
        module="workflow_defs",
        function_name="wf",
        file_path="workflow_defs.py",
        line_number=44,
        type="MODULE_FUNCTION_REF",
    ),
    imports={
        "hello_zapata": model.GitImport(
            id="hello_zapata",
            repo_url="git@github.com:zapatacomputing/evangelism-workflows.git",
            git_ref="morawiec/test",
            type="GIT_IMPORT",
        ),
        "git-1871174d6e_github_com_zapatacomputing_orquestra_sdk": model.GitImport(
            id="git-1871174d6e_github_com_zapatacomputing_orquestra_sdk",
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
            type="GIT_IMPORT",
        ),
        "python-import-86be3435bb": model.PythonImports(
            id="python-import-86be3435bb",
            packages=[
                model.PackageSpec(
                    name="numpy",
                    extras=[],
                    version_constraints=["==1.21.5"],
                    environment_markers="",
                ),
                model.PackageSpec(
                    name="pip",
                    extras=[],
                    version_constraints=[],
                    environment_markers="",
                ),
            ],
            pip_options=[],
            type="PYTHON_IMPORT",
        ),
    },
    tasks={
        "task-my-task-ed08df330d": model.TaskDef(
            id="task-my-task-ed08df330d",
            fn_ref=model.ModuleFunctionRef(
                module="workflow_defs",
                function_name="my_task",
                file_path="workflow_defs.py",
                line_number=36,
                type="MODULE_FUNCTION_REF",
            ),
            parameters=[
                model.TaskParameter(
                    name="prev", kind=model.ParameterKind.POSITIONAL_OR_KEYWORD
                )
            ],
            source_import_id="hello_zapata",
            dependency_import_ids=[
                "git-1871174d6e_github_com_zapatacomputing_orquestra_sdk",
                "python-import-86be3435bb",
            ],
            resources=None,
            custom_image="zapatacomputing/orquestra-default:v0.0.0",
        )
    },
    artifact_nodes={
        "artifact-0-my-task": model.ArtifactNode(
            id="artifact-0-my-task",
            custom_name=None,
            serialization_format=model.ArtifactFormat.AUTO,
            artifact_index=None,
        )
    },
    constant_nodes={
        "constant-0": model.ConstantNodeJSON(
            id="constant-0",
            value="null",
            serialization_format=model.ArtifactFormat.JSON,
            value_preview="None",
        )
    },
    task_invocations={
        "invocation-0-task-my-task": model.TaskInvocation(
            id="invocation-0-task-my-task",
            task_id="task-my-task-ed08df330d",
            args_ids=["constant-0"],
            kwargs_ids={},
            output_ids=["artifact-0-my-task"],
            resources=None,
        )
    },
    output_ids=["artifact-0-my-task"],
    data_aggregation=None,
)


@pytest.fixture
def override_semver(monkeypatch):
    version_mock = Mock(return_value="main")
    monkeypatch.setattr(_imports, "_get_package_version_tag", version_mock)


DATA_PATH = Path("tests/sdk/v2/conversions/data")


class TestWorkflowToYAML:
    @pytest.mark.parametrize(
        "workflow,ref_yaml_path",
        [
            (WORKFLOW_WITH_POSITIONAL_ARGS, DATA_PATH / "positional.yml"),
            (
                WORKFLOW_WITH_POSITIONAL_ARTIFACT_ARGS,
                DATA_PATH / "positional_artifact.yml",
            ),
            (WORKFLOW_WITH_CUSTOM_JSON_ARGS, DATA_PATH / "custom_json.yml"),
            (BASICS_WORKFLOW, DATA_PATH / "basics.yml"),
            (BASICS_WORKFLOW_WITH_GPU, DATA_PATH / "basics_with_gpu.yml"),
            (BASICS_FILE_REF_WORKFLOW, DATA_PATH / "basics_file_ref.yml"),
            (ADDITIONAL_METRICS, DATA_PATH / "additional-metrics.yml"),
            (MULTIPLE_TASK_OUTPUTS, DATA_PATH / "multi_task_outputs.yml"),
            (
                MULTIPLE_TASK_OUTPUTS_AS_INPUTS,
                DATA_PATH / "multi_task_outputs_as_inputs.yml",
            ),
            (
                DATA_AGGREGATION_SETTINGS,
                DATA_PATH / "basics_with_data_aggregation.yml",
            ),
            (INLINE_TASK, DATA_PATH / "inline_function.yml"),
            (PYTHON_REQUIREMENTS, DATA_PATH / "python_import.yml"),
        ],
    )
    def test_matches_ref_file(self, override_semver, workflow, ref_yaml_path):
        with ref_yaml_path.open() as f:
            ref_yaml_contents = yaml.safe_load(f)

        assert (
            _model_dict(yaml_converter.workflow_to_yaml(workflow)) == ref_yaml_contents
        )

    @pytest.mark.parametrize(
        "workflow",
        [
            WORKFLOW_WITH_PICKLED_ARGS,
        ],
    )
    def test_for_pickled_constants(self, override_semver, workflow):
        """
        This method check if is possible to create the yaml representation of a workflow
        that contains constant nodes with pickled values.

        Params
            workflow: model.WorkflowDef = A workflow definition
        """
        _ = yaml_converter.workflow_to_yaml(workflow)


OVERRIDE_WORKFLOW = model.WorkflowDef(
    name="basics",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="basics",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "welcome-to-orquestra": model.GitImport(
            id="welcome-to-orquestra",
            repo_url="git@github.com:zapatacomputing/tutorial-0-welcome",
            git_ref="master",
        ),
    },
    tasks={
        "welcome-welcome": model.TaskDef(
            id="welcome-welcome",
            fn_ref=model.ModuleFunctionRef(
                module="welcome",
                function_name="welcome",
                file_path="welcome.py",
                line_number=7,
            ),
            parameters=[],
            source_import_id="welcome-to-orquestra",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0-welcome": model.ArtifactNode(
            id="artifact-0-welcome",
            custom_name="welcome",
        ),
    },
    constant_nodes={},
    task_invocations={
        "step-0-welcome-welcome": model.TaskInvocation(
            id="step-0-welcome-welcome",
            task_id="welcome-welcome",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0-welcome"],
        ),
    },
    output_ids=["artifact-0-welcome"],
)


def test_workflow_to_yaml_override_git_ref():
    workflow = OVERRIDE_WORKFLOW
    ref_yaml_path = DATA_PATH / "basics.yml"

    with ref_yaml_path.open() as f:
        ref_yaml_contents = yaml.safe_load(f)

    ref_yaml_contents["imports"][1]["parameters"]["commit"] = "v0.26.0"

    assert (
        _model_dict(
            yaml_converter.workflow_to_yaml(
                workflow,
                orq_sdk_git_ref="v0.26.0",
            )
        )
        == ref_yaml_contents
    )


@pytest.mark.parametrize(
    "wf_def,yaml_path",
    [
        (exportable_wf.my_workflow, DATA_PATH / "exportable_wf.yml"),
    ],
)
def test_real_wf_to_yaml(monkeypatch, wf_def, yaml_path):
    def version_mock(package_name: str) -> str:
        if package_name == "orquestra-sdk":
            return "v0.19.0"
        else:
            return "v0.1.0"

    monkeypatch.setattr(_imports, "_get_package_version_tag", version_mock)

    workflow = wf_def.model

    with open(yaml_path) as f:
        ref_yaml_contents = yaml.safe_load(f)

    assert _model_dict(yaml_converter.workflow_to_yaml(workflow)) == ref_yaml_contents


def test_main(override_semver, monkeypatch, capsys):
    with (DATA_PATH / "main_test.json").open() as json_f:
        monkeypatch.setattr("sys.stdin", json_f)
        yaml_converter.main()
        captured = capsys.readouterr()
        with (DATA_PATH / "main_test.yml").open() as yaml_f:
            assert captured.out.strip() == yaml_f.read().strip()


PLAIN_WORKFLOW = model.WorkflowDef(
    name="plain",
    fn_ref=model.ModuleFunctionRef(
        module="does_not.exist",
        function_name="plain",
        file_path="does_not/exist.py",
        line_number=13,
    ),
    imports={
        "hello": model.GitImport(
            id="hello",
            repo_url="git@github.com:zapatacomputing/hello-world.git",
            git_ref="main",
        ),
    },
    tasks={
        "task-0_hello": model.TaskDef(
            id="task-0_hello",
            fn_ref=model.ModuleFunctionRef(
                module="does_not.matter",
                function_name="hello",
                file_path="does_not/matter.py",
                line_number=1,
            ),
            parameters=[],
            source_import_id="orquestra-sdk",
            dependency_import_ids=[],
            resources=model.Resources(
                cpu="1000m",
                memory="1Gi",
                disk="15Gi",
                gpu="1",
            ),
        ),
    },
    artifact_nodes={
        "artifact-0_hello": model.ArtifactNode(
            id="artifact-0_hello",
        ),
    },
    constant_nodes={},
    task_invocations={
        "invocation-0_task-0_hello": model.TaskInvocation(
            id="invocation-0_task-0_hello",
            task_id="task-0_hello",
            args_ids=[],
            kwargs_ids={},
            output_ids=["artifact-0_hello"],
        ),
    },
    output_ids=["artifact-0_hello"],
)


def test_workflow_with_invalid_package_version(monkeypatch):
    version_mock = Mock(side_effect=_imports.PackageVersionError("mock", "mocked"))
    monkeypatch.setattr(_imports, "_get_package_version_tag", version_mock)
    with pytest.raises(RuntimeError):
        _ = yaml_converter.workflow_to_yaml(PLAIN_WORKFLOW)
