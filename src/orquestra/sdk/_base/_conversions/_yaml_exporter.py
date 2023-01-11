#################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import io
import json
import typing as t

import yaml

import orquestra.sdk.schema.ir as model
from orquestra.sdk.schema import yaml_model
from orquestra.sdk.schema.yaml_model import DataAggregation

from ._imports import ImportTranslator, PackageVersionError
from ._invocations import InvocationTranslator


def _normalize_wf_name(sdk_name: str) -> str:
    """Make the workflow def name compliant with QE.

    Known limitation: after we submit a "normalized" workflow to QE, the SDK might
    not be able to infer back the original workflow name. For now, this is
    okay, because no existing functionality requires it. We store the workflow
    def in a client-side DB and we are able to identify these things by workflow run ID.

    More info: orquestra.sdk.schema.yaml_model.WorkflowName
    """
    return sdk_name.replace("_", "-")


def workflow_to_yaml(
    workflow: model.WorkflowDef,
    orq_wf_git_ref: t.Optional[str] = None,
    orq_sdk_git_ref: t.Optional[str] = None,
) -> yaml_model.Workflow:
    if workflow.data_aggregation is None:
        data_aggregation = None
    elif workflow.data_aggregation.resources is None:
        data_aggregation = DataAggregation(
            run=workflow.data_aggregation.run, resources=None
        )
    else:
        resources = yaml_model.Resources(
            cpu=workflow.data_aggregation.resources.cpu,
            memory=workflow.data_aggregation.resources.memory,
            disk=workflow.data_aggregation.resources.disk,
            gpu=workflow.data_aggregation.resources.gpu,
        )
        data_aggregation = DataAggregation(
            run=workflow.data_aggregation.run, resources=resources
        )

    try:
        import_translator = ImportTranslator(
            ir_imports=workflow.imports,
            orq_sdk_git_ref=orq_sdk_git_ref,
        )
    except PackageVersionError as e:
        raise RuntimeError(
            f"Unable to automatically figure out the `git_ref` for {e.package_name}.\n"
            f"{str(e)}\n"
            "Please add the Git repository for this package to your workflow "
            "tasks manually.\n"
            "If you're not expecting to see this, please contact the SDK team."
        ) from e

    # Add SDK imports to the step if they're not already included
    # The order is important:
    # - orquestra-sdk should be imported before any user code
    sdk_lib_names = [
        import_translator.orq_sdk_import_name,
    ]

    inv_translator = InvocationTranslator(
        wf=workflow,
        ir_yaml_import_map=import_translator.ir_yaml_id_map,
        prepend_imports=sdk_lib_names,
        sdk_import_name=import_translator.orq_sdk_import_name,
    )

    return yaml_model.Workflow(
        apiVersion="io.orquestra.workflow/1.0.0",
        name=_normalize_wf_name(workflow.name),
        imports=list(import_translator.yaml_imports.values()),
        steps=inv_translator.yaml_steps,
        types=[
            inv_translator.sdk_metadata_type_name,
            inv_translator.result_dict_type_name,
        ],
        dataAggregation=data_aggregation,
    )


def pydantic_to_yaml(obj) -> str:
    """Workaround for funky yaml output of Pydantic enums."""
    json_str = obj.json(exclude_none=True)
    json_dict = json.loads(json_str)
    buf = io.StringIO()
    yaml.dump(json_dict, buf)
    buf.seek(0)
    return buf.read()


def main():
    workflow_json_str = input()
    workflow_json = json.loads(workflow_json_str)
    workflow = model.WorkflowDef.parse_obj(workflow_json)
    v1_workflow = workflow_to_yaml(workflow)
    print(pydantic_to_yaml(v1_workflow))


if __name__ == "__main__":
    main()
