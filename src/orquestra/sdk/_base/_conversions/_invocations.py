################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import json
import pathlib
import typing as t

from orquestra.sdk.schema import ir, responses, yaml_model

# TODO: it would be nice to have a single document where the user could look up a "type"
# from the workflow yaml and see what it's about.
RESULT_DICT_TYPE_NAME = "workflow-result-dict"
SDK_METADATA_TYPE_NAME = "sdk-metadata"


class InvocationTranslator:
    """
    Translates task invocations from the IR (`orquestra.sdk.schema.ir`) to YAML
    (`orquestra.sdk.schema.yaml_model`).

    Objects of this class are considered immutable. All the computation is done in the
    `__init__`.
    """

    def __init__(
        self,
        wf: ir.WorkflowDef,
        ir_yaml_import_map: t.Mapping[ir.ImportId, yaml_model.ImportName],
        prepend_imports: t.Sequence[yaml_model.ImportName],
        sdk_import_name: yaml_model.ImportName,
    ):
        """
        Args:
            wf: used to get the additional information that's not included in the IR
                "task invocations", but is required to produce yaml "steps".
            ir_yaml_import_map: tells us how "imports" will be named in the produced
                yaml. This is retained as an attribute. Why do we need it? QE yamls
                have two sections: imports (produced elsewhere) and steps (produced by
                this class). The steps refer to the imports by names, so they have to
                match.
            prepend_imports: import names to prepend in each produced yaml step. If
                some of these are already present in the step, they won't be duplicated.
            sdk_import_name: required to produce a proper filepath+function for the
                task code entrypoint
        """
        # 1. Prep dicts ahead of time
        # 2. Iterate over invocations, and prepare fields one-by-one.

        self._ir_yaml_import_map = ir_yaml_import_map

        self._task_defs_dict = wf.tasks
        self._constants_dict = wf.constant_nodes
        self._artifacts_dict = wf.artifact_nodes
        self._ir_imports = wf.imports
        # Tells what invocation is required to produce a given artifact.
        self._artifact_producer_dict: t.Mapping[
            ir.ArtifactNodeId, ir.TaskInvocation
        ] = _make_producer_dict(wf.task_invocations.values())

        self._prepend_imports = prepend_imports
        self._sdk_import_name = sdk_import_name

        self._yaml_steps = [
            self._make_yaml_step(inv) for inv in wf.task_invocations.values()
        ]

    @property
    def yaml_steps(self) -> t.Sequence[yaml_model.Step]:
        return self._yaml_steps

    @property
    def result_dict_type_name(self):
        return RESULT_DICT_TYPE_NAME

    @property
    def sdk_metadata_type_name(self):
        return SDK_METADATA_TYPE_NAME

    def _make_yaml_step(self, inv: ir.TaskInvocation):
        task = self._task_defs_dict[inv.task_id]
        task_resources = inv.resources or task.resources
        if task_resources is None:
            step_resources = None
        else:
            step_resources = yaml_model.Resources(
                cpu=task_resources.cpu,
                memory=task_resources.memory,
                disk=task_resources.disk,
                gpu=task_resources.gpu,
            )

        dependency_ids = task.dependency_import_ids or []

        ir_import_ids = [task.source_import_id, *dependency_ids]
        step_import_names = [
            self._ir_yaml_import_map[ir_id]
            for ir_id in ir_import_ids
            if not isinstance(self._ir_imports[ir_id], ir.InlineImport)
        ]

        for name in reversed(self._prepend_imports):
            if name not in step_import_names:
                step_import_names.insert(0, name)

        return yaml_model.Step(
            name=inv.id,
            config=yaml_model.StepConfig(
                runtime=yaml_model.Runtime(
                    language=yaml_model.RuntimeLanguage.PYTHON3,
                    customImage=inv.custom_image or task.custom_image,
                    imports=step_import_names,
                    parameters=yaml_model.RuntimeParameters(
                        # Hardcoded reference of the single entrypoint "dispatcher" in
                        # the SDK.
                        file=_make_fn_file_path(
                            self._sdk_import_name,
                            "src/orquestra/sdk/_base/dispatch.py",
                        ),
                        function="exec_task_fn",
                    ),
                ),
                resources=step_resources,
            ),
            inputs=[
                *self._make_sdk_metadata(
                    task.source_import_id,
                    task.fn_ref,
                    inv.output_ids,
                ),
                *self._make_args_inputs(inv.args_ids),
                *self._make_kwargs_inputs(inv.kwargs_ids),
            ],
            outputs=self._make_outputs(inv.output_ids),
            passed=_resolve_dependent_invocations(inv, self._artifact_producer_dict),
        )

    def _make_sdk_metadata(
        self,
        source_import_id: ir.ImportId,
        fn_ref: ir.FunctionRef,
        output_ids: t.Iterable[ir.ArtifactNodeId],
    ):
        output_dict = {
            "__sdk_output_node_dicts": [
                {
                    **art_dict,
                    "id": art_dict["id"],
                }
                for output_id in output_ids
                for art_dict in [self._artifacts_dict[output_id].dict()]
            ],
            "type": self.sdk_metadata_type_name,
        }

        source_import_name = self._ir_yaml_import_map[source_import_id]

        fn_dict = {
            "__sdk_fn_ref_dict": _fn_ref_dict(fn_ref, source_import_name),
            "type": self.sdk_metadata_type_name,
        }

        # The following is closely related to how python3-runtime runs code referenced
        # in the yaml:
        # https://github.com/zapatacomputing/python3-runtime/blob/4f9eb3ff470683dbaaf8566ce9fc21db120d9c35/run#L319
        #
        # tl;dr:
        # - pwd is set to `/app`
        # - "imports" are cloned to `/app/step/<import name>`
        #
        # Python projects can be either (1) setuptools-like packages that can be
        # pip-installed locally, or (2) a collection of loose directories and python
        # files.
        #
        # To make Orquestra work with (2) we need to make Python recognize the files in
        # the project directory. The list of directories where Python looks for packages
        # is `sys.path`. The following metadata entry will be used by the dispatcher
        # function to extend `sys.path`.
        sys_path_dict = {
            "__sdk_additional_sys_paths": [f"step/{source_import_name}"],
            "type": self.sdk_metadata_type_name,
        }

        return [fn_dict, output_dict, sys_path_dict]

    def _make_args_inputs(self, args_ids: t.Sequence[ir.ArgumentId]):
        args_inputs = [
            {
                "__sdk_positional_args_ids": args_ids,
                "type": self.sdk_metadata_type_name,
            }
        ]
        for arg_id in args_ids:
            try:
                constant = self._constants_dict[arg_id]
                args_inputs.append(_make_constant_input(arg_id, constant))
            except KeyError:
                artifact = self._artifacts_dict[arg_id]
                producing_invocation = self._artifact_producer_dict[arg_id]
                args_inputs.append(
                    _make_artifact_input(arg_id, artifact, producing_invocation)
                )
        return args_inputs

    def _make_kwargs_inputs(
        self, kwargs_ids: t.Optional[t.Mapping[ir.ParameterName, ir.ArgumentId]]
    ):
        inputs = []
        for arg_name, arg_id in (kwargs_ids or {}).items():
            try:
                constant = self._constants_dict[arg_id]
                inputs.append(_make_constant_input(arg_name, constant))
            except KeyError:
                artifact = self._artifacts_dict[arg_id]
                producing_invocation = self._artifact_producer_dict[arg_id]
                inputs.append(
                    _make_artifact_input(arg_name, artifact, producing_invocation)
                )

        return inputs

    def _make_outputs(self, output_ids: t.Iterable[ir.ArtifactNodeId]):
        outputs = []
        for output_id in output_ids:
            artifact = self._artifacts_dict[output_id]
            outputs.append(
                yaml_model.StepOutput(
                    name=artifact.custom_name or artifact.id,
                    type=self.result_dict_type_name,
                )
            )
        return outputs


def _make_producer_dict(
    invocations: t.Iterable[ir.TaskInvocation],
) -> t.Mapping[ir.ArtifactNodeId, ir.TaskInvocation]:
    return {
        output_id: invocation
        for invocation in invocations
        for output_id in invocation.output_ids
    }


def _make_fn_file_path(import_id, script_file_path):
    return f"{import_id}/{script_file_path}"


def _resolve_dependent_invocations(
    invocation: ir.TaskInvocation,
    output_invocation_dict: t.Mapping[ir.ArtifactNodeId, ir.TaskInvocation],
) -> t.List[yaml_model.StepName]:
    input_ids = [
        *(invocation.args_ids or []),
        *(invocation.kwargs_ids or {}).values(),
    ]
    dependent_invocations = [
        output_invocation_dict[input_id]
        for input_id in input_ids
        if input_id in output_invocation_dict
    ]
    # We need to use a set because QE only supports a single instance of each dependent
    # task. Then we return the expected list.
    return list({invocation.id for invocation in dependent_invocations})


def _fn_ref_dict(fn_ref: ir.FunctionRef, source_import_name: yaml_model.ImportName):
    if isinstance(fn_ref, ir.FileFunctionRef):
        return {
            **fn_ref.dict(),
            # Orquestra v1 and python3-runtime use a convention where the script path is
            # prepended with the "import" name. Considerations:
            # - example of a full script path in QE:
            #   `/app/step/z-quantum-core/steps/optimize.py`
            # - QE's pwd is set to `/app` by the docker image
            # - QE clones every "import" repo under `<pwd>/step/<import-name>`.
            # - python3-runtime also prepends the `<pwd>/step` component but we don't
            #   need to worry about it here, because python3-runtime calls the
            #   dispatcher, and the dispatcher calls whatever we define here.
            #
            # To make it all work, we need to prepend the `step/<import id>` here.
            # Path conversion here is required to make always pure posix path string
            "file_path": str(
                pathlib.PurePosixPath("step", source_import_name, fn_ref.file_path)
            ),
        }
    else:
        return fn_ref


def _make_constant_input(
    arg_name: str, constant: ir.ConstantNode
) -> yaml_model.StepInput:

    result: responses.WorkflowResult

    if isinstance(constant, ir.ConstantNodeJSON):
        result = responses.JSONResult(value=constant.value)
    elif isinstance(constant, ir.ConstantNodePickle):
        result = responses.PickleResult(chunks=constant.chunks)
    else:  # pragma: no cover
        raise ValueError(
            "Only JSON-serializable and PICKLE-serializable workflow constants "
            f"are supported. ({type(constant.serialization_format)}) is not."
        )

    json_dict = json.loads(result.json())

    return {
        arg_name: json_dict,
        "type": RESULT_DICT_TYPE_NAME,
    }


def _make_artifact_input(
    arg_name, artifact: ir.ArtifactNode, producing_invocation: ir.TaskInvocation
) -> yaml_model.StepInput:
    invocation_id = producing_invocation.id
    artifact_name = artifact.custom_name or artifact.id
    return {
        arg_name: f"(({invocation_id}.{artifact_name}))",
        "type": RESULT_DICT_TYPE_NAME,
    }
