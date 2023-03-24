################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Workflow runtime based on the Ray Core/Ray Remotes API.
"""

from contextlib import contextmanager
import json
import typing as t
from pathlib import Path

import ray
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from .._base.abc import ArtifactValue, RuntimeInterface
from .._base import _graphs, serde
from . import _id_gen
from ._dag import _locate_user_fn, _pydatic_to_json_dict


class WorkflowRepo:
    def __init__(self):
        self._path = Path.home() / ".orquestra" / "alpha" / "wfs.json"

    def _read_state(self):
        try:
            with self._path.open() as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _write_state(self, state):
        self._path.parent.mkdir(exist_ok=True)

        with self._path.open("w") as f:
            return json.dump(state, f, indent=2)

    @contextmanager
    def _db_state(self):
        state_dict = self._read_state()
        # We expect the caller to mutate the state dict
        yield state_dict

        self._write_state(state_dict)

    def create_run(self, run_id: WorkflowRunId, wf_def: ir.WorkflowDef):
        with self._db_state() as state:
            if run_id in state.get("wf_runs", {}):
                raise ValueError()

            state.setdefault("wf_runs", {})[run_id] = {
                "run_id": run_id,
                "wf_def": _pydatic_to_json_dict(wf_def),
            }


@ray.remote
def _exec_task(fn_ref_dict, task_args):
    # Ray Remote arguments need to be JSON-serializable.
    fn_ref = ir.ModuleFunctionRef.parse_obj(fn_ref_dict)

    user_fn = _locate_user_fn(fn_ref)

    fn_result = user_fn(*task_args)

    return fn_result


class TaskRunner:
    """
    Suitable for running tasks from single workflow run.
    """

    def __init__(self, wf_def):
        self._wf_def = wf_def
        self._constant_vals: t.Mapping[ir.ConstantNodeId, t.Any] = {
            node.id: serde.deserialize_constant(node)
            for node in wf_def.constant_nodes.values()
        }
        # The proof-of-concept only works with single-output tasks for simplicity.
        self._inv_refs: t.MutableMapping[ir.TaskInvocationId, ray.ObjectRef] = {}
        self._ready_artifact_vals: t.MutableMapping[
            ir.ArtifactNodeId, ray.ObjectRef
        ] = {}

        ray.init(address="auto")

    def _prep_vals(
        self,
        node_ids: t.Sequence[ir.ArgumentId],
        wf_task_invocations: t.Sequence[ir.TaskInvocation],
    ):
        arg_vals = []
        for arg_id in node_ids:
            if arg_id in self._constant_vals:
                arg_val = self._constant_vals[arg_id]
            elif arg_id in self._ready_artifact_vals:
                arg_val = self._ready_artifact_vals[arg_id]
            else:
                producing_invocation = [
                    inv for inv in wf_task_invocations if arg_id in inv.output_ids
                ][0]

                assert (
                    len(producing_invocation.output_ids) == 1
                ), "The proof-of-concept only works with single-output invocations"

                arg_obj_ref = self._inv_refs[producing_invocation.id]
                arg_val = ray.get(arg_obj_ref)
                self._ready_artifact_vals[arg_id] = arg_val

            arg_vals.append(arg_val)
        return arg_vals

    # @ray.remote
    def run_tasks(self):
        all_invs = list(self._wf_def.task_invocations.values())
        for inv in _graphs.iter_invocations_topologically(self._wf_def):
            arg_vals = self._prep_vals(
                node_ids=inv.args_ids, wf_task_invocations=all_invs
            )

            assert (
                len(inv.kwargs_ids) == 0
            ), "The proof-of-concept only works with positional arguments"

            task_def = self._wf_def.tasks[inv.task_id]
            task_result_obj_ref = _exec_task.remote(
                _pydatic_to_json_dict(task_def.fn_ref), arg_vals
            )
            self._inv_refs[inv.id] = task_result_obj_ref

        wf_outputs = self._prep_vals(
            node_ids=self._wf_def.output_ids, wf_task_invocations=all_invs
        )

        return wf_outputs


# class LocalRayCoreRuntime(RuntimeInterface):
class LocalRayCoreRuntime:
    def __init__(self):
        self._repo = WorkflowRepo()

    def create_workflow_run(
        self,
        workflow_def: ir.WorkflowDef,
    ) -> WorkflowRunId:
        run_id = _id_gen.gen_short_uid(5)
        self._repo.create_run(run_id=run_id, wf_def=workflow_def)

        runner = TaskRunner(workflow_def)
        outputs = runner.run_tasks()
        breakpoint()

        return run_id
