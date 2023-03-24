################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Workflow runtime based on the Ray Core/Ray Remotes API.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import typing as t
from pathlib import Path

import ray
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from .._base import _graphs, serde
from .._base.cli._dorq import _dumpers
from . import _id_gen
from ._dag import _locate_user_fn, _pydatic_to_json_dict


def _make_timestamp():
    return datetime.now(timezone.utc).astimezone()


class WorkflowStateRepo:
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

    def create_run(
        self, run_id: WorkflowRunId, wf_def: ir.WorkflowDef, started_at: datetime
    ):
        with self._db_state() as state:
            if run_id in state.get("wf_runs", {}):
                raise ValueError()

            state.setdefault("wf_runs", {})[run_id] = {
                "run_id": run_id,
                "wf_def": _pydatic_to_json_dict(wf_def),
                "started_at": started_at.isoformat(),
            }

    def mark_run_as_succeeded(self, run_id: WorkflowRunId, finished_at: datetime):
        with self._db_state() as state:
            wf_run = state["wf_runs"][run_id]
            wf_run["finished_at"] = finished_at.isoformat()


@ray.remote
def _exec_task(
    fn_ref_dict, wf_run_id: WorkflowRunId, task_inv_id: ir.TaskInvocationId, task_args
):
    # Ray Remote arguments need to be JSON-serializable.
    fn_ref = ir.ModuleFunctionRef.parse_obj(fn_ref_dict)

    user_fn = _locate_user_fn(fn_ref)

    fn_result = user_fn(*task_args)

    dumper = _dumpers.TaskOutputDumper()
    base_path = Path.home() / ".orquestra" / "alpha" / "computed_values"
    dumper.dump(
        # The POC assumes single-output tasks
        value=fn_result,
        wf_run_id=wf_run_id,
        task_inv_id=task_inv_id,
        output_index=0,
        dir_path=base_path,
    )

    return fn_result


class TaskRunner:
    """
    Suitable for running tasks from single workflow run.
    """

    def __init__(self, wf_def, repo: WorkflowStateRepo):
        self._wf_def = wf_def
        self._repo = repo
        self._constant_refs: t.MutableMapping[ir.ConstantNodeId, ray.ObjectRef] = {}
        # The proof-of-concept only works with single-output tasks for simplicity.
        self._inv_refs: t.MutableMapping[ir.TaskInvocationId, ray.ObjectRef] = {}

        ray.init(address="auto")

    def _prep_vals(
        self,
        node_ids: t.Sequence[ir.ArgumentId],
        wf_task_invocations: t.Sequence[ir.TaskInvocation],
    ):
        arg_vals = []
        for arg_id in node_ids:
            if arg_id in self._constant_refs:
                obj_ref = self._constant_refs[arg_id]
                arg_val = ray.get(obj_ref)
            else:
                producing_invocation = [
                    inv for inv in wf_task_invocations if arg_id in inv.output_ids
                ][0]

                assert (
                    len(producing_invocation.output_ids) == 1
                ), "The proof-of-concept only works with single-output invocations"

                arg_obj_ref = self._inv_refs[producing_invocation.id]
                arg_val = ray.get(arg_obj_ref)
            arg_vals.append(arg_val)
        return arg_vals

    # @ray.remote
    def run_tasks(self, wf_run_id: WorkflowRunId):
        for constant_node in self._wf_def.constant_nodes.values():
            constant_val = serde.deserialize_constant(constant_node)
            ref = ray.put(constant_val)
            self._constant_refs[constant_node.id] = ref

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
                fn_ref_dict=_pydatic_to_json_dict(task_def.fn_ref),
                wf_run_id=wf_run_id,
                task_inv_id=inv.id,
                task_args=arg_vals,
            )
            self._inv_refs[inv.id] = task_result_obj_ref

        # Hack: await all workflow outputs
        _ = self._prep_vals(
            node_ids=self._wf_def.output_ids, wf_task_invocations=all_invs
        )

        finished_at = _make_timestamp()
        repo = WorkflowStateRepo()
        repo.mark_run_as_succeeded(run_id=wf_run_id, finished_at=finished_at)


# class LocalRayCoreRuntime(RuntimeInterface):
class LocalRayCoreRuntime:
    def __init__(self):
        self._repo = WorkflowStateRepo()

    def create_workflow_run(
        self,
        workflow_def: ir.WorkflowDef,
    ) -> WorkflowRunId:
        run_id = _id_gen.gen_short_uid(5)
        started_at = datetime.now(timezone.utc).astimezone()
        self._repo.create_run(run_id=run_id, wf_def=workflow_def, started_at=started_at)

        runner = TaskRunner(wf_def=workflow_def, repo=self._repo)
        runner.run_tasks(wf_run_id=run_id)

        return run_id

    # def get_workflow_run_outputs_non_blocking(self, workflow_run_id: WorkflowRunId):
    #     base_path = Path.home() / ".orquestra" / "alpha" / "computed_values"
    #     inv_dir_path = base_path / workflow_run_id / "task_results" / inv_id
