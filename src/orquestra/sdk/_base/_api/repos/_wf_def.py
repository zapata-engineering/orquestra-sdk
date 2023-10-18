from ....exceptions import UnauthorizedError, WorkflowRunNotFoundError
from ....schema.ir import WorkflowDef
from ....schema.workflow_run import WorkflowRunId
from ...abc import RuntimeInterface


class WFDefRepo:
    def get_wf_def(
        self,
        wf_run_id: WorkflowRunId,
        runtime: RuntimeInterface,
    ) -> WorkflowDef:
        try:
            wf_run_model = runtime.get_workflow_run_status(wf_run_id)
        except (UnauthorizedError, WorkflowRunNotFoundError):
            raise
        return wf_run_model.workflow_def
