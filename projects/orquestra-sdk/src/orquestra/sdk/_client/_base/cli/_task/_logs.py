################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq workflow logs'."""
import typing as t
from pathlib import Path

from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.schema.ir import TaskInvocationId
from orquestra.workflow_shared.schema.workflow_run import WorkflowRunId

from .. import _arg_resolvers, _dumpers, _repos
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq workflow logs``."""

    def __init__(
        self,
        logs_presenter=_presenters.LogsPresenter(),
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        dumper=_dumpers.LogsDumper(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_resolver: t.Optional[_arg_resolvers.WFConfigResolver] = None,
        wf_run_resolver: t.Optional[_arg_resolvers.WFRunResolver] = None,
        task_inv_id_resolver: t.Optional[_arg_resolvers.TaskInvIDResolver] = None,
    ):
        # data sources
        self._wf_run_repo = wf_run_repo

        # arg resolvers
        self._config_resolver = config_resolver or _arg_resolvers.WFConfigResolver(
            wf_run_repo=wf_run_repo
        )
        self._wf_run_resolver = wf_run_resolver or _arg_resolvers.WFRunResolver(
            wf_run_repo=wf_run_repo
        )
        self._task_inv_id_resolver = (
            task_inv_id_resolver or _arg_resolvers.TaskInvIDResolver(wf_run_repo)
        )

        # output
        self._logs_presenter = logs_presenter
        self._error_presenter = error_presenter
        self._dumper = dumper

    def on_cmd_call(self, *args, **kwargs):
        try:
            self._on_cmd_call_with_exceptions(*args, **kwargs)
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        task_inv_id: t.Optional[TaskInvocationId],
        fn_name: t.Optional[str],
        config: t.Optional[ConfigName],
        download_dir: t.Optional[Path],
    ):
        # The order of resolving config and run ID is important. It dictates the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        resolved_wf_run_id = self._wf_run_resolver.resolve_id(
            wf_run_id, resolved_config
        )
        resolver_task_inv_id = self._task_inv_id_resolver.resolve(
            task_inv_id=task_inv_id,
            fn_name=fn_name,
            wf_run_id=resolved_wf_run_id,
            config=resolved_config,
        )

        logs = self._wf_run_repo.get_task_logs(
            wf_run_id=resolved_wf_run_id,
            task_inv_id=resolver_task_inv_id,
            config_name=resolved_config,
        )

        if download_dir is not None:
            dump_paths = self._dumper.dump(
                logs=logs,
                wf_run_id=resolved_wf_run_id,
                dir_path=download_dir,
            )
            for dump_path in dump_paths:
                self._logs_presenter.show_dumped_wf_logs(dump_path)
        else:
            self._logs_presenter.show_logs(logs)
