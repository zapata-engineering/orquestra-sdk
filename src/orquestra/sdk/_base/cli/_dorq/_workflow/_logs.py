################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Code for 'orq workflow logs'.
"""
import typing as t
from pathlib import Path

from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import TaskInvocationId, WorkflowRunId

from .. import _arg_resolvers, _dumpers, _repos
from .._ui import _presenters


class Action:
    """
    Encapsulates app-related logic for handling ``orq workflow logs``.
    """

    def __init__(
        self,
        presenter=_presenters.WrappedCorqOutputPresenter(),
        dumper=_dumpers.LogsDumper(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_resolver: t.Optional[_arg_resolvers.WFConfigResolver] = None,
        wf_run_resolver: t.Optional[_arg_resolvers.WFRunResolver] = None,
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

        # output
        self._presenter = presenter
        self._dumper = dumper

    def on_cmd_call(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        download_dir: t.Optional[Path],
    ):
        try:
            self._on_cmd_call_with_exceptions(
                wf_run_id=wf_run_id,
                download_dir=download_dir,
                config=config,
            )
        except Exception as e:
            self._presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        download_dir: t.Optional[Path],
    ):
        # The order of resolving config and run ID is important. It dictates the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        resolved_wf_run_id = self._wf_run_resolver.resolve_id(
            wf_run_id, resolved_config
        )
        logs = self._wf_run_repo.get_wf_logs(
            wf_run_id=resolved_wf_run_id, config_name=resolved_config
        )

        if download_dir is not None:
            dump_path = self._dumper.dump(
                logs=logs,
                wf_run_id=resolved_wf_run_id,
                dir_path=download_dir,
            )
            self._presenter.show_dumped_wf_logs(dump_path)
        else:
            self._presenter.show_logs(logs)
