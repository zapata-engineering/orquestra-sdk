################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Code for 'orq workflow logs'.
"""
import typing as t
import warnings
from pathlib import Path

from orquestra.sdk._base._logs._interfaces import WorkflowLogTypeName
from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import WorkflowRunId

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
        task: t.Optional[bool],
        system: t.Optional[bool],
        env_setup: t.Optional[bool],
    ):
        try:
            self._on_cmd_call_with_exceptions(
                wf_run_id=wf_run_id,
                download_dir=download_dir,
                config=config,
                task=task,
                system=system,
                env_setup=env_setup,
            )
        except Exception as e:
            self._presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        download_dir: t.Optional[Path],
        task: t.Optional[bool],
        system: t.Optional[bool],
        env_setup: t.Optional[bool],
    ):
        # The order of resolving config and run ID is important. It dictates the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        resolved_wf_run_id = self._wf_run_resolver.resolve_id(
            wf_run_id, resolved_config
        )

        # Get the available logs
        logs = self._wf_run_repo.get_wf_logs(
            wf_run_id=resolved_wf_run_id, config_name=resolved_config
        )

        # Resolve the log type switches. This must happen after getting the logs as we
        # need to check against which logs are available.
        resolved_task_switch: bool
        resolved_system_switch: bool
        resolved_env_setup_switch: bool
        (
            resolved_task_switch,
            resolved_system_switch,
            resolved_env_setup_switch,
        ) = self._wf_run_resolver.resolve_log_switches(task, system, env_setup, logs)

        for switch, log, identifier in zip(
            [resolved_task_switch, resolved_system_switch, resolved_env_setup_switch],
            [logs.per_task, logs.system, logs.env_setup],
            [
                WorkflowLogTypeName.PER_TASK,
                WorkflowLogTypeName.SYSTEM,
                WorkflowLogTypeName.ENV_SETUP,
            ],
        ):
            if not switch:
                continue

            if len(log) < 1:
                warnings.warn(f"No {identifier} logs found.", category=UserWarning)
                continue

            if download_dir:
                dump_path = self._dumper.dump(
                    logs=log,
                    wf_run_id=resolved_wf_run_id,
                    dir_path=download_dir,
                    log_type=identifier,
                )

                self._presenter.show_dumped_wf_logs(dump_path, log_type=identifier)
            else:
                self._presenter.show_logs(log, log_type=identifier)
