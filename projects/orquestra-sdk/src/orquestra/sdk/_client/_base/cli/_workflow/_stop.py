################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq workflow stop'."""
import typing as t

from orquestra.workflow_shared import exceptions
from orquestra.workflow_shared.schema.configs import ConfigName
from orquestra.workflow_shared.schema.workflow_run import WorkflowRunId

from .. import _arg_resolvers, _repos
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq workflow stop``.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._submit.Action``.
    """

    def __init__(
        self,
        presenter=_presenters.WrappedCorqOutputPresenter(),
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

        # text IO
        self._presenter = presenter

    def on_cmd_call(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        force: t.Optional[bool],
    ):
        try:
            self._on_cmd_call_with_exceptions(wf_run_id, config, force)
        except Exception as e:
            self._presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        force: t.Optional[bool],
    ):
        """Implementation of the command action. Doesn't catch exceptions."""
        # The order of resolving config and run ID is important. It dictates the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        resolved_id = self._wf_run_resolver.resolve_id(wf_run_id, resolved_config)

        try:
            self._wf_run_repo.stop(resolved_id, resolved_config, force)
        except (exceptions.UnauthorizedError, exceptions.WorkflowRunCanNotBeTerminated):
            # Other exception types aren't expected to be raised here.
            raise

        self._presenter.show_stopped_wf_run(resolved_id)
