################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Code for 'orq workflow view'.
"""
import typing as t

from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from .. import _arg_resolvers, _repos
from .._ui import _presenters


class Action:
    """
    Encapsulates app-related logic for handling ``orq workflow view``.
    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._workflow._view.Action``.
    """

    def __init__(
        self,
        presenter=_presenters.WrappedCorqOutputPresenter(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_resolver: t.Optional[_arg_resolvers.ConfigResolver] = None,
        wf_run_id_resolver: t.Optional[_arg_resolvers.WFRunIDResolver] = None,
    ):
        # data sources
        self._wf_run_repo = wf_run_repo

        # arg resolvers
        self._config_resolver = config_resolver or _arg_resolvers.ConfigResolver(
            wf_run_repo=wf_run_repo
        )
        self._wf_run_id_resolver = wf_run_id_resolver or _arg_resolvers.WFRunIDResolver(
            wf_run_repo=wf_run_repo
        )

        # text IO
        self._presenter = presenter

    def on_cmd_call(
        self, wf_run_id: t.Optional[WorkflowRunId], config: t.Optional[ConfigName]
    ):
        # The order of resolving config and run ID is important. It dictactes the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        resolved_id = self._wf_run_id_resolver.resolve(wf_run_id, resolved_config)

        wf_run = self._wf_run_repo.get_wf_by_run_id(
            wf_run_id=resolved_id, config_name=resolved_config
        )

        self._presenter.show_wf_run(wf_run)
