################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for 'orq wf submit' glue code.
"""
import warnings
from unittest.mock import Mock, create_autospec

import pytest

from orquestra.sdk import exceptions
from orquestra.sdk._base.cli._dorq import _repos
from orquestra.sdk._base.cli._dorq._ui import _presenters, _prompts
from orquestra.sdk._base.cli._dorq._workflow import _submit


def _assert_called_with_type(mock: Mock, *args_types, **kwargs_types):
    """
    Looks into ``mock``'s call history and verifies that each arg and kwarg was of the
    appropriate type.
    """
    for args_call in mock.call_args_list:
        for call_value, expected_type in zip(args_call.args, args_types):
            assert isinstance(call_value, expected_type)

        for kwarg_key in kwargs_types.keys():
            assert isinstance(args_call.kwargs[kwarg_key], kwargs_types[kwarg_key])


class TestAction:
    """
    Test boundary::

        [_submit.Action]->[Prompter]
                        ->[Presenter]
                        ->[WfRunRepo]
                        ->[WfDefRepo]
    """

    class TestPassingAllValues:
        @staticmethod
        @pytest.mark.parametrize("force", [False, True])
        def test_success(force: bool):
            # Given
            module = "my_wfs"
            name = "sample_wf"
            config = "cluster_z"
            workspace = "ws"
            project = "project'"

            prompter = create_autospec(_prompts.Prompter)
            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

            wf_run_id = "wf.test"
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            # submit() doesn't raise = no effect of the "--force" flag
            wf_run_repo.submit.return_value = wf_run_id

            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_module_from_spec.return_value = module
            wf_def_sentinel = "<wf def sentinel>"
            wf_def_repo.get_workflow_def.return_value = wf_def_sentinel

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(module, name, config, workspace, project, force)

            # Then
            # We don't expect prompts.
            prompter.choice.assert_not_called()

            # We expect getting workflow def from the module.
            wf_def_repo.get_workflow_def.assert_called_with(module, name)

            # We expect submitting the retrieved wf def to the passed config cluster.
            wf_run_repo.submit.assert_called_with(
                wf_def_sentinel,
                config,
                ignore_dirty_repo=force,
                workspace_id=workspace,
                project_id=project,
            )

            # We expect telling the user the wf run ID.
            presenter.show_submitted_wf_run.assert_called_with(wf_run_id)

    class TestOmittingName:
        @staticmethod
        @pytest.mark.parametrize("force", [False, True])
        def test_multiple_wf_defs_in_module(force: bool):
            # Given
            module = "my_wfs"
            name = None
            config = "cluster_z"
            workspace = "workspace"
            project = "project"

            wf_names = ["my_wf1", "my_wf2"]
            selected_name = wf_names[1]

            prompter = create_autospec(_prompts.Prompter)
            prompter.choice.return_value = selected_name

            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

            wf_run_id = "wf.test"
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            # submit() doesn't raise = no effect of the "--force" flag
            wf_run_repo.submit.return_value = wf_run_id

            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_module_from_spec.return_value = module
            wf_def_repo.get_worklow_names.return_value = wf_names

            wf_def_sentinel = "<wf def sentinel>"
            wf_def_repo.get_workflow_def.return_value = wf_def_sentinel

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(module, name, config, workspace, project, force)

            # Then
            # We expect prompting for selection of the workflow name.
            prompter.choice.assert_called_with(wf_names, message="Workflow definition")

            # We expect getting workflow def from the module, with the name selected
            # by the prompt.
            wf_def_repo.get_workflow_def.assert_called_with(module, selected_name)

            # We expect submitting the retrieved wf def to the passed config cluster.
            wf_run_repo.submit.assert_called_with(
                wf_def_sentinel,
                config,
                ignore_dirty_repo=force,
                workspace_id=workspace,
                project_id=project,
            )

            # We expect telling the user the wf run ID.
            presenter.show_submitted_wf_run.assert_called_with(wf_run_id)

        @staticmethod
        @pytest.mark.parametrize("force", [False, True])
        def test_single_wf_def(force: bool):
            # Given
            module = "my_wfs"
            name = None
            config = "cluster_z"
            workspace = "workspace"
            project = "project"

            wf_names = ["my_wf1"]

            prompter = create_autospec(_prompts.Prompter)

            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

            wf_run_id = "wf.test"
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            # submit() doesn't raise = no effect of the "--force" flag
            wf_run_repo.submit.return_value = wf_run_id

            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_module_from_spec.return_value = module
            wf_def_repo.get_worklow_names.return_value = wf_names

            wf_def_sentinel = "<wf def sentinel>"
            wf_def_repo.get_workflow_def.return_value = wf_def_sentinel

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(module, name, config, workspace, project, force)

            # Then
            # We don't expect prompts.
            prompter.choice.assert_not_called()

            # We expect getting workflow def from the module, with the only
            # available name.
            wf_def_repo.get_workflow_def.assert_called_with(module, wf_names[0])

            # We expect submitting the retrieved wf def to the passed config cluster.
            wf_run_repo.submit.assert_called_with(
                wf_def_sentinel,
                config,
                ignore_dirty_repo=force,
                workspace_id=workspace,
                project_id=project,
            )

            # We expect telling the user the wf run ID.
            presenter.show_submitted_wf_run.assert_called_with(wf_run_id)

        @staticmethod
        @pytest.mark.parametrize("force", [False, True])
        def test_no_wf_defs_in_module(force: bool):
            # Given
            module = "my_wfs"
            name = None
            config = "cluster_z"
            workspace = "workspace"
            project = "project"

            prompter = create_autospec(_prompts.Prompter)
            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)
            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_worklow_names.side_effect = (
                exceptions.NoWorkflowDefinitionsFound(module_name=module)
            )

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(module, name, config, workspace, project, force)

            # Then
            # We don't expect prompts.
            prompter.choice.assert_not_called()

            # We don't expect getting any workflow.
            wf_def_repo.get_workflow_def.assert_not_called()

            # We don't expect submits.
            wf_run_repo.submit.assert_not_called()

            # We expect presenting the error.
            _assert_called_with_type(
                presenter.show_error, exceptions.NoWorkflowDefinitionsFound
            )

        @staticmethod
        @pytest.mark.parametrize("force", [False, True])
        def test_invalid_module(force: bool):
            module = "doesnt_exist"
            name = None
            config = "cluster_z"
            workspace = "workspace"
            project = "project"

            prompter = create_autospec(_prompts.Prompter)
            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)

            sys_path = [module, "foo", "bar"]
            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_module_from_spec.side_effect = (
                exceptions.WorkflowDefinitionModuleNotFound(
                    module_name=module, sys_path=sys_path
                )
            )

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # When
            action.on_cmd_call(module, name, config, workspace, project, force)

            # Then
            # We don't expect prompts.
            prompter.choice.assert_not_called()

            # We don't expect loading defs.
            wf_def_repo.get_workflow_def.assert_not_called()

            # We don't expect submits.
            wf_run_repo.submit.assert_not_called()

            # We expect telling the user about the error.
            _assert_called_with_type(
                presenter.show_error, exceptions.WorkflowDefinitionModuleNotFound
            )

    class TestDirtyRepo:
        """
        Test scenarios when a user submits the workflow and the repo has uncommitted
        changes.
        """

        @staticmethod
        def test_no_force():
            # Given
            module = "my_wfs"
            name = "sample_wf"
            config = "cluster_z"
            workspace = "workspace"
            project = "project"

            force = False

            prompter = create_autospec(_prompts.Prompter)
            # Simulate a user saying "yes"
            prompter.confirm.return_value = True

            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

            wf_run_id = "wf.test"
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)

            def _fake_submit_method(*args, ignore_dirty_repo, **kwargs):
                if ignore_dirty_repo:
                    warnings.warn(
                        "You have uncommitted changes", exceptions.DirtyGitRepo
                    )
                    return wf_run_id
                else:
                    raise exceptions.DirtyGitRepo()

            wf_run_repo.submit.side_effect = _fake_submit_method

            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_module_from_spec.return_value = module
            wf_def_sentinel = "<wf def sentinel>"
            wf_def_repo.get_workflow_def.return_value = wf_def_sentinel

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # Then
            # We expect a warning being presented.
            with pytest.warns(exceptions.DirtyGitRepo):
                # When
                action.on_cmd_call(module, name, config, workspace, project, force)

            # We expect getting workflow def from the module.
            wf_def_repo.get_workflow_def.assert_called_with(module, name)

            # We expect a confirmation prompt.
            prompter.confirm.assert_called()

            # We expect telling the user the wf run ID.
            presenter.show_submitted_wf_run.assert_called_with(wf_run_id)

        @staticmethod
        def test_force():
            # Given
            module = "my_wfs"
            name = "sample_wf"
            config = "cluster_z"
            workspace = "workspace"
            project = "project"
            force = True

            prompter = create_autospec(_prompts.Prompter)
            presenter = create_autospec(_presenters.WrappedCorqOutputPresenter)

            wf_run_id = "wf.test"
            wf_run_repo = create_autospec(_repos.WorkflowRunRepo)

            def _fake_submit_method(*args, ignore_dirty_repo, **kwargs):
                if ignore_dirty_repo:
                    warnings.warn(
                        "You have uncommitted changes", exceptions.DirtyGitRepo
                    )
                    return wf_run_id
                else:
                    raise exceptions.DirtyGitRepo()

            wf_run_repo.submit.side_effect = _fake_submit_method

            wf_def_repo = create_autospec(_repos.WorkflowDefRepo)
            wf_def_repo.get_module_from_spec.return_value = module
            wf_def_sentinel = "<wf def sentinel>"
            wf_def_repo.get_workflow_def.return_value = wf_def_sentinel

            action = _submit.Action(
                prompter=prompter,
                presenter=presenter,
                wf_run_repo=wf_run_repo,
                wf_def_repo=wf_def_repo,
            )

            # Then
            # We expect a warning being presented.
            with pytest.warns(exceptions.DirtyGitRepo):
                # When
                action.on_cmd_call(module, name, config, workspace, project, force)

            # We expect getting workflow def from the module.
            wf_def_repo.get_workflow_def.assert_called_with(module, name)

            # We don't expect any confirmation prompts.
            prompter.confirm.assert_not_called()

            # We expect telling the user the wf run ID.
            presenter.show_submitted_wf_run.assert_called_with(wf_run_id)
