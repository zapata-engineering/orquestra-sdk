################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import logging
import typing as t

from orquestra import sdk


@sdk.task(source_import=sdk.InlineImport())
def sum_inline(a, b):
    """
    Example of a task that's inlined in the workflow IR.
    """
    return a + b


def get_pat_secret(config: str, workspace_id: t.Optional[str]):
    return sdk.Secret(
        name="ev-wfs-repo-cloner",
        config_name=config,
        workspace_id=workspace_id,
    )


def make_sum_git_task(
    secret_config: t.Optional[str],
    secret_workspace: t.Optional[str],
    github_username: t.Optional[str],
):
    """
    Prepares a task suitable for running on a remote runtime.
    The git import requires a username and PAT, these may be different
    depending on who submits the workflow.
    """

    # `orquestra-workflow` is a private repo we had in the past. It doesn't
    # contain any code anymore. It's added as a dependency import here only
    # to ensure we can clone private repos on the remote runtimes.

    if secret_config is None:
        # When testing local runtimes (ray, in_process), we don't need to
        # pass details for secrets. These runtimes plainly ignore
        # GithubImports.
        pat = None
    else:
        pat = get_pat_secret(config=secret_config, workspace_id=secret_workspace)
        assert github_username is not None, (
            "Looks like you're testing a remote runtime. "
            "This test will make use of GithubImports. Please specify GH username!"
        )

    @sdk.task(
        source_import=sdk.InlineImport(),
        dependency_imports=[
            sdk.GithubImport(
                repo="zapatacomputing/orquestra-workflow",
                username=github_username,
                personal_access_token=pat,
            )
        ],
    )
    def sum_git(a, b):
        """
        Example of a task with a private git repo dependency.
        """
        return a + b

    return sum_git


@sdk.task
def sum_with_logs(a, b):
    """
    Example of a task with logging.
    """
    logger = logging.getLogger(__name__)

    print(f"Adding two numbers: {a}, {b}")

    if a == b:
        logger.warning("Added numbers are the same! %s", a)

    return a + b
