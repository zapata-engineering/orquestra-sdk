################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
"dorq" CLI entrypoint.

``click`` uses function name as the group and command name.
"""
import typing as t
from pathlib import Path

import click
import cloup

# Adds '-h' alias for '--help'
CLICK_CTX_SETTINGS = {"help_option_names": ["-h", "--help"]}

DOWNLOAD_DIR_OPTION = cloup.option(
    "--download-dir",
    help=(
        "Directory path to store the artifact value. If passed, the command will "
        "create a file under this location."
    ),
    type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path),
)

CONFIG_OPTION = cloup.option(
    "-c",
    "--config",
    required=False,
    help="""
Name of the config used to submit workflow. Use 'in-process' for running workflow
as local python process, 'ray' to run workflow in local ray cluster.
To get config name for remote runtime, use orq login -s <uri> first
""",
)


@cloup.group(context_settings=CLICK_CTX_SETTINGS)
def dorq():
    # Normally, click would infer command name from function name. This is different,
    # because it's the top-level group. User-facing name depends on the entrypoint spec
    # in setup.cfg.
    pass


# ----------- 'orq workflow' commands ----------


@dorq.group(aliases=["wf"])
def workflow():
    """
    Commands related to workflow runs.
    """
    pass


@workflow.command()
@cloup.argument(
    "module",
    required=True,
    help="""
Location of the module where the workflow is defined. Can be a dotted
name like 'package.module' or a filepath like 'my_proj/workflows.py'. If
it's a filepath, the project layout is assumed to be "flat-layout" or
"src-layout" as defined by Setuptools:
https://setuptools.pypa.io/en/latest/userguide/package_discovery.html#automatic-discovery.
    """,
)
@cloup.argument(
    "name",
    required=False,
    help="""
Name of the workflow function to load from 'module'. If ommitted, 'orq' will ask for
selecting a function from the ones available in 'module'.
""",
)
@CONFIG_OPTION
@cloup.option(
    "--force",
    is_flag=True,
    default=False,
    help=(
        "If passed, submits the workflow without confirmation even if there are "
        "uncommitted changes."
    ),
)
def submit(module: str, name: t.Optional[str], config: t.Optional[str], force: bool):
    """
    Submits a workflow for execution.

    Loads workflow definition from a Python module,
    creates a new workflow run, and returns the workflow run ID for use in other
    commands.

    If there's a task defined in a git repo with uncommitted changes you are asked for
    confirmation before submitting the workflow.
    """
    from ._workflow._submit import Action

    action = Action()
    action.on_cmd_call(module, name, config, force)


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
def view(wf_run_id: t.Optional[str], config: t.Optional[str]):
    """
    Prints details of a single workflow run that was already submitted.
    """

    from ._workflow._view import Action

    action = Action()
    action.on_cmd_call(wf_run_id, config)


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
def results(
    wf_run_id: t.Optional[str],
    config: t.Optional[str],
    download_dir: t.Optional[Path],
):
    """
    Shows preview of a workflow output values corresponding to the variables
    returned from the ``@workflow`` function.

    Only works with succeeded workflows. If a workflow is still running this command
    won't wait for the workflow's completion.

    This command tries to print a human-friendly values preview, but the output isn't
    guaranteed to be a valid parseable value. If you need the artifact value for
    further processing, use the ``download_dir`` option or use
    ``orquestra.sdk.WorkflowRun.get_results()`` directly from Python.
    """

    from ._workflow._results import Action

    action = Action()
    action.on_cmd_call(
        wf_run_id=wf_run_id,
        config=config,
        download_dir=download_dir,
    )


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
def logs(
    wf_run_id: t.Optional[str],
    config: t.Optional[str],
    download_dir: t.Optional[Path],
):
    """
    Shows logs gathered during execution of a workflow produced by all tasks.
    """

    from ._workflow._logs import Action

    action = Action()
    action.on_cmd_call(
        wf_run_id=wf_run_id,
        config=config,
        download_dir=download_dir,
    )


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
def stop(wf_run_id: t.Optional[str], config: t.Optional[str]):
    """
    Stops a running workflow.
    """

    from ._workflow._stop import Action

    action = Action()
    action.on_cmd_call(wf_run_id, config)


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Start a Ray cluster"),
    cloup.option(
        "--fluentbit", is_flag=True, default=None, help="Start a Fluentbit container"
    ),
    cloup.option("--all", is_flag=True, default=None, help="Start all known services"),
)
def up(ray: t.Optional[bool], fluentbit: t.Optional[bool], all: t.Optional[bool]):
    """
    Starts managed services required to execute workflows locally.

    By default, this command only starts a Ray cluster.
    """
    from ._services._up import Action

    action = Action()
    action.on_cmd_call(ray, fluentbit, all)


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Start a Ray cluster"),
    cloup.option(
        "--fluentbit", is_flag=True, default=None, help="Start a Fluentbit container"
    ),
    cloup.option("--all", is_flag=True, default=None, help="Start all known services"),
)
def down(ray: t.Optional[bool], fluentbit: t.Optional[bool], all: t.Optional[bool]):
    """
    Stops managed services required to execute workflows locally.

    By default, this command only stops the Ray cluster.
    """
    from ._services._down import Action

    action = Action()
    action.on_cmd_call(ray, fluentbit, all)


@cloup.command()
def status():
    """
    Prints the status of known services.

    Currently, this will print the status of the managed Ray cluster and managed Fluent
    Bit container.
    """
    from ._services._status import Action

    action = Action()
    action.on_cmd_call()


dorq.section(
    "Service Management",
    up,
    down,
    status,
)


@dorq.group()
def task():
    """
    Commands related to workflow runs.
    """
    pass


@task.command()  # type: ignore[no-redef]
@cloup.argument("wf_run_id", required=False)
@cloup.argument("task_inv_id", required=False)
@cloup.argument("fn_name", required=False)
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
def logs(  # noqa: F811
    wf_run_id: t.Optional[str],
    task_inv_id,
    fn_name,
    config: t.Optional[str],
    download_dir: t.Optional[Path],
):
    """
    Shows logs gathered during execution of a workflow produced by all tasks.
    """

    from ._task._logs import Action

    action = Action()
    action.on_cmd_call(
        wf_run_id=wf_run_id,
        task_invocation_id=task_inv_id,
        fn_name=fn_name,
        config=config,
        download_dir=download_dir,
    )


@dorq.command()
@cloup.option(
    "-s", "--server", required=True, help="server URI that you want to log into"
)
@cloup.option(
    "-t",
    "--token",
    required=False,
    help="User Token to given server. To generate token, use this command without -t"
    "option first",
)
@cloup.option("--ce", is_flag=True, default=False, help="Start a Ray cluster")
def login(server: str, token: t.Optional[str], ce: bool):
    """
    Login in to remote cluster
    """
    from ._login._login import Action

    action = Action()
    action.on_cmd_call(server, token, ce)


def main():
    dorq()


if __name__ == "__main__":
    main()
