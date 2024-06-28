################################################################################
# © Copyright 2022-2024 Zapata Computing Inc.
################################################################################
""""dorq" CLI entrypoint.

``click`` uses function name as the group and command name.
"""
import typing as t
from pathlib import Path

import click
import cloup
from cloup.constraints import constraint, mutually_exclusive
from orquestra.workflow_shared.schema.configs import RuntimeName

from . import _cli_logs
from ._ui._click_default_group import DefaultGroup

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
Name of the config used to submit workflow. Use 'in_process' for running workflow
as local python process, 'ray' to run workflow in local ray cluster.
To get config name for remote runtime, use orq login -s <uri> first
""",
)

WORKSPACE_OPTION = cloup.option(
    "-w",
    "--workspace-id",
    required=False,
    help="""
ID of the workspace used to submit workflow. Used only on CE runtime
""",
)

PROJECT_OPTION = cloup.option(
    "-p",
    "--project-id",
    required=False,
    help="""
ID of the project used to submit workflow. Used only on CE runtime
""",
)


@cloup.group(context_settings=CLICK_CTX_SETTINGS)
@click.version_option(
    None,  # version number not set, click will infer it using importlib.metadata
    "-V",
    "--version",
    prog_name="Orquestra Workflow SDK",  # displayed to the user
    package_name="orquestra-sdk",
)
def dorq():
    # Normally, click would infer command name from function name. This is different,
    # because it's the top-level group. User-facing name depends on the entrypoint spec
    # in setup.cfg.
    #
    # This function's body is executed before any other, more specific command is run.

    _cli_logs.configure_verboseness_if_needed()


# ----------- 'orq workflow' commands ----------


@dorq.group(aliases=["wf"])
def workflow():
    """Commands related to workflow runs."""
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
Name of the workflow function to load from 'module'. If omitted, 'orq' will ask for
selecting a function from the ones available in 'module'.
""",
)
@CONFIG_OPTION
@WORKSPACE_OPTION
@PROJECT_OPTION
@cloup.option(
    "--force",
    default=None,
    help=(
        "If passed, submits the workflow without confirmation even if there are "
        "uncommitted changes."
    ),
)
def submit(
    module: str,
    name: t.Optional[str],
    config: t.Optional[str],
    workspace_id: t.Optional[str],
    project_id: t.Optional[str],
    force: bool,
):
    """Submits a workflow for execution.

    Loads workflow definition from a Python module,
    creates a new workflow run, and returns the workflow run ID for use in other
    commands.

    If there's a task defined in a git repo with uncommitted changes you are asked for
    confirmation before submitting the workflow.
    """
    from ._workflow._submit import Action

    action = Action()
    action.on_cmd_call(module, name, config, workspace_id, project_id, force)


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
def view(wf_run_id: t.Optional[str], config: t.Optional[str]):
    """Prints details of a single workflow run that was already submitted."""
    from ._workflow._view import Action

    action = Action()
    action.on_cmd_call(wf_run_id, config)


@workflow.command()
@CONFIG_OPTION
@cloup.option(
    "--id",
    type=str,
    help="The ID of a previously submitted workflow. Replaces the WORKFLOW argument.",
)
@cloup.option(
    "-m",
    "--module",
    type=str,
    help="""
Location of the module where the workflow is defined. Replaces the WORKFLOW argument.
    """,
)
@cloup.option(
    "-n",
    "--name",
    required=False,
    help="""
Name of the workflow function to load from 'module'. If omitted, 'orq' will ask for
selecting a function from the ones available in 'module'.
""",
)
@cloup.option(
    "-f",
    "--file",
    help=(
        "Path to store the generated graph. If passed, the command will create a pdf "
        "file with this name and location."
    ),
    type=click.Path(file_okay=True, dir_okay=False, writable=True, path_type=Path),
)
@constraint(mutually_exclusive, ["config", "module"])
@constraint(mutually_exclusive, ["config", "name"])
@constraint(mutually_exclusive, ["id", "module"])
@constraint(mutually_exclusive, ["id", "name"])
def graph(
    config: t.Optional[str],
    id: t.Optional[str],
    module: t.Optional[str],
    name: t.Optional[str],
    file: t.Optional[Path],
):
    """Generate a graph of a single workflow."""
    from ._workflow._graph import Action

    action = Action()
    action.on_cmd_call(
        config=config,
        wf_run_id=id,
        module=module,
        name=name,
        file=file,
    )


@workflow.command(name="results", aliases=["outputs"])
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
def wf_results(
    wf_run_id: t.Optional[str],
    config: t.Optional[str],
    download_dir: t.Optional[Path],
):
    """Shows a preview of a workflow's output values.

    The values shown correspond to the variables returned from the ``@workflow``
    function.

    Only works with succeeded workflows.
    If a workflow is still running this command won't wait for the workflow's
    completion.

    This command tries to print a human-friendly values preview, but the output isn't
    guaranteed to be a valid parseable value.
    If you need the artifact value for further processing, use the ``download-dir``
    option or use ``orquestra.sdk.WorkflowRun.get_results()`` directly from Python.
    """
    from ._workflow._results import Action

    action = Action()
    action.on_cmd_call(
        wf_run_id=wf_run_id,
        config=config,
        download_dir=download_dir,
    )


@workflow.command(name="logs")
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
@cloup.option(
    "--task/--no-task", is_flag=True, default=None, help="Show per-task logs."
)
@cloup.option(
    "--system/--no-system", is_flag=True, default=None, help="Show system-level logs."
)
@cloup.option(
    "--env-setup/--no-env-setup",
    is_flag=True,
    default=None,
    help="Show env-setup logs.",
)
@cloup.option(
    "--other/--no-other",
    is_flag=True,
    default=None,
    help="Show logs not included in the per-task, system, or env-setup logs.",
)
def wf_logs(
    wf_run_id: t.Optional[str],
    config: t.Optional[str],
    download_dir: t.Optional[Path],
    task: t.Optional[bool],
    system: t.Optional[bool],
    env_setup: t.Optional[bool],
    other: t.Optional[bool],
):
    """Shows logs gathered during execution of a workflow produced by all tasks."""
    from ._workflow._logs import Action

    action = Action()
    action.on_cmd_call(
        wf_run_id=wf_run_id,
        config=config,
        download_dir=download_dir,
        task=task,
        system=system,
        env_setup=env_setup,
        other=other,
    )


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@CONFIG_OPTION
@cloup.option(
    "--force/--no-force",
    is_flag=True,
    default=None,
    help="""
Will forcefully terminate a workflow, without waiting for it to gracefully exit.
If neither `--force` nor `--no-force` is passed, the runtime will determine if the
workflow should be forcefully stopped.
    """,
)
def stop(wf_run_id: t.Optional[str], config: t.Optional[str], force: t.Optional[bool]):
    """Stops a running workflow."""
    from ._workflow._stop import Action

    action = Action()
    action.on_cmd_call(wf_run_id, config, force)


@workflow.command()
@CONFIG_OPTION
@cloup.option(
    "-i",
    "--interactive",
    is_flag=True,
    flag_value=True,
    help="Specify filters in an interactive terminal session.",
)
@cloup.option(
    "-l", "--limit", type=int, help="Maximum number of runs to display for each config."
)
@cloup.option("-t", "--max-age", help="Maximum age of runs to display.")
@cloup.option(
    "-s",
    "--state",
    multiple=True,
    help="State of workflow runs to display. May be specified multiple times.",
)
@WORKSPACE_OPTION
def list(
    config: t.Optional[str],
    interactive: t.Optional[bool] = False,
    limit: t.Optional[int] = None,
    max_age: t.Optional[str] = None,
    state: t.Optional[t.List[str]] = None,
    workspace_id: t.Optional[str] = None,
):
    """Lists the available workflows."""
    from ._workflow._list import Action

    action = Action()
    action.on_cmd_call(config, limit, max_age, state, workspace_id, interactive)


# ----------- 'orq task' commands ----------


@dorq.group()
def task():
    """Commands related to task runs."""
    pass


WF_RUN_ID_OPTION = cloup.option(
    "--wf-run-id", help="Workflow run ID. Get it from 'orq wf submit'"
)
FN_NAME_OPTION = cloup.option(
    "--fn-name",
    help=(
        "Task function name. Narrows down the search. "
        "Ignored when --task-inv-id is passed."
    ),
)
TASK_INV_ID_OPTION = cloup.option(
    "--task-inv-id",
    help="Task invocation ID. Locates single use of the task in the workflow.",
)


@task.command(name="results", aliases=["outputs"])
@WF_RUN_ID_OPTION
@FN_NAME_OPTION
@TASK_INV_ID_OPTION
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
def task_results(*args, **kwargs):
    """Shows a preview of a task's output values.

    The values shown correspond to the variables returned from the ``@task`` function.

    This command tries to print a human-friendly values preview, but the output isn't
    guaranteed to be a valid parseable value.
    If you need the artifact value for further processing, use the ``download-dir``
    option or use ``orquestra.sdk.WorkflowRun.get_tasks()`` and ``task.get_results()``
    directly from Python.
    """
    from ._task._results import Action

    action = Action()
    action.on_cmd_call(*args, **kwargs)


@task.command(name="logs")
@WF_RUN_ID_OPTION
@FN_NAME_OPTION
@TASK_INV_ID_OPTION
@CONFIG_OPTION
@DOWNLOAD_DIR_OPTION
def task_logs(*args, **kwargs):
    """Shows logs gathered during execution of a single task run."""
    from ._task._logs import Action

    action = Action()

    action.on_cmd_call(*args, **kwargs)


# ----------- top-level 'orq' commands ----------


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Start a Ray cluster"),
    cloup.option("--all", is_flag=True, default=None, help="Start all known services"),
)
def up(ray: t.Optional[bool], all: t.Optional[bool]):
    """Starts managed services required to execute workflows locally.

    By default, this command only starts a Ray cluster.
    """
    from ._services._up import Action

    action = Action()
    action.on_cmd_call(manage_ray=ray, manage_all=all)


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Stop a Ray cluster"),
    cloup.option("--all", is_flag=True, default=None, help="Stop all known services"),
)
def down(ray: t.Optional[bool], all: t.Optional[bool]):
    """Stops managed services required to execute workflows locally.

    By default, this command only stops the Ray cluster.
    """
    from ._services._down import Action

    action = Action()
    action.on_cmd_call(manage_ray=ray, manage_all=all)


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Stop a Ray cluster"),
    cloup.option("--all", is_flag=True, default=None, help="Stop all known services"),
)
def restart(ray: t.Optional[bool], all: t.Optional[bool]):
    """Stops and then restarts managed services required to execute workflows locally.

    By default, this command only restarts the ray cluster.
    """
    from ._services._down import Action as DownAction
    from ._services._up import Action as UpAction

    down_action = DownAction()
    up_action = UpAction()

    down_action.on_cmd_call(manage_ray=ray, manage_all=all)
    up_action.on_cmd_call(manage_ray=ray, manage_all=all)


@cloup.command()
def status():
    """Prints the status of known services.

    Currently, this will print the status of the managed Ray cluster.
    """
    from ._services._status import Action

    action = Action()
    action.on_cmd_call()


dorq.section(
    "Service Management",
    up,
    down,
    status,
    restart,
)


# Ignoring pyright errors as this class inherits typing issues form 3rd party code
# region: login
class GroupWithDefaultCommand(  # pyright: ignore[reportIncompatibleMethodOverride, reportIncompatibleVariableOverride] # noqa: E501
    cloup.Group, DefaultGroup
):
    ...

    def get_help(self, ctx) -> str:
        # Hack to get the help for `orq login` to make sense
        sub = super().get_help(ctx).split("\n")
        return auth.get_help(ctx) + "\n\n" + "\n".join(sub[sub.index("Commands:") :])


@dorq.group(cls=GroupWithDefaultCommand, default="auth", invoke_without_command=False)
def login():
    """Commands related to logging in to clusters."""
    pass


server_config_group = cloup.OptionGroup(
    "Server configuration", constraint=cloup.constraints.RequireExactly(1)
)


@login.command(hidden=True)
@server_config_group.option(
    "-c", "--config", required=False, help="The name of an existing configuration."
)
@server_config_group.option(
    "-s", "--server", required=False, help="The server URI that you want to log into."
)
@cloup.option(
    "-t",
    "--token",
    required=False,
    help="User Token to given server. To generate token, use this command without the "
    "-t option first.",
)
@cloup.option_group(
    "Remote Environment",
    cloup.option(
        "--ce", is_flag=True, default=False, help="Log in to Compute Engine. (Default)"
    ),
    constraint=cloup.constraints.mutually_exclusive,
)
def auth(config: str, server: str, token: t.Optional[str], ce: bool):
    """Log in to remote cluster."""
    from ._login._login import Action

    action = Action()
    action.on_cmd_call(
        config=config, url=server, token=token, runtime_name=RuntimeName.CE_REMOTE
    )


@login.command(
    help="List the stored logins.",
    aliases=["-l", "list"],
)
def __list():
    from ._config._list import Action

    action = Action()
    action.on_cmd_call()


# endregion


def main():
    dorq()


if __name__ == "__main__":
    main()
