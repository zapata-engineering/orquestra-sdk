# Changelog

## Unreleased

🚨 *Breaking Changes*
* Compute Engine is now the default when logging in
* InlineImport() is now always default source_import for tasks

🔥 *Features*
* Force stop workflow runs via the CLI or Python API
* `WorkflowRun.get_tasks()` supports filtering tasks by state, function name, task run ID and task invocation ID.

👩‍🔬 *Experimental*

🐛 *Bug Fixes*

💅 *Improvements*

🥷 *Internal*

📃 *Docs*
* Fix broken "Dependency Installation" docs.

## v0.50.0

🚨 *Breaking Changes*
* `WorkflowRun.get_logs()` now returns a data structure with logs split into categories.
* Local workflow database has changed format: downgrading from v0.50.0 will require user intervention.

🔥 *Features*
* Add .project property to WorkflowRun to get the info about workspace and project of running workflow
* `VersionMismatch` warnings are shown only when interacting with specific workflow runs, not while listing workflows.
* Add `--qe` flag to `orq login`, this is the default so there is no change in behavior.
* Bump Ray version to 2.4.0
* New API method `WorkflowRun.start_from_ir()` that allows to start workflow run having only IR object
* The WorkflowLogs object returned from `WorkflowRun.get_logs()` now includes Compute Engine system logs for workflow runs using CE.

🐛 *Bug Fixes*
* Secrets with workspaces now work inside workflow functions and for personal access tokens in `GithubImport`.
* `list_workspaces` and `list_projects` work inside studio with `auto` config
* Listing workflows on QE no longer fails if there was a CE workflow in the DB

💅 *Improvements*
* Add prompters to `orq wf submit` command for CE runtime if workspace and project weren't passed explicitly
* Auto-select and highlight current workspace and project when using `auto` config in CLI in studio
* `orquestra-sdk-base` CPU container image has a 20% size reduction.
* Added `State` enum to the base `orquestra.sdk` package for easier filtering task runs.
* Logs fetched from CE are now split into "task" and "env setup" categories.


📃 *Docs*
* Update resource management guide for Compute Engine
* Add section for using custom images on Compute Engine

## v0.49.1

🐛 *Bug Fixes*
* Fix CLI prompters to not throw exceptions after selecting project and workspace

## v0.49.0

🚨 *Breaking Changes*
* Removed `WorkflowDef.prepare()` and `WorkflowRun.start()` functions. Use `WorkflowDef.run()` instead

🔥 *Features*
* The API list_workflow_runs() function now accepts workspace and project arguments when used with CE configs.
* Login CLI command now accepts the name of a stored config as an alternative to the uri.
* Choice and Checklist CLI prompts present an informative error message when there are no options rather than prompting the user to select from an empty list.
* New API functions: list_workspaces() and list_projects(). Usable only on CE runtime.
* Setting workflow_id and project_id is now available using "orq wf submit" command.
* `sdk.current_run_ids()` can now be used within task code to access the workflow run ID, task invocation ID, and task run ID.
* All CLI commands that prompted for `wf_run_id` will now first prompt for workspace and project if `wf_run_id` is not provided.
* The error raised when trying to submit to Ray while Ray is not running now tells the user how to start Ray.
* `sdk.secrets.list()`, `sdk.secrets.get()`, `sdk.secrets.set()` and `sdk.secrets.delete()` now accept `workspace_id` parameter to specify secrets in particular workspace.
* `auto` config inside studio will infer workspace and project IDs from studio instance.
* Support for running tasks in Docker containers with custom images on Compute Engine.

🐛 *Bug Fixes*
* Fixed tasks that failed when explicitly state `n_outputs=1` on QE and in-process.

💅 *Improvements*
* `orq login` will perform some sanity checks before saving the token.
* If `orq up` fails, the output will now include the error message from the underlying subprocess.

🥷 *Internal*
* Fix random CI failures on socket warning

📃 *Docs*
* Update secrets guide to take workspaces into account

## v0.48.0

🚨 *Breaking Changes*
* Removed deprecated "name" parameter for `RuntimeConfig` factory methods, like `qe()` or `ray()`
* Removed deprecated `save()` method from `RuntimeConfig` class
* Removed `is_saved()` method and "name" setter from `RuntimeConfig` class
* `WorkflowRun.get_results()` returns values consistent with vanilla python. Single results are returned as-as, multiple results are returned as a tuple.

🔥 *Features*
* New API functions: `list_workspaces()` and `list_projects()`. Usable only on CE runtime.
* Setting `workflow_id` and `project_id` is now available using `orq wf submit` command.

🐛 *Bug Fixes*
* Tasks will no longer be retried on Ray and Compute Engine when the process crashes, preventing duplicated MLflow errors.

💅 *Improvements*
* In the CLI, where the user would be prompted with a choice, but only one option is available, we now prompt for confirmation instead.

🥷 *Internal*
* Switch the login URL endpoint
* Rewrite tests to avoid hangs on Windows CI


## 0.47.0

🚨 *Breaking Changes*
* Task results on QE have changed shape. This may cause some oddness when downloading older task artifacts.

🔥 *Features*
* New built-in config name - "auto" - used to submit workflows to a remote cluster when used inside Orquestra Studio.
* "auto" built-in config name becomes alias to "local" if not in a Studio environment

👩‍🔬 *Experimental*
* Setting `workflow_id` and `project_id` is now available on workflow Python API `start()` and `prepare()` functions

🐛 *Bug Fixes*
* Retry getting results from CE if the results were not ready but the workflow succeeded.
* Using secrets inside the workflow function will now work correctly on Ray
* Fix `WorkflowDef.graph` - honor kwargs of tasks and add `aggregate_output` to show outputs
* Fixed returning intermediate workflow values (e.g. with `orq task results`) when the task has multiple outputs and only some of them were used in the rest of the workflow function. The following should work now as expected:
```python
@sdk.workflow
def my_wf():
    _, b = two_output_task()
    all_outputs = two_output_task()
    out1, out2 = all_outputs
    return b, all_outputs, out1, out2
```
* Pickled workflow/task results should no longer cause workflows to fail inside the SDK machinery. Note: when passing a Python object between your tasks, you **must** ensure the Python dependencies are installed.

💅 *Improvements*
* `VersionMismatch` warning will be presented if we detect accessing a workflow def IR generated with another SDK version.

🥷 *Internal*
* `TaskOutputMetadata` model was added to the workflow def IR schema.
* Workflows from CE return a new shape for workflow results and task results.
* Workflows from Ray return a new shape for workflow and task results.
* Workflows from QE return a new shape for task results.
* Custom images will default to `None`, unless using Quantum Engine where the defaults stay the same.

📃 *Docs*
* _Resource Management_ guide should render properly now.

## v0.46.0

🚨 *Breaking Changes*
* Workflow definitions now require at least one task in order to be submitted. This check is performed during traversal, and raises a `WorkflowSyntaxError` if no tasks are required to be executed.
* Remove `TaskDef.model` and `TaskDef.import_models` interfaces
* Public API classes `sdk.GitImport`, `sdk.GithubImport`, `sdk.LocalImport`, `sdk.InlineImport` now use `dataclasses.dataclass` instead of `typing.NamedTuple`.
* Local Ray will now always pass resources to underlying ray.remote functions.

🔥 *Features*
* Sort WF runs by start date in `list wf` command. Show start date as one of the columns
* Sort WF runs by start date in all workflow commands in prompt selection. Show start date with WF id
* Set resources for workflows on CE via `resources` keyword argument in the `@workflow` decorator or with `.with_resources()` on a `WorkflowDef`.
* New parameters for `@workflow` decorator - `default_source_import` and `default_dependency_imports`.
These parameters let you set the default imports for all tasks in given workflow.
If a task defines its own imports (either source, dependencies, or both) - it will overwrite workflow defaults.
* Allow single imports as `dependency_imports` in `@task` decorators.
* Listing workflow runs from Compute Engine now allows an upper limit to the number of runs to be listed to be set via the `limit` keyword.
* Print HTTP requests and other debug information from `orq` CLI if `ORQ_VERBOSE` env flag is set.
* CE runtime now supports getting logs from remote Ray runtimes.

🐛 *Bug Fixes*
* Stopping a QE workflow after it has already stopped will no longer raise an exception.
* Fix dependency issues causing CE workflows to fail if WF constant was library-dependent object.
* Attempting to use the "in-process" runtime on the CLI will no longer raise an exception. Instead, a message telling you to use the Python API or Ray will be printed.

🥷 *Internal*
* During YAML conversion, Workflow SDK repo matched on host and path, not full URL.
* On QE, Github URLs will be converted to SSH URLs.
* Removed `corq` code.
* Old `RuntimeInterface` methods have been removed.

📃 *Docs*
* Guide: CE Resource Management

## v0.45.1

🐛 *Bug Fixes*
* Ensure `int`-like resource values are passed to Ray correctly

## v0.45.0

🚨 *Breaking Changes*
* Pickling library switched to `cloudpickle` instead of `dill`. While no breakages are expected, this change may result in objects raising an error during pickling, even if they were previously able to be pickled. Please report any instances of these as bugs.


🔥 *Features*
* Use the requested resources from a workflow's tasks when submitting to CE


🥷 *Internal*
* RayRuntime can now be configured to pass resources to underlying remote functions
* Added version metadata to the workflow IR

## v0.44.0

🚨 *Breaking Changes*
* Removed FluentBit-related CLI options: `orq {up,down} --fluentbit` flag. Logs produced by the local Ray runtime are read directly by the SDK now. This only affects users who used the experimental integration with FluentBit docker container.
* `GitImport` will no longer be downloaded automatically when using Ray locally. This reverts behavior to `v0.42.0`.
* Internal configuration environment variables have changed.


🔥 *Features*
* Secrets can now be used inside workflow functions
* `sdk.secrets.get("name")` will now use passport-based authorization if `ORQUESTRA_PASSPORT_FILE` environment variable is set. Otherwise, passing a valid `config_name="..."` is required.
* Bump Ray version to 2.3
* `GithubImport` can be used with a username and a secret referring to a "personal access token" to enable private GitHub repositories on Compute Engine. Server side support coming soon!


👩‍🔬 *Experimental*


🐛 *Bug Fixes*
* Getting full logs produced by Ray workflows. Previously, the dictionary returned by `logs_dict = wf_run.get_logs()` had just a single entry: `{"logs": ["task 1 log", "task 1 log", "task 2 log", "task 2 log"]}`. Now, the dictionary has a correct shape: `{"task_invocation_id1": ["task 1 log", "task 1 log"], "task_invocation_id2": ["task 2 log", "task 2 log"]}`.
* Getting single task logs. Previously `orq task logs` would raise an unhandled exception. Now, it prints the log lines.
* Workflow run IDs inside logs on CE now match the expected run ID.


💅 *Improvements*
* `orq wf view` now shows `TaskInvocationID`s instead of `TaskRunID`s. This improves usage of `orq wf view` with other CLI commands that require passing invocation ID, like `orq task {logs,results}`.
* `sdk.WorkflowRun.wait_until_finished()` will now print workflow status every now and then.


🥷 *Internal*
* Git URL model changed inside the IR
* `orq up` will now configure Ray's Plasma directory


*Docs*
* Guide: Dependency Installation - sources, order, and best practice


## v0.43.0

🚨 *Breaking Changes*
* Brand-new `orq` CLI with simplified command tree and interactive prompts when a required argument isn't passed. New commands:
    * `orq workflow submit`
    * `orq workflow view`
    * `orq workflow list`
    * `orq workflow stop`
    * `orq workflow logs`
    * `orq workflow results`
    * `orq wf` as a shorthand for `orq workflow`
    * `orq task logs`
    * `orq task results`
    * `orq up`
    * `orq down`
    * `orq status`
    * `orq login`
* `sdk.WorkflowRun.get_logs()` doesn't accept any arguments any more. Now, it returns all the logs produced by the tasks in the workflow. If you're interested in only a subset of your workflow's logs, please consider using one of the following filtering options:
```python
from orquestra import sdk
from orquestra.sdk.schema.workflow_run import State

wf_run = sdk.WorkflowRun.by_id("foo")

logs = wf_run.get_logs()
# Option 1
single_task_logs = logs["my_inv_id"]

# Option 2
logs_subset = {id: lines for id, lines in logs.items() if id in ["foo", "bar", "baz"]}

# Option 3
for task in wf_run.get_tasks():
    if task.get_status() == State.FAILED:
        print(task.get_logs())
```
* `sdk.WorkflowRun.get_artifacts()` doesn't accept any arguments any more. Now, it returns all the artifacts produced by the tasks in the workflow.
* `sdk.TaskRun.get_logs()` returns a list of log lines produced by this task. Previously, it returned a dictionary with one entry.
* Executing a workflow on Ray with Git imports will now install them. A known limitation is that this will only work for Git repositories that are Python packages and will fail for Git repositories that are not Python packages.
* The API will no longer accept `config_save_file` as optional parameters, from now on if you want to use a different config file use the `ORQ_CONFIG_PATH` environment variable.


🔥 *Features*

* `list_workflow_runs` added to the Public API. This lets you list the workflows for a given config, for example `sdk.list_workflow_runs("ray")` or `sdk.list_workflow_runs("prod-d")`.

🐛 *Bug Fixes*

* Fixed broken link on docs landing page.
* Internal logs from Ray are no longer displayed.
* Fixed the docstrings for `sdk.WorkflowRun.get_artifacts()`. It returns a dictionary with `TaskInvocationID` as keys and whatever the task returns as values.
* Fixed bug where some log line from Ray may be duplicated when viewing logs
* Tasks with duplicate imports will no longer fail when running on QE
* AST parser will no longer print a lot of "Info" messages
* `sdk.WorkflowRun.get_logs()` now only returns logs produced by the user. Previously, it included internal debug messages produced by Ray.
* Logs from workflows submitted to Ray are now always returned as JSONL lines


## v0.42.0

🚨 *Breaking Changes*

* `sdk.WorkflowRun.by_id()` has a new positional parameter. `sdk.WorkflowRun.by_id("wf.1", "my/project/path", "my/config/path)` becomes `sdk.WorkflowRun.by_id("wf.1", project_dir="my/project/path", config_save_file="my/config/path)`
* `in_process` runtime now executes workflows in topological order. This may be different to the order tasks were called in the workflow function.
* Configs can no longer be named. For in-process, use "in_process" name, for local ray "ray" or "local". For QE remote - config name is auto generated based on URI (for https://prod-d.orquestra.io/ name becomes "prod-d" as an example).
* Removed ray_linked runtime.


👩‍🔬 *Experimental*

* Optional `config` param in `sdk.WorkflowRun.by_id()`. Allows access to workflows submitted by other users or from another machine, if the selected runtime supports it. Per-runtime support will be added separately.
* New CLI command: `python -m orquestra.sdk._base.cli._dorq._entry workflow stop`.
* New CLI commands that require `config` and `workflow_run_id` will now prompt the user for selecting value interactively.
* New CLI commands: `python -m orquestra.sdk._base.cli._dorq._entry up|down|status` for managing local services.


## v0.41.0

🚨 *Breaking Changes*

* `ray` is now a reserved configuration name.

🔥 *Features*

* Workflow def graph visualization via `my_wf().graph` (#317). The output object is rendered natively by Jupyter. For now, this shows task invocations and artifacts. Tasks run status will be added in the future, stay tuned!
* Automatically get the workflow run and task run ID on Argo when using `wfprint()`.
* `"ray"` can be used as an alias for `"local"` when using `RuntimeConfig.load(...)` or `WorkflowRun.run()`.
* New Python API to traverse and debug tasks in workflow: `wf_run.get_tasks()` (#283)
* New Runtime Config for CE (AKA Ray Remote): `RuntimeConfig.ce()`
* Public API for serializable model of the workflow run state: `sdk.WorkflowRun.get_status_model()`. Potential use cases include workflow status visualization by external tools.

👩‍🔬 *Experimental*

* Prototype of a new CLI command tree: `python -m orquestra.sdk._base.cli._dorq._entry workflow {submit,view}`.
* New Runtime for the Compute Engine API

🐛 *Bug Fixes*

* Fix `sdk.task()` typing when constructing a task inside a workflow.
* Fix getting logs from Ray after restarting the cluster, when not using Fluent.
* `WorkflowRun.get_results()` should return a Sequence instead of an Iterable.

*Internal*

* Allow Studio/Portal to override SDK internals.
* Workflow run endpoints added to the workflow driver client
* Deserialise workflow artifacts from Workflow Driver
* Add CODEOWNERS file
* Simplify internal interfaces
* Refactor: move old CLI code into a submodule.
* Remove obsolete system deps installation to speed up tests.
* Ignore slow tests in local development.

*Docs*

* Streamline installation & quickstart tutorials (#324)
* Create tutorial for parametrized workflows.
* Cleanup and unify tutorials for local Ray execution
* Cleanup and unify tutorials for remote QE execution (#341)
* Add recipe for running VQE on quantum hardware.


## v0.40.0

🚨 *Breaking Changes*

* `WorkflowDef.run()` now requires passing config name explicitly (#308)

🔥 *Features*

* public API for secrets (#304). Supports usage from workflows on Ray, QE, and local scripts.
* `orq create-config` command for explicit creation of configs

🐛 *Bug Fixes*

* Fix Unknown error '_DockerClient' object has no attribute '_client' error when `python_on_whales` is installed (#314)
* Raise exception if `TaskDef` is used in a workflow but isn't called (#312)
* Fix handling nested calls in workflows: `fn()()` (#311)
