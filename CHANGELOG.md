# Changelog

## Unreleased

🚨 *Breaking Changes*

* `sdk.WorkflowRun.by_id()` has a new positional parameter. `sdk.WorkflowRun.by_id("wf.1", "my/project/path", "my/config/path)` becomes `sdk.WorkflowRun.by_id("wf.1", project_dir="my/project/path", config_save_file="my/config/path)`
* `in_process` runtime now executes workflows in topological order. This may be different to the order tasks were called in the workflow function.
* Configs can no longer be named. For in-process, use "in_process" name, for local ray "ray" or "local". For QE remote - config name is auto generated based on URI (for https://prod-d.orquestra.io/ name becomes "prod-d" as an example).
* Removed ray_linked runtime.

🔥 *Features*


👩‍🔬 *Experimental*

* Optional `config` param in `sdk.WorkflowRun.by_id()`. Allows access to workflows submitted by other users or from another machine, if the selected runtime supports it. Per-runtime support will be added separately.
* New CLI command: `python -m orquestra.sdk._base.cli._dorq._entry workflow stop`.
* New CLI commands that require `config` and `workflow_run_id` will now prompt the user for selecting value interactively.
* New CLI commands: `python -m orquestra.sdk._base.cli._dorq._entry up|down|status` for managing local services.

🐛 *Bug Fixes*


*Internal*


*Docs*


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
