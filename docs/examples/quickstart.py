################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
# >> Tutorial code snippet: quickstart
# use for literalinclude start-after >> Start
from workflow_defs import hello_orquestra_wf

import orquestra.sdk as sdk

wf = hello_orquestra_wf()
wf_run = wf.run("in_process")
results = wf_run.get_results()

print(results)

# >> End - for literalinclide end-before

# >> Tutorial code snippet: run workflow with stored config - long version

wf = hello_orquestra_wf()

config = sdk.RuntimeConfig.load("in_process")
wf_run_1 = wf.run(config)

# >> end run workflow with stored config - long version

# >> Tutorial code snippet: run workflow with stored config - short version

wf_run_2 = wf.run("in_process")

# >> end run workflow with stored config - short version

results_1 = wf_run_1.get_results()
results_2 = wf_run_2.get_results()
assert results_1 == results_2, f"Expected {results_1} to look like {results_2}"
assert (
    results_1 == "Hello Orquestra!"
), f"Expected {results_1} to look like 'Hello Orquestra!'"

# Rename wf_run_1 to demonstrate some of its functions without having to refer back to
# the examples above
wf_run = wf_run_1

# >> Tutorial code snippet: get tasks

tasks = wf_run.get_tasks()
print(list(tasks)[0])

# >> end get tasks

# >> Tutorial code snippet: simple filter tasks
# Get only tasks that have failed
from orquestra.sdk.schema.workflow_run import State  # noqa: E402

succeeded_tasks = wf_run.get_tasks(state=State.SUCCEEDED)

# Get only tasks that execute a specific function
hello_orquestra_tasks = wf_run.get_tasks(function_name="hello_orquestra")

# Get only tasks matching a specific task run ID
run_id_tasks = wf_run.get_tasks(
    task_run_id="hello_orquestra_wf-1-invocation-0-task-hello-orquestra"
)

# Get only tasks matching a specific task invocation ID
inv_id_tasks = wf_run.get_tasks(task_invocation_id="invocation-0-task-hello-orquestra")

# >> end simple filter tasks

# >> Tutorial code snippet: multiple states

incomplete_tasks = wf_run.get_tasks(state=[State.WAITING, State.RUNNING])

# >> end multiple states

# >> Tutorial code snippet: regex filters

hello_tasks = wf_run.get_tasks(function_name="hello_.*")
run_id_regex_tasks = wf_run.get_tasks(
    task_run_id="hello_orquestra_wf-\\d*-invocation-\\d*-task-hello-orquestra"
)
inv_id_regex_tasks = wf_run.get_tasks(task_invocation_id=".+hello-orquestra")

# >> end regex filters

# >> Tutorial code snippet: complex filter tasks

multiple_filter_tasks = wf_run.get_tasks(
    state=State.SUCCEEDED,
    function_name="hello_orquestra",
    task_run_id="hello_orquestra_wf-1-invocation-0-task-hello-orquestra",
    task_invocation_id="invocation-0-task-hello-orquestra",
)

# >> end complex filter tasks

for i, task_set in enumerate(
    [
        succeeded_tasks,
        hello_orquestra_tasks,
        run_id_tasks,
        inv_id_tasks,
        hello_tasks,
        run_id_regex_tasks,
        inv_id_regex_tasks,
        multiple_filter_tasks,
    ]
):
    assert len(task_set) == 1, f"Failed on index {i}"

assert len(incomplete_tasks) == 0
