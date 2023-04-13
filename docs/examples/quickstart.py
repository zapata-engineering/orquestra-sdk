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
