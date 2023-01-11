################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import warnings

import orquestra.sdk as sdk

# +"NotATaskWarning",

source_import: sdk.Import = sdk.InlineImport()


def not_a_task():
    ...


@sdk.task(
    source_import=source_import,
    dependency_imports=[
        sdk.LocalImport(module="test"),
        sdk.GitImport(
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk",
            git_ref="main",
        ),
        sdk.GithubImport(repo="zapatacomputing/orquestra-workflow-sdk"),
        sdk.PythonImports("orquestra-sdk"),
    ],
)
def my_task():
    ...


assert isinstance(my_task, sdk.TaskDef)


with warnings.catch_warnings():
    warnings.simplefilter("ignore", sdk.NotATaskWarning)

    @sdk.workflow(
        data_aggregation=sdk.DataAggregation(resources=sdk.Resources(cpu="1000m"))
    )
    def my_workflow():
        return my_task(), not_a_task()


assert isinstance(my_workflow, sdk.WorkflowTemplate)

wf_def = my_workflow()

assert isinstance(wf_def, sdk.WorkflowDef)

wf_run = wf_def.run(sdk.RuntimeConfig.in_process())

assert isinstance(wf_run, sdk.WorkflowRun)
