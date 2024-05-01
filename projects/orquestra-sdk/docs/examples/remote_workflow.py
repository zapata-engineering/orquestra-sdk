################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################

# >> Tutorial code snippet: remote workflow definition
import orquestra.sdk as sdk


@sdk.task(
    source_import=sdk.GitImport.infer(),
)
def hello_orquestra() -> str:
    return "Hello Orquestra!"


@sdk.workflow
def hello_orquestra_wf():
    return [hello_orquestra()]


# >> End
