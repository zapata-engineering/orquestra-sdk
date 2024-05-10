################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
# >> Tutorial code snippet: quickstart
# use for literalinclude start-after >> Start
import orquestra.sdk as sdk


@sdk.task
def hello_orquestra() -> str:
    return "Hello Orquestra!"


@sdk.workflow
def hello_orquestra_wf():
    return [hello_orquestra()]


# >> End - for literalinclide end-before
