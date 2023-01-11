################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################

import orquestra.sdk as sdk
from orquestra.sdk._base._log_adapter import wfprint, workflow_logger


@sdk.task(n_outputs=1)
def hello(n):
    log = workflow_logger()
    log.info("Useful work done here...")
    wfprint(f"Hello Orquestra #{n}!")
    return "Hello"


@sdk.workflow
def hello_orquestra():
    return [hello(1), hello(2), hello(3)]


@sdk.task(n_outputs=1)
def do_something_useful():
    log = workflow_logger()
    log.info("Info message from do_something_useful()")
    log.warning("Warning message from do_something_useful()")
    log.error("Error message from do_something_useful()")
    return 42


@sdk.workflow
def hello_workflow():
    return [do_something_useful()]


if __name__ == "__main__":
    print()
    result = hello_orquestra().local_run()
    print("Workflow result:", result)
    print()
    result2 = hello_workflow().local_run()
    print("Workflow #2 result:", result2)
