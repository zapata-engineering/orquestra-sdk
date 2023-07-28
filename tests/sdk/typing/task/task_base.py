################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import orquestra.sdk as sdk


@sdk.task
def task(a: int) -> int:
    return a


@sdk.task
def kw_task(*, a: int) -> int:
    return a
