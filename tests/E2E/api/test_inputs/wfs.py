################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from typing import Optional

from orquestra import sdk

from . import tasks


@sdk.workflow
def add_some_ints():
    sum1 = tasks.sum_inline(10, 20)
    sum2 = tasks.sum_inline(sum1, 30)
    # Two separate invocations that produce logs helps with smoke-testing.
    sum3 = tasks.sum_with_logs(sum1, sum2)
    sum4 = tasks.sum_with_logs(200, 300)

    return sum1, sum2, sum3, sum4
