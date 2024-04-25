################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from typing import Optional

from orquestra import sdk

from . import tasks


@sdk.workflow
def add_some_ints(
    secret_config: Optional[str],
    secret_workspace: Optional[str],
    github_username: Optional[str],
):
    sum1 = tasks.sum_inline(10, 20)
    sum2 = tasks.sum_inline(sum1, 30)
    sum3 = tasks.make_sum_git_task(
        secret_config=secret_config,
        secret_workspace=secret_workspace,
        github_username=github_username,
    )(sum1, sum2)
    # Two separate invocations that produce logs helps with smoke-testing.
    sum4 = tasks.sum_with_logs(sum1, sum2)
    sum5 = tasks.sum_with_logs(200, 300)

    return sum1, sum3, sum4, sum5
