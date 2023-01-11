################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import orquestra.sdk as sdk


@sdk.task
def return_num(*args):
    return len(args)


@sdk.workflow
def simple_wf_one_task_two_invocations():
    return [return_num(), return_num()]


@sdk.task(source_import=sdk.InlineImport())
def return_five_inline():
    return 5


@sdk.task
def args_kwargs(*args, **kwargs):
    return 21


@sdk.workflow
def simple_wf_one_task_inline():
    return [return_five_inline()]


@sdk.workflow
def wf_task_with_two_parents():
    five = return_num(5)
    sum_1 = return_num(five)
    sum_2 = return_num(five)
    return [return_num(sum_1, sum_2, 7)]


@sdk.workflow
def wf_single_task_with_const_parent():
    return [return_num(21)]


@sdk.workflow
def wf_single_task_with_const_parent_kwargs():
    return [args_kwargs(**{"kwargs": 36})]


@sdk.workflow
def wf_single_task_with_const_parent_args_kwargs():
    return [args_kwargs(21, **{"kwargs": 36})]


@sdk.workflow
def wf_for_input_test():
    art_1 = return_num(5)
    art_2 = args_kwargs(10, **{"kwarg": art_1})
    art_3 = args_kwargs(10, **{"kwarg": art_1})
    return [return_num(art_2, art_3, 7)]


@sdk.task
def task_multiple_outputs():
    return 21, 36


@sdk.workflow
def wf_multi_output_task():
    art_1, _ = task_multiple_outputs()
    return [return_num(art_1)]
