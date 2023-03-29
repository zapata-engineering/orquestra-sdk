################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t
from orquestra import sdk


@sdk.task(source_import=sdk.InlineImport())
def a(thing=None):
    return thing or "hello-arg"


@sdk.task(source_import=sdk.InlineImport())
def b(*args):
    return 1, 2, 3, 4


@sdk.workflow
def wf():
    w, x, y, z = b()
    return a(a(None)), b(b()), b(w, x, y, z)


inline_task = sdk.task(source_import=sdk.InlineImport())


@inline_task
def multi_outputs():
    return 1, 2


@inline_task
def sum_tuple(ints_tuple: t.Tuple[int, ...]) -> int:
    return sum(ints_tuple)


@sdk.workflow
def wf_simplified():
    packed = multi_outputs()
    return sum_tuple(packed)


def main():
    wf_def = wf_simplified()
    r = wf_def.run("in_process")
    r.wait_until_finished()
    tasks = list(r.get_tasks())
    task = next(
        t for t in tasks if t.task_invocation_id == "invocation-0-task-sum-tuple"
    )
    print(task)
    breakpoint()

    wf_def.graph.render("wf")
    print(task.get_inputs())


if __name__ == "__main__":
    main()
