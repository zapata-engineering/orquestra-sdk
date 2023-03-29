################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

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


def main():
    r = wf().run("in_process")
    r.wait_until_finished()
    tasks = list(r.get_tasks())
    task = next(t for t in tasks if t.task_invocation_id == "invocation-2-task-b")
    print(task)
    breakpoint()
    print(task.get_inputs())


if __name__ == "__main__":
    main()
