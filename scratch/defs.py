from orquestra import sdk


@sdk.task
def generic_task(*args):
    return 42


@sdk.workflow
def wf():
    reduced = generic_task()
    for _ in range(10):
        fanned_out = [generic_task(reduced) for _ in range(10)]
        reduced = generic_task(*fanned_out)
    return reduced
