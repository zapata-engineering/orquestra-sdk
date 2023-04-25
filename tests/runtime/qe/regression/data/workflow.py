import orquestra.sdk as sdk


@sdk.task(source_import=sdk.InlineImport())
def task1():
    return 100


@sdk.task(source_import=sdk.InlineImport())
def task2(*_):
    return 200


@sdk.task(source_import=sdk.InlineImport())
def task3(*_):
    return set()


@sdk.task(source_import=sdk.InlineImport())
def dual():
    return 0, set()


@sdk.workflow
def artifacts_not_ok():
    a = task1()
    b = task2(a)
    c = task3(b)
    _, d = dual()
    return a, c, d
