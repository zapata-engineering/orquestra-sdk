################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from typing import Optional, Sequence, Tuple

import orquestra.sdk as sdk

from . import _ipc as ipc


@sdk.task
def make_greeting(name, company_name):
    return f"yooooo {name} from {company_name}"


@sdk.workflow
def greet_wf():
    name = "emiliano"
    company_name = "zapata computing"
    greeting = make_greeting(name, company_name)
    return [greeting]


@sdk.workflow
def greet_wf_kw():
    name = "emiliano"
    company_name = "zapata computing"
    greeting = make_greeting(name=name, company_name=company_name)
    return [greeting]


@sdk.task
def capitalize(text: str) -> str:
    return text.capitalize()


@sdk.task(source_import=sdk.InlineImport())
def capitalize_inline(text: str) -> str:
    return text.capitalize()


@sdk.task
def concat(text1: str, text2: str) -> str:
    return text1 + " " + text2


@sdk.task(n_outputs=2)
def multioutput_task():
    return "Zapata", "Computing"


@sdk.task
def multioutput_task_failing():
    assert False
    return "Zapata", "Computing"


@sdk.task(n_outputs=2)
def multioutput_task_failing_n_outpus() -> Tuple:
    assert False
    return "Zapata", "Computing"


@sdk.workflow
def complicated_wf():
    first_name = "emiliano"
    last_name = "zapata"
    company_name = "zapata computing"
    company_cap = capitalize(company_name)

    full_name = concat(first_name, capitalize(last_name))
    greeting = make_greeting(name=full_name, company_name=company_cap)

    unused_future = capitalize(first_name)  # noqa: F841

    return [greeting]


@sdk.task
def make_company_name(last_name: str):
    return f"{last_name} computing"


@sdk.task(
    source_import=sdk.GitImport(
        repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",
        git_ref="main",
    ),
    dependency_imports=[],
)
def make_greeting_message(first, last, additional_message: Optional[str] = None) -> str:
    return f"hello, {first} {last}!{additional_message or ''}"


@sdk.task(
    n_outputs=2,
    source_import=sdk.GitImport(
        repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",
        git_ref="main",
    ),
    dependency_imports=[],
)
def multi_output_test():
    return "hello", "there"


@sdk.task
def make_exception():
    # we want to intentionally throw an exception for the purpose of the test
    _ = 42 / 0
    return 42


@sdk.workflow
def multioutput_wf():
    last_name = "zapata"
    company_name = make_company_name(last_name)
    company_cap = capitalize(company_name)
    last_cap = capitalize(last_name)
    full_name = concat("Emiliano", last_cap)

    return [full_name, company_cap]


@sdk.workflow
def multioutput_task_wf():
    a, b = multioutput_task()
    _, c = multioutput_task()
    d, _ = multioutput_task()
    packed = multioutput_task()
    f, g = packed
    return a, b, c, d, packed, f, g


@sdk.workflow
def multioutput_task_failed_wf():
    a, b = multioutput_task_failing_n_outpus()
    _, c = multioutput_task_failing_n_outpus()
    d, _ = multioutput_task_failing()
    packed = multioutput_task_failing()
    f, g = packed
    return a, b, c, d, packed, f, g


@sdk.workflow
def my_workflow():
    first_name = "alex"
    last_name = "zapata"
    _, there = multi_output_test()
    return [
        make_greeting_message(
            first=first_name, last=last_name, additional_message=there
        )
    ]


@sdk.workflow
def exception_wf():
    return [make_exception()]


@sdk.workflow
def wf_using_inline_imports():
    last_name = "zapata"
    company_name = make_company_name(last_name)
    company_cap = capitalize_inline(company_name)
    last_cap = capitalize_inline(last_name)
    full_name = concat("Emiliano", last_cap)

    return [full_name, company_cap]


@sdk.task(dependency_imports=[sdk.GithubImport("alexjuda/piccup", git_ref="master")])
def task_with_git_import():  # pragma: no cover
    import piccup  # type: ignore # noqa

    # return whatever - make sure it just doesn't assert on import
    return 2


@sdk.workflow
def wf_using_git_imports():
    return [task_with_git_import()]


@sdk.task(dependency_imports=[sdk.PythonImports("inflect==7.0.0")])
def task_throwing_library_exception():
    import inflect  # type: ignore # noqa

    p = inflect.engine()
    p.number_to_words(1234, group=-1)


@sdk.workflow
def workflow_throwing_3rd_party_exception():
    return task_throwing_library_exception()


@sdk.task(dependency_imports=[sdk.PythonImports("polars")])
def task_with_python_imports() -> int:
    import polars  # type: ignore # noqa

    df = polars.DataFrame({"text": ["hello", "there"]})

    return len(df)


@sdk.workflow
def wf_using_python_imports(log_message: str):
    return [add_with_log(21, 37, msg=log_message), task_with_python_imports()]


@sdk.task
def add(a, b):
    return a + b


@sdk.task
def add_with_trigger(a, b, port, timeout: float):
    """Simulates a task that takes some time to run.

    Waits until a message in given socket appears.
    """
    ipc.TriggerClient(port).wait_on_trigger(timeout)

    return a + b


@sdk.workflow
def serial_wf_with_file_triggers(ports: Sequence[int], task_timeout: float):
    """Allows reproducing scenario where tasks take some time to run.

    Uses socket-based coordination.

    There are as many workflow graph nodes as there are `ports`.
    Each task in the series waits for message to be present at a corresponding port.
    """
    first_future = add_with_trigger(21, 37, ports[0], timeout=task_timeout)
    future = first_future
    for port in ports[1:]:
        future = add_with_trigger(future, future, port, timeout=task_timeout)

    return [future]


@sdk.task
def add_with_error(a, b):
    """Simulates a task with inputs that raises an exception."""
    # Raises ZeroDivisionError
    _ = 42 / 0

    return a + b


@sdk.workflow
def exception_wf_with_multiple_values():
    """::

    [1]
    │
    ▼
    [2] => exception
    │
    ▼
    [3] => won't run
    │
    ▼
    [return]

    """  # noqa: D415
    future1 = add(37, 21)
    future2 = add_with_error(future1, future1)
    future3 = add(future2, future2)

    return [future3]


@sdk.task
def add_with_log(a, b, msg: str):
    print(msg)
    return a + b


@sdk.workflow
def wf_with_log(msg: str):
    return [add_with_log(12, 34, msg)]


@sdk.workflow
def parametrized_wf(a: int):
    return add(a, 5)


@sdk.workflow
def wf_with_secrets():
    secret = sdk.secrets.get(
        "some-secret", config_name="test_config_default", workspace_id="test_workspace"
    )
    return capitalize_inline(secret)


@sdk.workflow
def workflow_parametrised_with_resources(
    cpu=None, memory=None, gpu=None, custom_image=None
):
    future = add(1, 1)

    if cpu is not None:
        future = future.with_invocation_meta(cpu=cpu)
    if memory is not None:
        future = future.with_invocation_meta(memory=memory)
    if gpu is not None:
        future = future.with_invocation_meta(gpu=gpu)
    if custom_image is not None:
        future = future.with_invocation_meta(custom_image=custom_image)

    return future


@sdk.workflow
def workflow_with_different_resources():
    cpu = add(1, 1).with_invocation_meta(cpu="5000m")
    small_cpu = add(1, 1).with_invocation_meta(cpu="1000m")
    memory = add(1, 1).with_invocation_meta(memory="3G")
    small_memory = add(1, 1).with_invocation_meta(memory="512Mi")
    gpu = add(1, 1).with_invocation_meta(gpu="1")
    all_resources = add(1, 1).with_invocation_meta(cpu="2000m", memory="2Gi", gpu="0")
    return cpu, small_cpu, memory, small_memory, gpu, all_resources


@sdk.task(source_import=sdk.InlineImport(), n_outputs=1)
def task_with_single_output_explicit():
    return True


@sdk.workflow
def wf_with_explicit_n_outputs():
    return task_with_single_output_explicit()


@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=sdk.PythonImports("MarkupSafe==1.0.0", "Jinja2==2.7.2"),
)
def cause_env_setup_error_task() -> bool:
    return True


@sdk.workflow
def cause_env_setup_error():
    """This WF errors out during environment setup so we can test how it is handled.

    Jinja2 relies on a higher version of MarkupSafe, so the dependency imports for the
    cause_env_setup_error_task task will cause pip to throw an error.
    """
    return [cause_env_setup_error_task()]


class GetEnvWhileReduce:
    def __init__(self):
        import os

        self.val = os.getenv("MY_UNIQUE_ENV")

    def __reduce__(self):
        return (GetEnvWhileReduce, tuple())


@sdk.task(env_vars={"MY_UNIQUE_ENV": "MY_UNIQUE_VALUE"})
def get_env_task(obj):
    return obj.val


@sdk.workflow
def get_env_before_task_executes_task():
    obj = GetEnvWhileReduce()
    return [get_env_task(obj)]
