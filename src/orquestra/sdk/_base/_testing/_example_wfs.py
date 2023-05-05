################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import time
from pathlib import Path
from typing import Optional, Sequence

import orquestra.sdk as sdk
import orquestra.sdk._base._testing._ipc as ipc


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


@sdk.task(dependency_imports=[sdk.GithubImport("alexjuda/piccup", git_ref="master")])
def task_with_git_import():
    import piccup  # type: ignore # noqa

    # return whatever - make sure it just doesn't assert on import
    return 2


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
        repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
        git_ref="main",
    ),
    dependency_imports=[],
)
def make_greeting_message(first, last, additional_message: Optional[str] = None) -> str:
    return f"hello, {first} {last}!{additional_message or ''}"


@sdk.task(
    n_outputs=2,
    source_import=sdk.GitImport(
        repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
        git_ref="main",
    ),
    dependency_imports=[],
)
def multi_output_test():
    return "hello", "there"


@sdk.task
def make_exception():
    # we want to intentionally throw an exception for the purpose of the test
    42 / 0
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


@sdk.workflow
def wf_using_git_imports():
    return [task_with_git_import()]


@sdk.task
def add(a, b):
    return a + b


@sdk.task
def add_slow(a, b):
    time.sleep(10)

    return a + b


@sdk.workflow
def serial_wf_with_slow_middle_task():
    art1 = add(21, 37)
    art2 = add_slow(art1, art1)
    art3 = add(art2, art2)

    return [art3]


@sdk.task
def add_with_trigger(a, b, port, timeout: float):
    """
    Simulates a task that takes some time to run. Waits until a message
    in given socket appears
    """
    ipc.TriggerClient(port).wait_on_trigger(timeout)

    return a + b


@sdk.workflow
def serial_wf_with_file_triggers(ports: Sequence[int], task_timeout: float):
    """
    Allows reproducing scenario where tasks take some time to run. Uses
    socket-based coordination.

    There are as many workflow graph nodes as there are `ports`. Each
    task in the series waits for message to be present at a corresponding port.
    """
    first_future = add_with_trigger(21, 37, ports[0], timeout=task_timeout)
    future = first_future
    for port in ports[1:]:
        future = add_with_trigger(future, future, port, timeout=task_timeout)

    return [future]


@sdk.task
def add_with_error(a, b):
    """
    Simulates a task with inputs that raises an exception.
    """
    # Raises ZeroDivisionError
    42 / 0

    return a + b


@sdk.workflow
def exception_wf_with_multiple_values():
    """
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
    """
    future1 = add(37, 21)
    future2 = add_with_error(future1, future1)
    future3 = add(future2, future2)

    return [future3]


@sdk.task
def add_with_log(a, b, msg: str):
    import orquestra.sdk._base._log_adapter

    logger = orquestra.sdk._base._log_adapter.workflow_logger()
    logger.info(msg)
    return a + b


@sdk.workflow
def wf_with_log(msg: str):
    return [add_with_log(12, 34, msg)]


@sdk.task
def get_exec_ctx() -> str:
    import orquestra.sdk._base._exec_ctx

    ctx = orquestra.sdk._base._exec_ctx.global_context
    return ctx.name


@sdk.workflow
def wf_with_exec_ctx():
    return [get_exec_ctx()]


@sdk.workflow
def parametrized_wf(a: int):
    return add(a, 5)


@sdk.workflow
def wf_with_secrets():
    secret = sdk.secrets.get("some-secret", config_name="test_config_default")
    return capitalize_inline(secret)


@sdk.workflow
def workflow_parametrised_with_resources(cpu=None, memory=None, gpu=None):
    return add(1, 1).with_invocation_meta(cpu=cpu, memory=memory, gpu=gpu)


@sdk.workflow
def workflow_with_different_resources():
    cpu = add(1, 1).with_invocation_meta(cpu="5000m")
    small_cpu = add(1, 1).with_invocation_meta(cpu="1000m")
    memory = add(1, 1).with_invocation_meta(memory="3G")
    small_memory = add(1, 1).with_invocation_meta(memory="512Mi")
    gpu = add(1, 1).with_invocation_meta(gpu="1")
    all_resources = add(1, 1).with_invocation_meta(cpu="2000m", memory="2Gi", gpu="0")
    return cpu, small_cpu, memory, small_memory, gpu, all_resources
