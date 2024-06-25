# since orquestra-workflow-runtime does not have access to client side
# but it requires IR WF models as test input - we store those inputs as generated
# JSON's, but this script allows to regenerate it on demand

# type: ignore

import json
import time
from typing import Tuple

import orquestra.sdk as sdk
from orquestra.workflow_shared.schema.ir import WorkflowDef


@sdk.task
def add_slow(a, b):
    time.sleep(10)

    return a + b


@sdk.task
def multioutput_task_failing():
    assert False
    return "Zapata", "Computing"


@sdk.task(n_outputs=2)
def multioutput_task_failing_n_outpus() -> Tuple:
    assert False
    return "Zapata", "Computing"


@sdk.task
def make_greeting(name, company_name):
    return f"yooooo {name} from {company_name}"


@sdk.task(n_outputs=2)
def multioutput_task():
    return "Zapata", "Computing"


@sdk.task
def make_company_name(last_name: str):
    return f"{last_name} computing"


@sdk.task
def capitalize(text: str) -> str:
    return text.capitalize()


@sdk.task
def concat(text1: str, text2: str) -> str:
    return text1 + " " + text2


@sdk.task
def add_with_error(a, b):
    """Simulates a task with inputs that raises an exception."""
    # Raises ZeroDivisionError
    _ = 42 / 0

    return a + b


@sdk.task
def add(a, b):
    return a + b


def make_workflow_with_dependencies(deps, *, n_tasks=1):
    """Generate a workflow definition with the specified dependencies."""

    @sdk.task(dependency_imports=deps)
    def hello_orquestra() -> str:
        return "Hello Orquestra!"

    @sdk.workflow()
    def hello_orquestra_wf():
        return [hello_orquestra() for _ in range(n_tasks)]

    return hello_orquestra_wf()


def save_in_file(model: WorkflowDef, name_of_file: str):
    with open(name_of_file, "w", encoding="utf-8") as f:
        json.dump(model.json(), f, indent=4)


def generate_100_tasks_with_imps():
    imps = [
        sdk.GithubImport(
            "zapata-engineering/orquestra-sdk",
            personal_access_token=sdk.Secret(
                "mock-secret", config_name="mock-config", workspace_id="mock-ws"
            ),
        ),
        sdk.PythonImports("numpy", "polars"),
        sdk.InlineImport(),
    ]
    workflow_def = make_workflow_with_dependencies(imps, n_tasks=100).model
    save_in_file(workflow_def, "100_tasks_with_imps.json")


def generate_wildcard_deps():
    imps = (sdk.PythonImports("numpy"),)
    workflow_def = make_workflow_with_dependencies(imps).model
    # find python import
    x = [
        imp_id
        for imp_id, imp in workflow_def.imports.items()
        if imp.type == "PYTHON_IMPORT"
    ]

    workflow_def.imports[x[0]].packages[0].name = "<WILDCARD_NAME>"
    workflow_def.imports[x[0]].packages[0].version_constraints = ["<WILDCARD_VERSION>"]

    save_in_file(workflow_def, "wildcard.json")


def generate_simple_wf():
    workflow_def = make_workflow_with_dependencies([]).model
    save_in_file(workflow_def, "simple_wf.json")


def generate_multioutput_wf():
    @sdk.workflow
    def multioutput_wf():
        last_name = "zapata"
        company_name = make_company_name(last_name)
        company_cap = capitalize(company_name)
        last_cap = capitalize(last_name)
        full_name = concat("Emiliano", last_cap)

        return [full_name, company_cap]

    workflow_def = multioutput_wf().model
    save_in_file(workflow_def, "multioutput.json")


def generate_multioutput_wf_with_exception():
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

    workflow_def = exception_wf_with_multiple_values().model
    save_in_file(workflow_def, "exception_wf_with_multiple_values.json")


def generate_wf_with_exec_ctx():
    @sdk.task
    def get_exec_ctx() -> str:
        import orquestra.workflow_shared.exec_ctx

        ctx = orquestra.workflow_shared.exec_ctx.get_current_exec_context()
        return ctx.name

    @sdk.workflow
    def wf_with_exec_ctx():
        return [get_exec_ctx()]

    workflow_def = wf_with_exec_ctx().model
    save_in_file(workflow_def, "wf_with_exec_ctx.json")


def generate_multioutput_task_wf():
    @sdk.workflow
    def multioutput_task_wf():
        a, b = multioutput_task()
        _, c = multioutput_task()
        d, _ = multioutput_task()
        packed = multioutput_task()
        f, g = packed
        return a, b, c, d, packed, f, g

    workflow_def = multioutput_task_wf().model
    save_in_file(workflow_def, "multioutput_task_wf.json")


def generate_greet_wf():
    @sdk.workflow
    def greet_wf():
        name = "emiliano"
        company_name = "zapata computing"
        greeting = make_greeting(name, company_name)
        return [greeting]

    workflow_def = greet_wf().model
    save_in_file(workflow_def, "greet_wf.json")


def generate_multioutput_task_failed_wf():
    @sdk.workflow
    def multioutput_task_failed_wf():
        a, b = multioutput_task_failing_n_outpus()
        _, c = multioutput_task_failing_n_outpus()
        d, _ = multioutput_task_failing()
        packed = multioutput_task_failing()
        f, g = packed
        return a, b, c, d, packed, f, g

    workflow_def = multioutput_task_failed_wf().model
    save_in_file(workflow_def, "multioutput_task_failed_wf.json")


def generate_serial_wf_with_slow_middle_task():
    @sdk.workflow
    def serial_wf_with_slow_middle_task():
        art1 = add(21, 37)
        art2 = add_slow(art1, art1)
        art3 = add(art2, art2)

        return [art3]

    workflow_def = serial_wf_with_slow_middle_task().model
    save_in_file(workflow_def, "serial_wf_with_slow_middle_task.json")


def generate_cause_env_setup_error():
    @sdk.task(
        source_import=sdk.InlineImport(),
        dependency_imports=sdk.PythonImports("MarkupSafe==1.0.0", "Jinja2==2.7.2"),
    )
    def cause_env_setup_error_task() -> bool:
        return True

    @sdk.workflow
    def cause_env_setup_error():
        """This WF errors out during environment setup so we can test how it is handled.

        Jinja2 relies on a higher version of MarkupSafe, so the dependency imports
        for the
        cause_env_setup_error_task task will cause pip to throw an error.
        """
        return [cause_env_setup_error_task()]

    workflow_def = cause_env_setup_error().model
    save_in_file(workflow_def, "cause_env_setup_error.json")


def generate_infinite_workflow():
    """Allows reproducing scenario where tasks take some time to run.

    This workflow is used to test termination as it will never complete.
    This workflow isn't actually infinite - it just takes an hour of sleep time to
    complete.
    """

    @sdk.task
    def long_task(*_):
        import time

        # sleep for an hour - just in case someone forgets to terminate this buddy
        time.sleep(60 * 60)

    @sdk.workflow
    def infinite_workflow():
        """Allows reproducing scenario where tasks take some time to run.

        This workflow is used to test termination as it will never complete.
        This workflow isn't actually infinite - it just takes an hour of sleep time to
        complete.
        """
        return long_task()

    workflow_def = infinite_workflow().model
    save_in_file(workflow_def, "infinite_workflow.json")


def main():
    generate_100_tasks_with_imps()
    generate_simple_wf()
    generate_wildcard_deps()
    generate_multioutput_wf()
    generate_wf_with_exec_ctx()
    generate_multioutput_wf_with_exception()
    generate_multioutput_task_wf()
    generate_greet_wf()
    generate_multioutput_task_failed_wf()
    generate_serial_wf_with_slow_middle_task()
    generate_cause_env_setup_error()
    generate_infinite_workflow()


if __name__ == "__main__":
    main()
