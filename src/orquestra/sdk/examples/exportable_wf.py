################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Example of a workflow file that can be exported to v1 (yaml) format and run on
Orquestra cluster.
"""  # noqa: D205
import typing as t

# public API
import orquestra.sdk as sdk


@sdk.task(
    source_import=sdk.GitImport(
        repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",
        git_ref="main",
    ),
)
def capitalize(text: str) -> str:
    return text.capitalize()


@sdk.task(
    source_import=sdk.GitImport(
        repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",
        git_ref="main",
    ),
)
def make_greeting(first, last, additional_message: t.Optional[str] = None) -> str:
    return f"hello, {first} {last}!{additional_message or ''}"


@sdk.task(
    n_outputs=2,
    source_import=sdk.GitImport(
        repo_url="git@github.com:zapata-engineering/orquestra-sdk.git",
        git_ref="main",
    ),
)
def multi_output_test():
    return "hello", "there"


@sdk.workflow
def my_workflow():
    first_name = "alex"
    last_name = "zapata"
    _, there = multi_output_test()
    return [make_greeting(first=first_name, last=last_name, additional_message=there)]


def capitalize_no_decorator(text: str) -> str:
    return text.capitalize()


def main():
    # output the intermediate workflow representation as a JSON
    print(my_workflow.model.model_dump_json())


if __name__ == "__main__":
    main()
