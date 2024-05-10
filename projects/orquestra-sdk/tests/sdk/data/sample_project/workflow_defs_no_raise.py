################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import helpers  # type: ignore

import orquestra.sdk as sdk


@sdk.task
def capitalize_task(text):
    return helpers.capitalize_helper(text)


@sdk.workflow
def sample_workflow():
    return [capitalize_task("sample")]


if __name__ == "__main__":
    model = sample_workflow.model
