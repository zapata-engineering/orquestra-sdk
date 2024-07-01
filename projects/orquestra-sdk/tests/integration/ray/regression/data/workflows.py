################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import orquestra.sdk as sdk


@sdk.task(source_import=sdk.InlineImport())
def single_json():
    return 1


@sdk.task(source_import=sdk.InlineImport())
def multi_json():
    return 1, "hello"


@sdk.task(source_import=sdk.InlineImport())
def single_pickle():
    return set()


@sdk.task(source_import=sdk.InlineImport())
def multi_pickle():
    return set(), set()


@sdk.workflow
def single_json_wf():
    a = single_json()
    return a


@sdk.workflow
def multi_json_wf():
    a, b = multi_json()
    return a, b


@sdk.workflow
def single_pickle_wf():
    a = single_pickle()
    return a


@sdk.workflow
def multi_pickle_wf():
    a, b = multi_pickle()
    return a, b
