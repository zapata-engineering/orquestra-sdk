################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import polars as pl  # type: ignore

import orquestra.sdk as sdk


@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=[sdk.PythonImports("polars==0.17.3")],
)
def my_fn(df):
    return df.item()


@sdk.workflow
def wf():
    thing = pl.DataFrame({"a": 21})
    return my_fn(thing)


if __name__ == "__main__":
    print(wf.model.json(exclude_none=True))
