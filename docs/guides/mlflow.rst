.. © Copyright 2023 Zapata Computing Inc.

MLFlow Utilities
================

Uploading Artifacts
-------------------

Artifacts must be written to a file before being uploaded to MLFlow. Rather than manually configuring an appropriate temporary directory for creating these files, we provide the ``get_temp_artifacts_dir()`` utility. This function can be used in any runtime and will return an appropriate directory.

.. literalinclude:: ../examples/tests/test_mlflow_utilities.py
    :start-after: def artifacts_dir_snippet():
    :end-before: </snippet>
    :language: python
    :dedent: 8


Compute Engine and Studio both configure temporary directories automatically. For local workflows, the default location is `~/.orquestra/mlflow/artifacts`, however this can be overridden by setting the ``ORQ_MLFLOW_ARTIFACTS_DIR`` environment variable.
