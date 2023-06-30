MLFlow Utilities
================

Uploading Artifacts
-------------------

Artifacts must be written to a file before being uploaded to MLFlow. Rather than manually configuring an appropriate temporary directory for creating these files, we provide the ``get_temp_artifacts_dir()`` utility. This function can be used in any runtime and will return an appropriate directory.

.. code-block:: python

    from orquestra import sdk
    from pathlib import Path
    import mlflow

    artifacts_dir: Path = sdk.mlflow.get_temp_artifacts_dir()

    artifact_path = artifacts_dir / "final_state_dict.pickle"
    with artifact_path.open("wb") as f:
        pickle.dumps(my_model.state_dict())
    mlflow.log_artifact(artifact_path)

Compute Engine and Studio both configure temporary directories automatically. For local workflows, the default location is `~/.orquestra/mlflow/artifacts`, however this can be overridden by setting the ``ORQ_MLFLOW_ARTIFACTS_DIR`` environment variable.
