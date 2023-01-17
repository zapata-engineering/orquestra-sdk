################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from pathlib import Path

import pytest

from orquestra.sdk._base.cli._dorq import _dumpers
from orquestra.sdk.schema.ir import ArtifactFormat


class TestArtifactDumper:
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param({"hello": "there"}, id="jsonable_value"),
            pytest.param(object(), id="non_jsonable_value"),
        ],
    )
    def test_dir_exists(tmp_path: Path, value):
        # Given
        wf_run_id = "wf.1234"
        output_index = 1
        dumper = _dumpers.ArtifactDumper()

        # When
        details = dumper.dump(
            value=value,
            wf_run_id=wf_run_id,
            output_index=output_index,
            dir_path=tmp_path,
        )

        # Then
        # Creates file
        children = list(tmp_path.iterdir())
        assert len(children) == 1

        # Sensible dump details
        assert details.file_path == children[0]
        assert details.format in {ArtifactFormat.JSON, ArtifactFormat.ENCODED_PICKLE}

        # Created extension
        assert details.file_path.suffix in {".json", ".pickle"}

    @staticmethod
    def test_no_dir(tmp_path: Path):
        # Given
        value = {"general": "kenobi"}
        wf_run_id = "wf.1234"
        output_index = 1
        dir_path = tmp_path / "new_dir"
        dumper = _dumpers.ArtifactDumper()

        # When
        details = dumper.dump(
            value=value,
            wf_run_id=wf_run_id,
            output_index=output_index,
            dir_path=dir_path,
        )

        # Then
        # Creates file
        children = list(dir_path.iterdir())
        assert len(children) == 1

        # Sensible dump details
        assert details.file_path == children[0]
        assert details.format in {ArtifactFormat.JSON, ArtifactFormat.ENCODED_PICKLE}

        # Created extension
        assert details.file_path.suffix in {".json", ".pickle"}


class TestLogsDumper:
    @staticmethod
    def test_logs_dumper(tmp_path: Path):
        # Given
        log_values = ["my_logs", "next_log"]
        task_invocation = "my_task_invocation"
        logs = {task_invocation: log_values}
        wf_run_id = "wf.1234"
        wf_log_file = wf_run_id + ".logs"
        dir_path = tmp_path / "new_dir"
        dumper = _dumpers.LogsDumper()

        # When
        path = dumper.dump(
            logs=logs,
            wf_run_id=wf_run_id,
            dir_path=dir_path,
        )

        # Then
        # Creates file
        children = list(dir_path.iterdir())
        assert len(children) == 1

        # Sensible dump details
        assert path == (dir_path / wf_log_file)

        with path.open("r") as f:
            full_logs = "".join(f.readlines())
            for log_value in log_values:
                # all logs should be in the file
                assert log_value in full_logs
            assert task_invocation in full_logs
