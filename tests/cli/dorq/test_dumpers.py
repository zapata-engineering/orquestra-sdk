################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from pathlib import Path

import pytest

from orquestra.sdk._base.cli._dorq import _dumpers
from orquestra.sdk.schema.ir import ArtifactFormat


class TestWFOutputDumper:
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
        dumper = _dumpers.WFOutputDumper()

        # When
        details = dumper.dump(
            value=value,
            wf_run_id=wf_run_id,
            output_index=output_index,
            dir_path=tmp_path,
        )

        # Then
        # Creates a subdir for workflow run
        children = list(tmp_path.iterdir())
        assert len(children) == 1
        assert children[0].name == wf_run_id

        # Creates a subdir with a static name
        grandchildren = list(children[0].iterdir())
        assert len(grandchildren) == 1
        assert grandchildren[0].name == "wf_results"

        # Creates a file for the artifact
        grand2children = list(grandchildren[0].iterdir())
        assert len(grand2children) == 1

        # Sensible dump details
        assert details.file_path == grand2children[0]
        assert details.format in {ArtifactFormat.JSON, ArtifactFormat.ENCODED_PICKLE}

        # Created extension
        assert details.file_path.suffix in {".json", ".pickle"}

    @staticmethod
    def test_no_dir(tmp_path: Path):
        # Given
        value = {"general": "kenobi"}
        wf_run_id = "wf.1234"
        output_index = 2
        dir_path = tmp_path / "new_dir"
        dumper = _dumpers.WFOutputDumper()

        # When
        details = dumper.dump(
            value=value,
            wf_run_id=wf_run_id,
            output_index=output_index,
            dir_path=dir_path,
        )

        # Then
        # Creates a subdir for workflow run
        children = list(dir_path.iterdir())
        assert len(children) == 1
        assert children[0].name == wf_run_id

        # Creates a subdir with a static name
        grandchildren = list(children[0].iterdir())
        assert len(grandchildren) == 1
        assert grandchildren[0].name == "wf_results"
        assert grandchildren[0].is_dir()

        # Creates a file for the artifact
        grand2children = list(grandchildren[0].iterdir())
        assert len(grand2children) == 1
        assert grand2children[0].name == "2.json"
        assert grand2children[0].is_file()

        # Sensible dump details
        assert details.file_path == grand2children[0]
        assert details.format in {ArtifactFormat.JSON, ArtifactFormat.ENCODED_PICKLE}


class TestTaskOutputDumper:
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
        task_inv_id = "inv3"
        output_index = 5
        dumper = _dumpers.TaskOutputDumper()

        # When
        details = dumper.dump(
            value=value,
            wf_run_id=wf_run_id,
            task_inv_id=task_inv_id,
            output_index=output_index,
            dir_path=tmp_path,
        )

        # Then
        # Creates a subdir for workflow run
        children = list(tmp_path.iterdir())
        assert len(children) == 1
        assert children[0].name == wf_run_id

        # Creates a subdir with a static name
        grandchildren = list(children[0].iterdir())
        assert len(grandchildren) == 1
        assert grandchildren[0].name == "task_results"
        assert grandchildren[0].is_dir()

        # Creates a directory for the invocation
        grand2children = list(grandchildren[0].iterdir())
        assert len(grand2children) == 1
        assert grand2children[0].name == task_inv_id
        assert grand2children[0].is_dir()

        # Creates a file for the artifact
        grand3children = list(grand2children[0].iterdir())
        assert len(grand3children) == 1
        assert grand3children[0].is_file()

        # Sensible dump details
        assert details.file_path == grand3children[0]
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
        wf_log_file = wf_run_id + ".log"
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
