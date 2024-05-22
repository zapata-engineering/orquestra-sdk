################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from pathlib import Path
from unittest.mock import create_autospec

import pytest
from orquestra.workflow_shared.logs._interfaces import WorkflowLogs
from orquestra.workflow_shared.schema.ir import ArtifactFormat

from orquestra.sdk._client._base.cli import _dumpers


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
    class TestGetLogsFile:
        @staticmethod
        def test_without_log_type():
            dumper = _dumpers.LogsDumper()
            path = create_autospec(Path)
            assert (
                dumper._get_logs_file(path, wf_run_id="<wf run id sentinel>")
                == path / "<wf run id sentinel>.log"
            )

        @staticmethod
        @pytest.mark.parametrize(
            "log_type, expected_suffix",
            [
                (WorkflowLogs.WorkflowLogTypeName.PER_TASK, "_per_task"),
                (WorkflowLogs.WorkflowLogTypeName.SYSTEM, "_system"),
                (WorkflowLogs.WorkflowLogTypeName.ENV_SETUP, "_env_setup"),
            ],
        )
        def test_with_suffix(log_type, expected_suffix):
            dumper = _dumpers.LogsDumper()
            path = create_autospec(Path)
            assert (
                dumper._get_logs_file(
                    path, wf_run_id="<wf run id sentinel>", log_type=log_type
                )
                == path / f"<wf run id sentinel>{expected_suffix}.log"
            )

    class TestWritingToFile:
        @staticmethod
        def test_no_suffix(tmp_path: Path):
            # Given
            log_values = ["my_logs", "next_log"]
            task_invocation = "my_task_invocation"
            logs = {task_invocation: log_values}
            wf_run_id = "wf.1234"
            wf_log_file = wf_run_id + ".log"
            wf_err_log_file = wf_run_id + ".err"
            dir_path = tmp_path / "new_dir"
            dumper = _dumpers.LogsDumper()

            # When
            paths = dumper.dump(
                logs=logs,
                wf_run_id=wf_run_id,
                dir_path=dir_path,
            )

            # Then
            # Creates file
            children = list(dir_path.iterdir())
            assert len(children) == 2
            # Returns both paths
            assert len(paths) == 2

            # Sensible dump details
            assert paths[0] == (dir_path / wf_log_file)
            assert paths[1] == (dir_path / wf_err_log_file)

            full_logs = paths[0].read_text()
            err_logs = paths[1].read_text()
            for log_value in log_values:
                # all logs should be in the file
                assert log_value in full_logs
            assert task_invocation in full_logs
            assert task_invocation in err_logs

        @staticmethod
        def test_with_logs_dict(tmp_path: Path):
            # Given
            log_values = ["my_logs", "next_log"]
            task_invocation = "my_task_invocation"
            logs = {task_invocation: log_values}
            wf_run_id = "wf.1234"
            wf_log_file = wf_run_id + ".log"
            wf_err_log_file = wf_run_id + ".err"
            dir_path = tmp_path / "new_dir"
            dumper = _dumpers.LogsDumper()

            # When
            paths = dumper.dump(
                logs=logs,
                wf_run_id=wf_run_id,
                dir_path=dir_path,
            )

            # Then
            # Creates file
            children = list(dir_path.iterdir())
            assert len(children) == 2
            # Returns both paths
            assert len(paths) == 2

            # Sensible dump details
            assert paths[0] == (dir_path / wf_log_file)
            assert paths[1] == (dir_path / wf_err_log_file)

            full_logs = paths[0].read_text()
            err_logs = paths[1].read_text()
            for log_value in log_values:
                # all logs should be in the file
                assert log_value in full_logs
            assert task_invocation in full_logs
            assert task_invocation in err_logs

        @staticmethod
        def test_with_logs_sequence(tmp_path: Path):
            # Given
            logs = ["my_logs", "next_log"]
            log_values = ["my_logs", "next_log"]
            wf_run_id = "wf.1234"
            wf_log_file = wf_run_id + ".log"
            wf_err_log_file = wf_run_id + ".err"
            dir_path = tmp_path / "new_dir"
            dumper = _dumpers.LogsDumper()

            # When
            paths = dumper.dump(
                logs=logs,
                wf_run_id=wf_run_id,
                dir_path=dir_path,
            )

            # Then
            # Creates file
            children = list(dir_path.iterdir())
            assert len(children) == 2
            # Returns both paths
            assert len(paths) == 2

            # Sensible dump details
            assert paths[0] == (dir_path / wf_log_file)
            assert paths[1] == (dir_path / wf_err_log_file)

            full_logs = paths[0].read_text()
            for log_value in log_values:
                # all logs should be in the file
                assert log_value in full_logs
