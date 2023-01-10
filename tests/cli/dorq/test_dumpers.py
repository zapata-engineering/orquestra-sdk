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
