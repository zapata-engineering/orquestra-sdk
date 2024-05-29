################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from pathlib import Path
from typing import List
from unittest.mock import Mock, PropertyMock, create_autospec

import pytest

from orquestra.workflow_shared.logs import _models


class TestLogStreamType:
    @pytest.mark.parametrize(
        "value, expected",
        (
            # Known members
            (".out", _models.LogStreamType.STDOUT),
            (".err", _models.LogStreamType.STDERR),
            # Unknown extensions
            (".log", _models.LogStreamType.STDOUT),
            (".thing", _models.LogStreamType.STDOUT),
            # Passing the full filename is a mistake, but the behaviour is to treat
            # any unknown members as STDOUT.
            ("full_filename.out", _models.LogStreamType.STDOUT),
            ("full_filename.err", _models.LogStreamType.STDOUT),
        ),
    )
    def test_init(self, value: str, expected: _models.LogStreamType):
        log_stream_type = _models.LogStreamType(value)
        assert log_stream_type == expected

    @pytest.mark.parametrize(
        "path, expected",
        (
            (Path("filename.out"), _models.LogStreamType.STDOUT),
            (Path("filename.err"), _models.LogStreamType.STDERR),
            (Path("filename.log"), _models.LogStreamType.STDOUT),
            (Path("filename.thing"), _models.LogStreamType.STDOUT),
            (Path("noextension"), _models.LogStreamType.STDOUT),
            (
                Path("path", "with", "directories", "filename.out"),
                _models.LogStreamType.STDOUT,
            ),
            (
                Path("path", "with", "directories", "filename.err"),
                _models.LogStreamType.STDERR,
            ),
        ),
    )
    def test_by_file(self, path: Path, expected: _models.LogStreamType):
        log_stream_type = _models.LogStreamType.by_file(path)
        assert log_stream_type == expected


class TestLogAccumulator:
    class TestAddByFile:
        @pytest.fixture
        def mock_file(self):
            mock = create_autospec(Path)
            mock.read_text.return_value = "line 1\nline 2"
            return mock

        def test_out(self, monkeypatch: pytest.MonkeyPatch, mock_file):
            type(mock_file).suffix = PropertyMock(return_value=".out")
            logs = _models.LogAccumulator()
            add_lines_by_stream = Mock(spec=logs.add_lines_by_stream)
            monkeypatch.setattr(logs, "add_lines_by_stream", add_lines_by_stream)

            logs.add_lines_from_file(mock_file)

            add_lines_by_stream.assert_called_with(
                _models.LogStreamType.STDOUT, ["line 1", "line 2"]
            )

        def test_err(self, monkeypatch: pytest.MonkeyPatch, mock_file):
            type(mock_file).suffix = PropertyMock(return_value=".err")
            logs = _models.LogAccumulator()
            add_lines_by_stream = Mock(spec=logs.add_lines_by_stream)
            monkeypatch.setattr(logs, "add_lines_by_stream", add_lines_by_stream)

            logs.add_lines_from_file(mock_file)

            add_lines_by_stream.assert_called_with(
                _models.LogStreamType.STDERR, ["line 1", "line 2"]
            )

    class TestAddLineByStream:
        @pytest.fixture
        def log_line(self):
            return "some log line"

        def test_out(self, log_line: str):
            logs = _models.LogAccumulator()

            logs.add_line_by_stream(_models.LogStreamType.STDOUT, log_line)

            assert logs.out == [log_line]

        def test_err(self, log_line: str):
            logs = _models.LogAccumulator()

            logs.add_line_by_stream(_models.LogStreamType.STDERR, log_line)

            assert logs.err == [log_line]

        def test_append_to_out(self, log_line: str):
            logs = _models.LogAccumulator()

            logs.add_line_by_stream(_models.LogStreamType.STDOUT, log_line)
            logs.add_line_by_stream(_models.LogStreamType.STDOUT, log_line)

            assert logs.out == [log_line, log_line]

        def test_append_to_err(self, log_line: str):
            logs = _models.LogAccumulator()

            logs.add_line_by_stream(_models.LogStreamType.STDERR, log_line)
            logs.add_line_by_stream(_models.LogStreamType.STDERR, log_line)

            assert logs.err == [log_line, log_line]

    class TestAddLinesByStream:
        @pytest.fixture
        def log_lines(self):
            return ["line 1", "line 2"]

        def test_out(self, log_lines: List[str]):
            logs = _models.LogAccumulator()

            logs.add_lines_by_stream(_models.LogStreamType.STDOUT, log_lines)

            assert logs.out == log_lines

        def test_err(self, log_lines: List[str]):
            logs = _models.LogAccumulator()

            logs.add_lines_by_stream(_models.LogStreamType.STDERR, log_lines)

            assert logs.err == log_lines

        def test_append_to_out(self, log_lines: List[str]):
            logs = _models.LogAccumulator()

            logs.add_lines_by_stream(_models.LogStreamType.STDOUT, log_lines)
            logs.add_lines_by_stream(_models.LogStreamType.STDOUT, log_lines)

            assert logs.out == log_lines * 2

        def test_append_to_err(self, log_lines: List[str]):
            logs = _models.LogAccumulator()

            logs.add_lines_by_stream(_models.LogStreamType.STDERR, log_lines)
            logs.add_lines_by_stream(_models.LogStreamType.STDERR, log_lines)

            assert logs.err == log_lines * 2

    def test_access_empty_stdout(self):
        logs = _models.LogAccumulator()
        assert logs.out == []

    def test_access_empty_stderr(self):
        logs = _models.LogAccumulator()
        assert logs.err == []
