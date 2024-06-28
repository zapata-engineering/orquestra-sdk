################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################


import pytest

from orquestra.workflow_shared import _git_url_utils
from orquestra.workflow_shared.schema.ir import GitURL


class TestParseGitURL:
    @pytest.mark.parametrize(
        "url,expected",
        [
            (
                "https://user@example.com:3000/some/path.git?q=1",
                GitURL(
                    original_url="https://user@example.com:3000/some/path.git?q=1",
                    protocol="https",
                    user="user",
                    password=None,
                    host="example.com",
                    port=3000,
                    path="some/path.git",
                    query="q=1",
                ),
            ),
            (
                "git+ssh://user@example.com/some/path.git",
                GitURL(
                    original_url="git+ssh://user@example.com/some/path.git",
                    protocol="git+ssh",
                    user="user",
                    password=None,
                    host="example.com",
                    port=None,
                    path="some/path.git",
                    query=None,
                ),
            ),
            (
                "http://example.com",
                GitURL(
                    original_url="http://example.com",
                    protocol="http",
                    user=None,
                    password=None,
                    host="example.com",
                    port=None,
                    path="",
                    query=None,
                ),
            ),
            (
                "git@example.com:some/path.git",
                GitURL(
                    original_url="git@example.com:some/path.git",
                    protocol="ssh",
                    user="git",
                    password=None,
                    host="example.com",
                    port=None,
                    path="some/path.git",
                    query=None,
                ),
            ),
        ],
    )
    def test_parsing(self, url: str, expected: GitURL):
        parsed = _git_url_utils.parse_git_url(url)
        assert parsed == expected

    def test_with_password(self):
        url = "https://user:password@example.com/some/path.git"
        with pytest.raises(ValueError) as exc_info:
            _ = _git_url_utils.parse_git_url(url)
        exc_info.match("Passwords should not be stored in the URL directly")

    def test_missing_host(self):
        url = "https:///some/path.git"
        with pytest.raises(ValueError) as exc_info:
            _ = _git_url_utils.parse_git_url(url)
        exc_info.match("Could not parse the host for URL")

    def test_invalid_url(self):
        url = "::"
        with pytest.raises(ValueError) as exc_info:
            _ = _git_url_utils.parse_git_url(url)
        exc_info.match("Invalid URL format")
