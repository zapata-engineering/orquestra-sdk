from unittest.mock import create_autospec

import pytest

import orquestra.sdk.secrets
from orquestra.sdk._base import _git_url_utils
from orquestra.sdk.schema.ir import GitURL, SecretNode


@pytest.fixture
def git_url() -> GitURL:
    return GitURL(
        original_url="https://github.com/zapatacomputing/orquestra-workflow-sdk",
        protocol="https",
        user=None,
        password=None,
        host="github.com",
        port=None,
        path="zapatacomputing/orquestra-workflow-sdk",
        query=None,
    )


class TestBuildGitURL:
    @pytest.mark.parametrize(
        "protocol,expected_url",
        [
            (
                "git+ssh",
                "git+ssh://git@github.com/zapatacomputing/orquestra-workflow-sdk",
            ),
            (
                "ssh+git",
                "ssh+git://git@github.com/zapatacomputing/orquestra-workflow-sdk",
            ),
            ("ftp", "ftp://git@github.com/zapatacomputing/orquestra-workflow-sdk"),
            ("ftps", "ftps://git@github.com/zapatacomputing/orquestra-workflow-sdk"),
            ("http", "http://github.com/zapatacomputing/orquestra-workflow-sdk"),
            ("https", "https://github.com/zapatacomputing/orquestra-workflow-sdk"),
            (
                "git+http",
                "git+http://github.com/zapatacomputing/orquestra-workflow-sdk",
            ),
            (
                "git+https",
                "git+https://github.com/zapatacomputing/orquestra-workflow-sdk",
            ),
        ],
    )
    def test_different_protocols(
        self, git_url: GitURL, protocol: str, expected_url: str
    ):
        url = _git_url_utils.build_git_url(git_url, protocol)
        assert url == expected_url

    def test_ssh_with_port(self, git_url: GitURL):
        git_url.port = 22
        url = _git_url_utils.build_git_url(git_url, "ssh")
        assert url == "ssh://git@github.com:22/zapatacomputing/orquestra-workflow-sdk"

    @pytest.mark.parametrize(
        "protocol",
        ["http", "https", "git+http", "git+https"],
    )
    def test_http_with_user(self, git_url: GitURL, protocol: str):
        git_url.user = "amelio_robles_avila"
        url = _git_url_utils.build_git_url(git_url, protocol)
        assert url == (
            f"{protocol}://amelio_robles_avila@github.com"
            "/zapatacomputing/orquestra-workflow-sdk"
        )

    def test_uses_default_protocol(self, git_url: GitURL):
        url = _git_url_utils.build_git_url(git_url)
        assert url == "https://github.com/zapatacomputing/orquestra-workflow-sdk"

    def test_with_password(self, monkeypatch: pytest.MonkeyPatch, git_url: GitURL):
        secrets_get = create_autospec(orquestra.sdk.secrets.get)
        secrets_get.return_value = "<mocked secret>"
        monkeypatch.setattr(orquestra.sdk.secrets, "get", secrets_get)

        secret_name = "my_secret"
        secret_config = "secret config"
        git_url.password = SecretNode(
            id="mocked secret", secret_name=secret_name, secret_config=secret_config
        )

        url = _git_url_utils.build_git_url(git_url)
        assert url == (
            "https://git:<mocked secret>@github.com"
            "/zapatacomputing/orquestra-workflow-sdk"
        )
        secrets_get.assert_called_once_with(secret_name, config_name=secret_config)

    def test_unknown_protocol_in_original(self, git_url: GitURL):
        git_url.original_url = "custom_protocol://<blah>"
        git_url.protocol = "custom_protocol"
        url = _git_url_utils.build_git_url(git_url)
        assert url == git_url.original_url

    def test_unknown_protocol_override(self, git_url: GitURL):
        with pytest.raises(ValueError) as exc_info:
            _ = _git_url_utils.build_git_url(git_url, "custom_protocol")
        exc_info.match("Unknown protocol: `custom_protocol`")


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
