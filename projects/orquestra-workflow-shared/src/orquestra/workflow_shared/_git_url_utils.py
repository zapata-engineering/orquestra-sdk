################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import re
from typing import Optional

from urllib3.exceptions import LocationParseError
from urllib3.util import parse_url

from orquestra.workflow_shared.schema.ir import GitURL


def _split_auth(auth_value: Optional[str]):
    if auth_value is None:
        return None, None

    split_auth = auth_value.split(":")

    if len(split_auth) == 1:
        return split_auth[0], None
    else:
        return split_auth[0], ":".join(split_auth[1:])


def parse_git_url(url: str) -> GitURL:
    """Parse a git URL from a string.

    Accepted formats:

    * ``<protocol>://[<user>@]<host>[:<port>]/<path>[?<query>]``
    * ``<user>@<host>:<path>``
    * ``ssh://<user>@<host>:<repo>``
    """
    try:
        parsed = parse_url(url)
        protocol = parsed.scheme
        user, password = _split_auth(parsed.auth)
        host = parsed.host
        port = parsed.port
        query = parsed.query
        if parsed.path is not None:
            path = parsed.path.lstrip("/")
        else:
            path = ""
    except LocationParseError as e:
        _url = url.strip("ssh://")
        m = re.match(
            r"(?P<user>.+)@(?P<domain>[^/]+?):(?P<repo>.+)", _url, re.IGNORECASE
        )
        if m is None:
            raise ValueError(f"Invalid URL format: `{url}`") from e
        protocol = "ssh"
        user, password = _split_auth(m.group("user"))
        host = m.group("domain")
        port = None
        path = m.group("repo")
        query = None

    if host is None:
        raise ValueError(f"Could not parse the host for URL `{url}`")

    if password is not None:
        raise ValueError("Passwords should not be stored in the URL directly")

    return GitURL(
        original_url=url,
        protocol=protocol or "https",
        user=user,
        password=None,
        host=host,
        port=port,
        path=path,
        query=query,
    )
