################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import re
from typing import Optional

from urllib3.exceptions import LocationParseError
from urllib3.util import parse_url

from .. import secrets
from ..schema.ir import GitURL


def _split_auth(auth_value: Optional[str]):
    if auth_value is None:
        return None, None

    split_auth = auth_value.split(":")

    if len(split_auth) == 1:
        return split_auth[0], None
    else:
        return split_auth[0], ":".join(split_auth[1:])


def build_git_url(url: GitURL, protocol_override: Optional[str] = None) -> str:
    """Returns a usable string from a GitURL.

    This will get the password from the secrets API, if required.

    Args:
        url: the GitURL to build the URL string from
        protocol_override: Ignore the protocol defined in the URL and build the URL
            using a different protocol.

    Raises:
        ValueError: when the protocol is not recognised.
    """
    protocol = protocol_override or url.protocol
    user = url.user or "git"
    port = f":{url.port}" if url.port is not None else ""

    # Dereference secret used as password
    if url.password is not None:
        secret = secrets.get(
            url.password.secret_name,
            config_name=url.password.secret_config,
            workspace_id=url.password.workspace_id,
        )
        password = f":{secret}"
    else:
        password = ""

    if protocol == "ssh":
        if url.port is None:
            return f"{user}@{url.host}:{url.path}"
        else:
            return f"ssh://{user}{password}@{url.host}{port}/{url.path}"
    elif protocol in ("git+ssh", "ssh+git", "ftp", "ftps"):
        return f"{protocol}://{user}{password}@{url.host}{port}/{url.path}"
    elif protocol in ("http", "https", "git+https", "git+http"):
        if url.user is None and url.password is None:
            return f"{protocol}://{url.host}{port}/{url.path}"
        else:
            return f"{protocol}://{user}{password}@{url.host}{port}/{url.path}"
    else:
        if protocol == url.protocol:
            return url.original_url
        else:
            raise ValueError(f"Unknown protocol: `{protocol}`")


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
