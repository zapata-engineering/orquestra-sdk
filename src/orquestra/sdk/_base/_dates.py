################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Single place to handle datetimes and timezones without shooting yourself in the
foot.
"""

from datetime import datetime, timezone


def now() -> datetime:
    """
    Generates a timezone-aware current instant. The timezone is set to UTC.
    """
    return datetime.now(timezone.utc)


def local_isoformat(instant: datetime) -> str:
    """
    Formats the instant using ISO8601 format with explicit time zone. The instant is
    shifted to a local timezone for human-friendliness.
    """
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    local_instant = instant.astimezone()
    return local_instant.isoformat()


def from_isoformat(formatted: str) -> datetime:
    instant = datetime.fromisoformat(formatted.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    return instant


def unix_time(instant: datetime) -> float:
    """
    Generates a timezone-aware datetime object from a UNIX epoch timestamp (UTC seconds
    since 1970).
    """
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")
    return instant.timestamp()


def from_unix_time(epoch_seconds: float) -> datetime:
    """
    Parses a unix epoch timestamp (UTC seconds since 1970) into a
    timezone-aware datetime object.
    """
    return datetime.fromtimestamp(epoch_seconds, timezone.utc)
