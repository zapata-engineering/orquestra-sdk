################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Utilities to handle datetimes and timezones without shooting yourself in the foot."""

import typing as t
from datetime import datetime, timedelta, timezone

# Timezone-aware datetime. Represents an unambiguous time instant.
Instant = t.NewType("Instant", datetime)


def now() -> Instant:
    """Generates a timezone-aware current instant. The timezone is set to UTC."""
    return Instant(datetime.now(timezone.utc))


def isoformat(instant: Instant) -> str:
    """Formats the instant using ISO8601 format with explicit time zone."""
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    return instant.isoformat()


def local_isoformat(instant: Instant) -> str:
    """Formats the instant using ISO8601 format with explicit time zone.

    The instant is shifted to a local timezone for human-friendliness.
    """
    # We need to check it as soon as possible. `.astimezone()` overrides empty tzinfo
    # with local time zone.
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    local_instant = instant.astimezone()
    return isoformat(local_instant)


def from_isoformat(formatted: str) -> Instant:
    instant = datetime.fromisoformat(formatted.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    return Instant(instant)


def unix_time(instant: Instant) -> float:
    """Generates a timezone-aware datetime object from a UNIX epoch timestamp.

    (Unix epoch timestamp is UTC seconds since 1970)
    """
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")
    return instant.timestamp()


def from_unix_time(epoch_seconds: float) -> Instant:
    """Parses a unix epoch timestamp into a timezone-aware datetime object.

    (Unix epoch timestamp is UTC seconds since 1970)
    """
    return Instant(datetime.fromtimestamp(epoch_seconds, timezone.utc))


def from_comps(
    *args,
    utc_hour_offset,
    **kwargs,
) -> Instant:
    """Builds an instant from from timezone-local date components.

    Uses the same components as ``datetime.datetime(...)``, apart from ``tzinfo``.
    """
    new_kwargs: t.Dict[str, t.Any] = {
        **kwargs,
        "tzinfo": timezone(timedelta(hours=utc_hour_offset)),
    }

    return Instant(datetime(*args, **new_kwargs))


def utc_from_comps(*args, **kwargs) -> Instant:
    """Builds an timezone-aware instant from specific date components.

    The components should be in UTC.
    """
    new_kwargs = {**kwargs, "utc_hour_offset": 0}
    return from_comps(*args, **new_kwargs)
