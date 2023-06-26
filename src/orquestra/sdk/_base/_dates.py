################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Single place to handle datetimes and timezones without shooting yourself in the
foot.
"""

import typing as t
from datetime import datetime, timedelta, timezone

# Timezone-aware datetime. Represents an unambiguous time instant.
Instant = t.NewType("Instant", datetime)


def now() -> Instant:
    """
    Generates a timezone-aware current instant. The timezone is set to UTC.
    """
    return Instant(datetime.now(timezone.utc))


def local_isoformat(instant: datetime) -> str:
    """
    Formats the instant using ISO8601 format with explicit time zone. The instant is
    shifted to a local timezone for human-friendliness.
    """
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    local_instant = instant.astimezone()
    return local_instant.isoformat()


def from_isoformat(formatted: str) -> Instant:
    instant = datetime.fromisoformat(formatted.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")

    return Instant(instant)


def unix_time(instant: Instant) -> float:
    """
    Generates a timezone-aware datetime object from a UNIX epoch timestamp (UTC seconds
    since 1970).
    """
    if instant.tzinfo is None:
        raise ValueError("We only work with timezone-aware datetimes")
    return instant.timestamp()


def from_unix_time(epoch_seconds: float) -> Instant:
    """
    Parses a unix epoch timestamp (UTC seconds since 1970) into a
    timezone-aware datetime object.
    """
    return Instant(datetime.fromtimestamp(epoch_seconds, timezone.utc))


def from_comps(
    year,
    month,
    day,
    hour,
    minute,
    second,
    microsecond,
    utc_hour_offset: int,
) -> Instant:
    """
    Builds an instant from from timezone-local date components.
    """
    # I wanted to use `*args` instead of enumerating all the components one-by-one but I
    # couldn't appease mypy errors.
    return Instant(
        datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            tzinfo=timezone(timedelta(hours=utc_hour_offset)),
        )
    )


def utc_from_comps(*args, **kwargs) -> Instant:
    """
    Builds an timezone-aware instant from specific date components. The components
    should be in UTC.
    """
    new_kwargs = {**kwargs, "utc_hour_offset": 0}
    return from_comps(*args, **new_kwargs)
