################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Utilities to handle datetimes and timezones without shooting yourself in the foot."""

import typing as t
from datetime import datetime, timedelta, timezone


class SDKInstant:
    """Wrapper around datetime.datetime that provides our custom datetime utilities."""

    def __init__(self, base: t.Optional[str] = None):
        self._datetime_object: datetime
        if base is None:
            self._datetime_object = datetime.now(timezone.utc)
        elif isinstance(base, str):
            self._datetime_object = datetime.fromisoformat(base.replace("Z", "+00:00"))
        elif isinstance(base, (int, float)):
            self._datetime_object = datetime.fromtimestamp(base, timezone.utc)
        elif isinstance(base, datetime):
            self._datetime_object = base
        else:
            raise NotImplementedError(
                f"Cannot initialise SDKInstant from type {type(base)}"
            )
        self._enforce_timezone_aware()

    def __str__(self):
        return self._datetime_object.__str__()

    def __eq__(self, *args, **kwargs):
        return self._datetime_object.__eq__(*args, **kwargs)

    def _enforce_timezone_aware(self):
        """Enforce the requirement that the Instant includes timezone information.

        Raises:
            ValueError: when the Instant is not timezone-aware.
        """
        if self._datetime_object.tzinfo is None:
            raise ValueError("We only work with timezone-aware datetimes")

    def isoformat(self) -> str:
        """Formats the instant using ISO8601 format with explicit time zone."""
        self._enforce_timezone_aware()
        return self._datetime_object.isoformat()

    def local_isoformat(self) -> str:
        """Formats the instant using ISO8601 format with explicit time zone.

        The instant is shifted to a local timezone for human-friendliness.
        """
        self._enforce_timezone_aware()
        return self._datetime_object.astimezone().isoformat()


# Timezone-aware datetime. Represents an unambiguous time instant.
Instant = t.NewType("Instant", datetime)


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
