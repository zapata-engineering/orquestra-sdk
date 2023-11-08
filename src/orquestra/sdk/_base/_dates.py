################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Utilities to handle datetimes and timezones without shooting yourself in the foot."""

import typing as t
from datetime import datetime, timedelta, timezone


class SDKInstant(datetime):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            self = super().now(timezone.utc)
        elif len(args) > 1:
            self = super().__new__(cls, *args, **kwargs)
        elif isinstance(args[0], str):
            self = super().fromisoformat(args[0].replace("Z", "+00:00"))
        elif isinstance(args[0], (int, float)):
            self = super().fromtimestamp(args[0], timezone.utc)
        elif isinstance(args[0], datetime):
            self = super().__new__(
                cls,
                args[0].year,
                args[0].month,
                args[0].day,
                args[0].hour,
                args[0].minute,
                args[0].second,
                args[0].microsecond,
                args[0].tzinfo,
            )
        else:
            raise NotImplementedError(
                f"Cannot initialise SDKInstant from type {type(args[0])}"
            )
        self._enforce_timezone_aware()
        return self

    def _enforce_timezone_aware(self):
        """Enforce the requirement that the Instant includes timezone information.

        Raises:
            ValueError: when the Instant is not timezone-aware.
        """
        if self.tzinfo is None:
            raise ValueError("We only work with timezone-aware datetimes")

    @classmethod
    def from_local_comps(cls, *args, utc_hour_offset: int, **kwargs) -> "SDKInstant":
        """Builds an instant from from timezone-local date components.

        Uses the same components as ``datetime.datetime(...)``, apart from ``tzinfo``.
        """
        new_kwargs: t.Dict[str, t.Any] = {
            **kwargs,
            "tzinfo": timezone(timedelta(hours=utc_hour_offset)),
        }

        return cls(datetime(*args, **new_kwargs))

    @classmethod
    def from_utc_comps(cls, *args, **kwargs) -> "SDKInstant":
        """Builds an timezone-aware instant from specific date components.

        The components should be in UTC.
        """
        new_kwargs = {**kwargs, "utc_hour_offset": 0}
        return cls.from_local_comps(*args, **new_kwargs)

    def local_isoformat(self) -> str:
        """Formats the instant using ISO8601 format with explicit time zone.

        The instant is shifted to a local timezone for human-friendliness.
        """
        self._enforce_timezone_aware()
        return self.astimezone().isoformat()


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
