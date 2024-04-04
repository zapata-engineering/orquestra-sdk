################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Unit tests for ``orquestra.sdk._base._dates``.
"""

import re
from datetime import datetime, timedelta, timezone

import pytest

from orquestra.sdk._client._base import _dates


class TestNow:
    @staticmethod
    def test_is_tz_aware():
        # When
        instant = _dates.now()

        # Then
        assert instant.tzinfo is not None


class TestFromISOFormat:
    @staticmethod
    @pytest.mark.parametrize("formatted", ["2011-11-04T00:05:23"])
    def test_invalid(formatted: str):
        # Then
        with pytest.raises(ValueError):
            # When
            _ = _dates.from_isoformat(formatted)

    @staticmethod
    @pytest.mark.parametrize(
        "formatted",
        [
            "2011-11-04T00:05:23+04:00",
            "2011-11-04T00:05:23Z",
        ],
    )
    def test_has_timezone(formatted: str):
        # When
        instant = _dates.from_isoformat(formatted)

        # Then
        assert instant.tzinfo is not None


class TestISOFormat:
    @staticmethod
    def test_tz_naive():
        # Given
        dt = datetime.fromtimestamp(1687528083)
        # Instant is supposed to represent only timezone-aware datetimes. This test case
        # deliberately uses timezone-naive object. Casting it to Instant anyway prevents
        # typechecker errors.
        instant = _dates.Instant(dt)

        # Then
        with pytest.raises(ValueError):
            # When
            _ = _dates.isoformat(instant)

    @staticmethod
    def test_utc_input():
        # Given
        instant = _dates.Instant(datetime.fromtimestamp(1687528083, timezone.utc))

        # When
        formatted = _dates.isoformat(instant)

        # Then
        assert formatted == "2023-06-23T13:48:03+00:00"

    @staticmethod
    def test_non_utc_input():
        # Given
        instant = _dates.Instant(
            datetime.fromtimestamp(1687528083, timezone(timedelta(hours=-4)))
        )

        # When
        formatted = _dates.isoformat(instant)

        # Then
        assert formatted == "2023-06-23T09:48:03-04:00"


class TestLocalISOFormat:
    @staticmethod
    def test_tz_naive():
        # Given
        dt = datetime.fromtimestamp(1687528083)
        # Instant is supposed to represent only timezone-aware datetimes. This test case
        # deliberately uses timezone-naive object. Casting it to Instant anyway prevents
        # typechecker errors.
        instant = _dates.Instant(dt)

        # Then
        with pytest.raises(ValueError):
            # When
            _ = _dates.local_isoformat(instant)

    @staticmethod
    def test_utc_input():
        # Given
        instant = _dates.Instant(datetime.fromtimestamp(1687528083, timezone.utc))

        # When
        formatted = _dates.local_isoformat(instant)

        # Then
        # The output depends on the local time zone where the tests are run. We can work
        # around this by regexes.
        assert re.match(r"2023-06-23T\d\d:\d8:03[\+-]\d\d:\d0", formatted)

    @staticmethod
    def test_non_utc_input():
        # Given
        instant = _dates.Instant(
            datetime.fromtimestamp(1687528083, timezone(timedelta(hours=-4)))
        )

        # When
        formatted = _dates.local_isoformat(instant)

        # Then
        # The output depends on the local time zone where the tests are run. We can work
        # around this by regexes.
        assert re.match(r"2023-06-23T\d\d:\d8:03[+-]\d\d:\d0", formatted)


class TestUnixTime:
    @staticmethod
    def test_tz_naive():
        # Given
        dt = datetime.fromtimestamp(1687528083)
        # Instant is supposed to represent only timezone-aware datetimes. This test case
        # deliberately uses timezone-naive object. Casting it to Instant anyway prevents
        # typechecker errors.
        instant = _dates.Instant(dt)

        # Then
        with pytest.raises(ValueError):
            # When
            _ = _dates.unix_time(instant)


class TestFromUnixTime:
    @staticmethod
    def test_has_timezone():
        # When
        instant = _dates.from_unix_time(1687528083)

        # Then
        assert instant.tzinfo is not None

    @staticmethod
    def test_roundtrip():
        # Given
        ts1 = 1687528083
        instant = _dates.from_unix_time(ts1)

        # When
        ts2 = _dates.unix_time(instant)

        # Then
        assert ts2 == ts1


class TestFromComps:
    @staticmethod
    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            pytest.param(
                {"year": 1964, "month": 9, "day": 2, "utc_hour_offset": 3},
                "1964-09-02T00:00:00+03:00",
                id="minimal-args",
            ),
            pytest.param(
                {
                    "year": 2005,
                    "month": 4,
                    "day": 2,
                    "hour": 21,
                    "minute": 36,
                    "second": 59,
                    "utc_hour_offset": 1,
                },
                "2005-04-02T21:36:59+01:00",
                id="full-printable",
            ),
        ],
    )
    def test_against_isoformat(kwargs, expected):
        # When
        instant = _dates.from_comps(**kwargs)
        formatted = _dates.isoformat(instant)

        # Then
        assert formatted == expected


class TestUTCFromComps:
    @staticmethod
    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            pytest.param(
                {"year": 1964, "month": 9, "day": 2},
                "1964-09-02T00:00:00+00:00",
                id="minimal-args",
            ),
            pytest.param(
                {
                    "year": 2005,
                    "month": 4,
                    "day": 2,
                    "hour": 20,
                    "minute": 36,
                    "second": 59,
                },
                "2005-04-02T20:36:59+00:00",
                id="full-printable",
            ),
        ],
    )
    def test_against_isoformat(kwargs, expected):
        # When
        instant = _dates.utc_from_comps(**kwargs)
        formatted = _dates.isoformat(instant)

        # Then
        assert formatted == expected
