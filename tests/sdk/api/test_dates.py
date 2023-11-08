################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._dates.py
"""

from datetime import datetime, timedelta, timezone

import pytest

from orquestra.sdk import SDKInstant


@pytest.fixture()
def my_instant() -> SDKInstant:
    return SDKInstant("2013-04-08T11:00+04:00")


class TestSDKInstant:
    class TestInitiation:
        @staticmethod
        def test_current_instant():
            my_instant = SDKInstant()
            now = datetime.now(timezone.utc)

            assert isinstance(my_instant, SDKInstant)
            assert (my_instant - now).total_seconds() < 1e-5

        @staticmethod
        @pytest.mark.parametrize(
            "iso_string, expected_datetime",
            [
                (
                    "2000-01-01T14:30Z",
                    datetime(2000, 1, 1, 14, 30, tzinfo=timezone.utc),
                ),
                (
                    "1312-01-01T11:00+00:00",
                    datetime(1312, 1, 1, 11, 0, tzinfo=timezone.utc),
                ),
                (
                    "1312-01-01T11:00+04:00",
                    datetime(
                        1312, 1, 1, 11, 0, tzinfo=timezone(timedelta(seconds=14400))
                    ),
                ),
            ],
        )
        def test_from_iso_formatted_string(
            iso_string: str, expected_datetime: datetime
        ):
            my_instant = SDKInstant(iso_string)

            assert my_instant == expected_datetime, (
                f"Expected {expected_datetime}, got {my_instant} "
                f"(difference of {expected_datetime - my_instant._datetime_object})"
            )

        @staticmethod
        @pytest.mark.parametrize(
            "timestamp, expected_datetime",
            [
                (-20764486800.0, datetime(1312, 1, 1, 7, 0, tzinfo=timezone.utc)),
            ],
        )
        def test_from_unix_timestamp(timestamp: float, expected_datetime: datetime):
            my_instant = SDKInstant(timestamp)

            assert my_instant == expected_datetime, (
                f"Expected {expected_datetime}, got {my_instant} "
                f"(difference of {expected_datetime - my_instant._datetime_object})"
            )

        @staticmethod
        @pytest.mark.parametrize(
            "datetime", [datetime(1312, 1, 1, 7, 0, tzinfo=timezone.utc)]
        )
        def test_from_datetime(datetime: datetime):
            my_instant = SDKInstant(datetime)

            assert my_instant == datetime, (
                f"Expected {datetime}, got {my_instant} "
                f"(difference of {datetime - my_instant._datetime_object})"
            )

        @staticmethod
        def test_from_local_comps():
            my_instant = SDKInstant.from_local_comps(
                1312, 1, 1, 14, 30, utc_hour_offset=4
            )

            assert my_instant == datetime(
                1312, 1, 1, 14, 30, tzinfo=timezone(timedelta(seconds=14400))
            )

        @staticmethod
        def test_from_utc_comps():
            my_instant = SDKInstant.from_utc_comps(
                1312,
                1,
                1,
                14,
                30,
            )

            assert my_instant == datetime(1312, 1, 1, 14, 30, tzinfo=timezone.utc)

    class TestFormatting:
        @staticmethod
        def test_iso_formatting(my_instant: SDKInstant):
            assert my_instant.isoformat() == "2013-04-08T11:00:00+04:00"

        @staticmethod
        def test_local_iso_formatting(my_instant: SDKInstant):
            assert my_instant.local_isoformat() == "2013-04-08T08:00:00+01:00"

        @staticmethod
        def test_unix_formatting(my_instant: SDKInstant):
            assert my_instant.timestamp() == 1365404400.0

    class TestFailureStates:
        @staticmethod
        def test_initialising_from_timezone_unaware_datetime_raises_exception():
            with pytest.raises(ValueError) as e:
                SDKInstant(datetime(1312, 1, 1, 7, 0))
            assert e.exconly() == (
                "ValueError: We only work with timezone-aware datetimes"
            )

        @staticmethod
        def test_initialising_from_unsupported_type_raises_exception():
            with pytest.raises(NotImplementedError) as e:
                SDKInstant({})
            assert e.exconly() == (
                "NotImplementedError: Cannot initialise SDKInstant from type <class 'dict'>"  # noqa: E501
            )

    class TestInteractionWithDatetime:
        @staticmethod
        def test_subtract_datetime_from_instant(my_instant: SDKInstant):
            dt = my_instant - datetime(2000, 1, 1, 14, 30, tzinfo=timezone.utc)

            assert dt == timedelta(days=4845, hours=16, minutes=30)

        @staticmethod
        def test_subtract_timedelta_from_instant(my_instant: SDKInstant):
            dt = my_instant - timedelta(days=14, hours=15, minutes=9)

            assert isinstance(dt, SDKInstant)
            assert dt == SDKInstant("2013-03-24T19:51+04:00")

    class TestSelfInteraction:
        @staticmethod
        def test_subtraction(my_instant: SDKInstant):
            dt = my_instant - my_instant

            assert isinstance(dt, timedelta)
            assert dt == timedelta(0)
