################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._dates.py
"""

from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from orquestra.sdk import SDKInstant


class TestSDKInstant:
    class TestInitiation:
        @staticmethod
        @freeze_time("1312-01-01", tz_offset=4)
        def test_current_instant():
            my_instant = SDKInstant()
            now = datetime.now(timezone.utc)

            assert isinstance(my_instant, SDKInstant)
            assert my_instant == now, (
                f"Expected {now}, got {my_instant} "
                f"(difference of {now - my_instant._datetime_object})"
            )
            assert str(my_instant) == "1312-01-01 04:00:00+00:00"

        @staticmethod
        @pytest.mark.parametrize(
            "iso_string, expected_datetime",
            [
                (
                    "2007-04-05T14:30Z",
                    datetime(2007, 4, 5, 14, 30, tzinfo=timezone.utc),
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
