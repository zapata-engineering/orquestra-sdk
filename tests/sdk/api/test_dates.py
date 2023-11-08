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


class TestInitiation:
    @staticmethod
    @freeze_time("1312-01-01", tz_offset=4)
    def test_current_instant():
        my_instant = SDKInstant()

        assert isinstance(my_instant, SDKInstant)
        assert my_instant == datetime.now(timezone.utc)
        assert str(my_instant) == "1312-01-01 04:00:00+00:00"

    @staticmethod
    @freeze_time("1312-01-01", tz_offset=4)
    @pytest.mark.parametrize(
        "iso_string, expected_datetime",
        [
            ("2007-04-05T14:30Z", datetime(2007, 4, 5, 14, 30, tzinfo=timezone.utc)),
            (
                "1312-01-01T11:00+00:00",
                datetime(1312, 1, 1, 11, 0, tzinfo=timezone.utc),
            ),
            (
                "1312-01-01T11:00+04:00",
                datetime(1312, 1, 1, 11, 0, tzinfo=timezone(timedelta(seconds=14400))),
            ),
        ],
    )
    def test_from_iso_formatted_string(iso_string, expected_datetime):
        my_instant = SDKInstant(iso_string)

        assert my_instant == expected_datetime, my_instant
