################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._dates.py
"""

from datetime import datetime, timezone

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
