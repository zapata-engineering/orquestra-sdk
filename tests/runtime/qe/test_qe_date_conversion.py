################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import datetime

import pytest

from orquestra.sdk._base._dates import Instant
from orquestra.sdk._base._qe._qe_runtime import parse_date_or_none


def test_with_none_date_str():
    assert parse_date_or_none(None) is None


def test_with_empty_date_str():
    assert parse_date_or_none("") is None


@pytest.mark.parametrize(
    "date_str",
    [
        # Generic dates
        "2012",
        "1991-02",
        "1971-11-03",
        # MM/DD/YYYY
        "07/04/1776",
        "08/24/1814",
        # Generic datetimes
        "2012 00:00",
        "1991-02 00:00:00",
        "1971-11-03 12:00",
        # MM/DD/YYYY ...
        "07/04/1776 00:00:00",
        "08/24/1814 13:37:00",
        # Almost RFC3339
        "2000/01/01T00:00:00Z",
        "2000/01/01 00:00:00Z",
        "2000/01/01 00:00Z",
        "2000/01/01T00:00Z",
        "2000-01-01T00:00Z",
        # Random strings
        "hello there",
        "ZZZZ",
    ],
)
def test_with_invalid_date_str(date_str: str):
    with pytest.raises(ValueError):
        parse_date_or_none(date_str)


def _utc_instant(*args) -> Instant:
    return Instant(datetime.datetime(*args, tzinfo=datetime.timezone.utc))


@pytest.mark.parametrize(
    "date_str, expected",
    [
        # RFC3339
        (
            "2006-01-02T15:04:05Z",
            _utc_instant(2006, 1, 2, 15, 4, 5, 0),
        ),
        (
            "1989-12-13T00:00:00Z",
            _utc_instant(1989, 12, 13, 0, 0, 0, 0),
        ),
        # RFC3339Nano
        (
            "2006-01-02T15:04:05.999999999Z",
            _utc_instant(2006, 1, 2, 15, 4, 5, 999999),
        ),
        (
            "2006-01-02T15:04:05.999999Z",
            _utc_instant(2006, 1, 2, 15, 4, 5, 999999),
        ),
        (
            "1989-12-13T00:00:00.198922Z",
            _utc_instant(1989, 12, 13, 0, 0, 0, 198922),
        ),
    ],
)
def test_with_zulu(date_str: str, expected: Instant):
    assert parse_date_or_none(date_str) == expected


@pytest.mark.parametrize(
    "date_str, expected_exception",
    [
        # RFC3339
        ("2006-01-02T15:04:05Z07:00", ValueError),
        ("1989-12-13T00:00:00Z-05:00", ValueError),
        # RFC3339Nano
        ("2006-01-02T15:04:05.999999999Z07:00", ValueError),
        ("2006-01-02T15:04:05.999999Z05:30", ValueError),
        ("1989-12-13T00:00:00.999999999Z-05:00", ValueError),
    ],
)
def test_with_timezones(date_str: str, expected_exception):
    with pytest.raises(expected_exception):
        parse_date_or_none(date_str)
