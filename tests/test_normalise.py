from datetime import date

import pytest

from solution.normalise import (
    clean_value, norm_header, normalise_decimal, normalise_enum,
    normalise_phone, parse_date, parse_ts, ts_iso,
)


@pytest.mark.parametrize("raw,expected", [
    ("+916463745499", "+916463745499"),
    ("9932302346", "+919932302346"),
    ("+91 79328 96340", "+917932896340"),
    ("(+91) 9357136187", "+919357136187"),
    ("91-77187-50036", "+917718750036"),
    ("+9180331 73936", "+918033173936"),
    ("09932302346", "+919932302346"),
    ("5932302346", None),
    ("12345", None),
    ("19932302346", None),
    ("449932302346", None),
    ("", None),
    (None, None),
])
def test_phone(raw, expected):
    assert normalise_phone(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("21-11-2026", date(2026, 11, 21)),
    ("27/10/2026", date(2026, 10, 27)),
    ("2026-11-23", date(2026, 11, 23)),
    ("05/11/2026", date(2026, 11, 5)),
    ("13-01-2026", date(2026, 1, 13)),
    ("05-Nov-2026", date(2026, 11, 5)),
    ("5-nov-2026", date(2026, 11, 5)),
    ("2026-13-01", None),
    ("31/02/2026", None),
    ("11/31/2026", None),
    ("2026/11/23", None),
    ("05/11-2026", None),
    ("", None),
    (None, None),
])
def test_date(raw, expected):
    assert parse_date(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("₹27,554", "27554.00"),
    ("6850", "6850.00"),
    ("INR 25301.4", "25301.40"),
    ("Rs. 1,000", "1000.00"),
    ("rs 99.999", "100.00"),
    ("-5", None),
    ("abc", None),
    ("₹", None),
    ("", None),
    (None, None),
])
def test_decimal(raw, expected):
    got = normalise_decimal(raw)
    assert (None if got is None else str(got)) == expected


@pytest.mark.parametrize("raw,expected", [
    ("MR", "mr"), (" hi ", "hi"), ("Hindi", None), ("", None), (None, None),
])
def test_enum(raw, expected):
    assert normalise_enum(raw, ["en", "hi", "ta", "kn", "te", "mr"]) == expected


@pytest.mark.parametrize("raw,expected", [
    ("Mobile No", "mobile no"),
    ("customer_name", "customer name"),
    ("  Order   ID ", "order id"),
    ("agent_notes", "agent notes"),
    ("__Policy__No_", "policy no"),
])
def test_header(raw, expected):
    assert norm_header(raw) == expected


def test_clean_value():
    assert clean_value("  x ") == "x"
    assert clean_value("   ") is None
    assert clean_value(None) is None


def test_timestamps_become_ist():
    assert ts_iso(parse_ts("2026-10-01T05:26:10Z")) == "2026-10-01T10:56:10+05:30"
    assert ts_iso(parse_ts("2026-10-01T10:40:19+05:30")) == "2026-10-01T10:40:19+05:30"
    assert ts_iso(parse_ts("2026-10-01 10:40:19")) == "2026-10-01T10:40:19+05:30"


def test_ist_date_differs_from_utc_date():
    assert parse_ts("2026-10-01T20:00:00Z").date() == date(2026, 10, 2)


def test_bad_timestamp():
    assert parse_ts("garbage") is None
    assert parse_ts(None) is None
