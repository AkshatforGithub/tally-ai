"""Pure normalisers: header, phone, date, decimal, enum, timestamp. No I/O."""
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

IST = timezone(timedelta(hours=5, minutes=30))

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_ISO = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})")
_DMY = re.compile(r"([0-9]{1,2})([/-])([0-9]{1,2})\2([0-9]{4})")
_DMON = re.compile(r"([0-9]{1,2})-([A-Za-z]{3})-([0-9]{4})")
_DEC = re.compile(r"(?:₹|rs\.?|inr)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)", re.IGNORECASE)


def clean_value(raw):
    """Trim; empty string means missing (None)."""
    if raw is None:
        return None
    s = raw.strip()
    return s or None


def norm_header(s):
    """Lowercase, trim, collapse runs of underscores/whitespace to one space."""
    return re.sub(r"[_\s]+", " ", s.lower()).strip()


def normalise_phone(raw):
    """Return '+91XXXXXXXXXX' or None if invalid."""
    if raw is None:
        return None
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if re.fullmatch(r"[6-9][0-9]{9}", digits):
        return "+91" + digits
    return None


def parse_date(raw):
    """YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY (day-first). None if invalid."""
    if raw is None:
        return None
    s = raw.strip()
    if m := _ISO.fullmatch(s):
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
    elif m := _DMY.fullmatch(s):
        d, mo, y = int(m[1]), int(m[3]), int(m[4])
    elif m := _DMON.fullmatch(s):
        mo = _MONTHS.get(m[2].lower())
        if mo is None:
            return None
        d, y = int(m[1]), int(m[3])
    else:
        return None
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def normalise_decimal(raw):
    """Optional rupee prefix, commas as thousands, >= 0, rounded to 2dp."""
    if raw is None:
        return None
    m = _DEC.fullmatch(raw.strip())
    if not m:
        return None
    value = Decimal(m[1].replace(",", ""))
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalise_enum(raw, values):
    """Case-insensitive match against allowed values; returns lower-case or None."""
    if raw is None:
        return None
    v = raw.strip().lower()
    return v if v in {x.lower() for x in values} else None


def parse_ts(raw):
    """Timestamp -> tz-aware datetime in IST. Naive input is treated as IST."""
    try:
        dt = datetime.fromisoformat(raw.strip())
    except (ValueError, AttributeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def ts_iso(dt):
    """ISO-8601 with IST offset, e.g. 2026-10-14T17:55:24+05:30."""
    return dt.isoformat(timespec="seconds")
