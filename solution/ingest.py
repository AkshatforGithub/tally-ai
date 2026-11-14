"""Ingest lead CSVs and event JSONL files. Each file is loaded in one transaction and
recorded in ingested_files, so re-running ingest on a drop changes nothing."""
import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .db import phone_hash
from .normalise import (
    clean_value, norm_header, normalise_decimal, normalise_enum, normalise_phone,
    parse_date, parse_ts,
)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MICRO = timedelta(microseconds=1)


class FileChanged(Exception):
    """A file that was ingested before now has different content."""


# ---------------------------------------------------------------- leads

def _normalise_field(field, raw):
    kind = field["type"]
    if kind == "phone":
        return normalise_phone(raw)
    if kind == "date":
        d = parse_date(raw)
        return d.isoformat() if d else None
    if kind == "decimal":
        v = normalise_decimal(raw)
        return str(v) if v is not None else None
    if kind == "enum":
        return normalise_enum(raw, field["values"])
    return raw


def _cells(row):
    for key, value in row.items():
        if key is None:                    # extra cells beyond the header
            yield from value
        else:
            yield value


def process_row(row, schema):
    """Walk the schema in order. Returns ('rejected', reason) or ('ok', values, warnings)."""
    cells = {norm_header(h): clean_value(v) for h, v in row.items()
             if h is not None and (v is None or isinstance(v, str))}
    values, warnings = {}, 0
    for field in schema["fields"]:
        raw = None
        for name in [field["name"], *field.get("aliases", [])]:
            raw = cells.get(norm_header(name))
            if raw is not None:
                break
        if raw is None:
            if field.get("required"):
                return ("rejected", f"missing_required:{field['name']}")
            values[field["name"]] = None
            continue
        value = _normalise_field(field, raw)
        if value is None:
            if field["type"] == "phone":
                return ("rejected", "invalid_phone")
            warnings += 1                  # present but invalid -> null + one warning
        values[field["name"]] = value
    return ("ok", values, warnings)


def _ingest_lead_file(conn, file_key, path, schema, key):
    wf = schema["workflow_id"]
    batch_id = Path(path).stem
    phone_field = next(f["name"] for f in schema["fields"] if f["type"] == "phone")
    ref_field = next(f["name"] for f in schema["fields"] if f.get("reference"))
    pii = {f["name"] for f in schema["fields"] if f.get("pii") or f["type"] == "phone"}
    defaults = {f["name"]: f["default"] for f in schema["fields"] if "default" in f}
    stats = {"rows": 0, "rejected": 0, "by_reason": {}, "merged": 0, "warnings": 0}

    with open(path, newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            if all(clean_value(c) is None for c in _cells(row)):
                continue                                   # blank row: skipped, not counted
            stats["rows"] += 1
            result = process_row(row, schema)
            if result[0] == "rejected":
                reason = result[1]
                stats["rejected"] += 1
                stats["by_reason"][reason] = stats["by_reason"].get(reason, 0) + 1
                conn.execute("INSERT OR IGNORE INTO rejections VALUES (?, ?, ?, ?, ?)",
                             (file_key, wf, batch_id, stats["rows"], reason))
                continue

            _, values, warnings = result
            stats["warnings"] += warnings
            canonical = values[phone_field]
            digest = phone_hash(canonical, key)
            new_attrs = {k: v for k, v in values.items() if k not in pii}
            found = conn.execute(
                "SELECT attrs FROM leads WHERE workflow_id = ? AND phone_hash = ?",
                (wf, digest)).fetchone()
            if found is None:
                attrs = {k: (v if v is not None else defaults.get(k)) for k, v in new_attrs.items()}
                conn.execute("INSERT INTO leads VALUES (?, ?, ?, ?, ?, ?)",
                             (wf, digest, canonical[-4:], batch_id, attrs[ref_field],
                              json.dumps(attrs, sort_keys=True)))
            else:
                stats["merged"] += 1                       # later row: non-null values win
                attrs = json.loads(found[0])
                attrs.update({k: v for k, v in new_attrs.items() if v is not None})
                conn.execute(
                    "UPDATE leads SET attrs = ?, external_ref = ? "
                    "WHERE workflow_id = ? AND phone_hash = ?",
                    (json.dumps(attrs, sort_keys=True), attrs[ref_field], wf, digest))
    return stats


# --------------------------------------------------------------- events

def _parse_event(line, key):
    """Row tuple for the events table, or None if the line is malformed."""
    try:
        e = json.loads(line)
    except ValueError:
        return None
    if not isinstance(e, dict) or not e.get("event_id"):
        return None
    when = parse_ts(e.get("occurred_at"))
    if when is None:                       # cannot be placed on the timeline
        return None
    phone = e.get("phone")
    canonical = normalise_phone(phone) if isinstance(phone, str) else None
    payload = e.get("payload") if isinstance(e.get("payload"), dict) else {}
    code = payload.get("disposition_code")
    return (
        str(e["event_id"]), str(e.get("event_type") or ""), e.get("workflow_id"),
        None if e.get("call_id") is None else str(e["call_id"]),
        phone_hash(canonical, key) if canonical else None,
        (when - EPOCH) // MICRO,
        code if isinstance(code, str) else None,
        json.dumps(payload, sort_keys=True),
    )


def _ingest_event_file(conn, path, key):
    stats = {"total": 0, "malformed": 0, "duplicates": 0}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            stats["total"] += 1
            row = _parse_event(line, key)
            if row is None:
                stats["malformed"] += 1
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO events (event_id, event_type, workflow_id, call_id, "
                "phone_hash, occurred_us, code, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
            if cur.rowcount == 0:          # the UNIQUE(event_id) constraint caught a repeat
                stats["duplicates"] += 1
    return stats


# ---------------------------------------------------------------- drops

def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _already_ingested(conn, file_key, sha):
    row = conn.execute("SELECT sha256 FROM ingested_files WHERE file_key = ?",
                       (file_key,)).fetchone()
    if row is None:
        return False
    if row[0] != sha:
        raise FileChanged(f"{file_key} was ingested before but its content has changed")
    return True


def ingest_drop(conn, drop_dir, schemas, key):
    """Ingest one drop. Returns (ingested_file_keys, skipped_file_keys)."""
    drop = Path(drop_dir).resolve()
    ingested, skipped = [], []

    leads_dir = drop / "leads"
    for wf_dir in sorted(p for p in leads_dir.iterdir() if p.is_dir()) if leads_dir.is_dir() else []:
        if wf_dir.name not in schemas:
            raise ValueError(f"no lead schema for workflow directory {wf_dir.name!r}")
        for path in sorted(wf_dir.glob("*.csv")):
            file_key = f"{drop.name}/leads/{wf_dir.name}/{path.name}"
            sha = _sha256(path)
            if _already_ingested(conn, file_key, sha):
                skipped.append(file_key)
                continue
            with conn:                                      # one transaction per file
                stats = _ingest_lead_file(conn, file_key, path, schemas[wf_dir.name], key)
                conn.execute("INSERT INTO ingested_files VALUES (?, 'leads', ?, ?)",
                             (file_key, sha, json.dumps(stats, sort_keys=True)))
            ingested.append(file_key)

    events_dir = drop / "events"
    for path in sorted(events_dir.glob("*.jsonl")) if events_dir.is_dir() else []:
        file_key = f"{drop.name}/events/{path.name}"
        sha = _sha256(path)
        if _already_ingested(conn, file_key, sha):
            skipped.append(file_key)
            continue
        with conn:
            stats = _ingest_event_file(conn, path, key)
            conn.execute("INSERT INTO ingested_files VALUES (?, 'events', ?, ?)",
                         (file_key, sha, json.dumps(stats, sort_keys=True)))
        ingested.append(file_key)
    return ingested, skipped
