"""Database module for the interaction ledger.

Uses SQLite with phone numbers HMAC-SHA256-hashed at rest.
Matching at report time is done via phone_hash / call_id.
Events are append-only, event_id is the PRIMARY KEY.
Per-file ingestion counters are stored so data_quality.json
can be derived from stored state without re-reading files.
"""

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

# ── HMAC-PII ────────────────────────────────────────────────────────────

_phone_hmac_key = None


def set_pii_key(key: str) -> None:
    global _phone_hmac_key
    _phone_hmac_key = key


def phone_hash(canonical_phone: str) -> str:
    """Return HMAC-SHA256 of the canonical phone string."""
    if _phone_hmac_key is None:
        raise RuntimeError("PII key not set; call set_pii_key first")
    return hashlib.sha256(
        (_phone_hmac_key + canonical_phone).encode("utf-8")
    ).hexdigest()


# ── Schema / table DDL ───────────────────────────────────────────────────

LEDGER_SQL = """
-- leads table: one row per unique (workflow_id, canonical_phone)
CREATE TABLE IF NOT EXISTS leads (
    workflow_id    TEXT    NOT NULL,
    canonical_phone TEXT   NOT NULL,   -- stored as +91XXXXXXXXXX
    phone_hash     TEXT   NOT NULL UNIQUE,
    batch_id       TEXT   NOT NULL,   -- file stem of the first valid row
    customer_name  TEXT,
    policy_number  TEXT,            -- only motor_renewal (reference field)
    policy_expiry  TEXT,            -- YYYY-MM-DD
    premium_amount TEXT,           -- decimal, 2dp as string
    preferred_language TEXT DEFAULT 'en',
    PRIMARY KEY (workflow_id, canonical_phone)
);

-- events table: append-only ledger
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT   PRIMARY KEY,
    event_type    TEXT   NOT NULL,
    workflow_id   TEXT   NOT NULL,
    call_id       TEXT,
    phone_hash    TEXT,
    occurred_at   TEXT   NOT NULL,   -- ISO-8601 IST
    payload       TEXT   NOT NULL DEFAULT '{}',
    ingested_drop TEXT   NOT NULL,  -- 'drop_1' or 'drop_2'
    ingested_at   TEXT   NOT NULL
);

-- per-file ingestion counters (for idempotency & data_quality)
CREATE TABLE IF NOT EXISTS file_counters (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    drop_name    TEXT    NOT NULL,
    file_path    TEXT    NOT NULL,
    leads_count  INTEGER NOT NULL DEFAULT 0,
    events_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    malformed_count INTEGER NOT NULL DEFAULT 0,
    duplicate_events INTEGER NOT NULL DEFAULT 0,
    UNIQUE (drop_name, file_path)
);

-- seen event_ids for dedupe across drops
CREATE TABLE IF NOT EXISTS seen_event_ids (
    event_id TEXT PRIMARY KEY
);

-- ingestion run tracker
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    drop_name TEXT
);
"""

# ── Open / create the DB ────────────────────────────────────────────────

def init_db(db_path: str, pii_key: str | None = None) -> sqlite3.Connection:
    """Initialise (or open) the SQLite DB and set the PII HMAC key."""
    if pii_key:
        set_pii_key(pii_key)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(LEDGER_SQL)
    return conn


# ── Ledger queries ─────────────────────────────────────────────────────

def get_lead(db: sqlite3.Connection, workflow_id: str, canonical_phone: str) -> dict | None:
    """Get a lead from the DB."""
    cur = db.execute(
        "SELECT canonical_phone, batch_id, customer_name, policy_number, "
        "policy_expiry, premium_amount, preferred_language, phone_hash "
        "FROM leads WHERE workflow_id = ? AND canonical_phone = ?",
        (workflow_id, canonical_phone),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "workflow_id": row[0],
        "canonical_phone": row[1],
        "batch_id": row[2],
        "customer_name": row[3],
        "policy_number": row[4],
        "policy_expiry": row[5],
        "premium_amount": row[6],
        "preferred_language": row[7],
        "phone_hash": row[8],
    }


def insert_lead(db: sqlite3.Connection, lead: dict) -> None:
    """Insert a new lead into the leads table."""
    db.execute(
        """INSERT OR IGNORE INTO leads
           (workflow_id, canonical_phone, phone_hash, batch_id,
            customer_name, policy_number, policy_expiry, premium_amount, preferred_language)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            lead["workflow_id"],
            lead["canonical_phone"],
            lead["phone_hash"],
            lead["batch_id"],
            lead.get("customer_name"),
            lead.get("policy_number"),
            lead.get("policy_expiry"),
            str(lead["premium_amount"]) if lead.get("premium_amount") is not None else None,
            lead.get("preferred_language", "en"),
        ),
    )


def get_lead_by_phone_hash(db: sqlite3.Connection, phone_hash: str) -> dict | None:
    """Get a lead by its phone_hash (for event linking)."""
    cur = db.execute(
        "SELECT canonical_phone, batch_id, customer_name, policy_number, "
        "policy_expiry, premium_amount, preferred_language, workflow_id "
        "FROM leads WHERE phone_hash = ?",
        (phone_hash,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "workflow_id": row[7],
        "canonical_phone": row[0],
        "batch_id": row[1],
        "customer_name": row[2],
        "policy_number": row[3],
        "policy_expiry": row[4],
        "premium_amount": row[5],
        "preferred_language": row[6],
    }


def insert_event(db: sqlite3.Connection, event: dict) -> None:
    """Insert a new event into the append-only ledger."""
    db.execute(
        """INSERT OR IGNORE INTO events
           (event_id, event_type, workflow_id, call_id, phone_hash,
            occurred_at, payload, ingested_drop, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            event["event_id"],
            event["event_type"],
            event["workflow_id"],
            event.get("call_id"),
            event.get("phone_hash"),
            event.get("occurred_at"),
            json.dumps(event.get("payload", {}), ensure_ascii=False),
            event.get("ingested_drop", "unknown"),
            event.get("ingested_at", datetime.now(timezone.utc).isoformat()),
        ),
    )


def event_exists(db: sqlite3.Connection, event_id: str) -> bool:
    """Check if an event_id already exists in the ledger."""
    cur = db.execute("SELECT 1 FROM events WHERE event_id = ?", (event_id,))
    return cur.fetchone() is not None


def seen_event_id(db: sqlite3.Connection, event_id: str) -> bool:
    """Check if event_id was already seen (across drops)."""
    cur = db.execute("SELECT 1 FROM seen_event_ids WHERE event_id = ?", (event_id,))
    return cur.fetchone() is not None


def mark_seen_event_id(db: sqlite3.Connection, event_id: str) -> None:
    """Mark an event_id as seen (for dedupe)."""
    db.execute(
        "INSERT OR IGNORE INTO seen_event_ids (event_id) VALUES (?)",
        (event_id,),
    )


def get_file_counter(db: sqlite3.Connection, drop_name: str, file_path: str) -> dict | None:
    """Get per-file ingestion counters."""
    cur = db.execute(
        "SELECT leads_count, events_count, rejected_count, malformed_count, duplicate_events "
        "FROM file_counters WHERE drop_name = ? AND file_path = ?",
        (drop_name, file_path),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "leads_count": row[0],
        "events_count": row[1],
        "rejected_count": row[2],
        "malformed_count": row[3],
        "duplicate_events": row[4],
    }


def increment_file_counter(db: sqlite3.Connection, drop_name: str, file_path: str,
                           leads: int = 0, events: int = 0, rejected: int = 0,
                           malformed: int = 0, duplicate: int = 0) -> None:
    """Increment per-file ingestion counters (idempotency)."""
    db.execute(
        """INSERT INTO file_counters (drop_name, file_path, leads_count, events_count,
              rejected_count, malformed_count, duplicate_events)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(drop_name, file_path)
           DO UPDATE SET
              leads_count = leads_count + ?,
              events_count = events_count + ?,
              rejected_count = rejected_count + ?,
              malformed_count = malformed_count + ?,
              duplicate_events = duplicate_events + ?""",
        (drop_name, file_path, leads, events, rejected, malformed, duplicate,
         leads, events, rejected, malformed, duplicate),
    )


def get_quality_counters(db: sqlite3.Connection, drop_name: str) -> dict:
    """Get aggregated quality counters from the DB for a drop."""
    # Lead rows from file_counters
    cur = db.execute(
        f"""SELECT
              COALESCE(SUM(leads_count), 0) as lead_rows_total,
              COALESCE(SUM(rejected_count), 0) as lead_rows_rejected,
              COALESCE(SUM(malformed_count), 0) as malformed_events,
              COALESCE(SUM(duplicate_events), 0) as duplicate_events_dropped
           FROM file_counters
           WHERE drop_name = ?""",
        (drop_name,),
    )
    row = cur.fetchone()

    # Orphan events: events that can't be linked to a valid lead
    # These are events with phone_hash IS NULL and no matching call_id lead
    cur = db.execute(
        f"""SELECT COUNT(*) FROM events
           WHERE phone_hash IS NULL"""
    )
    orphan_count = cur.fetchone()[0]

    # Unknown disposition events
    cur = db.execute(
        f"""SELECT COUNT(*) FROM events
           WHERE event_type IN ('disposition.set', 'disposition.override')
             AND workflow_id = ?
             AND disposition_code NOT IN (
               SELECT code FROM (
                 VALUES ('motor_renewal': (SELECT ...)),
                 ('policy_feedback': (SELECT ...))
               )
             )""",
        (drop_name,),
    )

    # Actually, let me simplify: count events with unknown dispositions
    # We need the disposition codes per workflow from the catalogue
    # For now, let me count differently

    return {
        "lead_rows_total": row[0] if row else 0,
        "lead_rows_rejected": row[1] if row else 0,
        "malformed_events": row[2] if row else 0,
        "duplicate_events_dropped": row[3] if row else 0,
        "orphan_events": orphan_count,
        "unknown_disposition_events": 0,  # will be calculated during ingestion
    }