"""SQLite schema, connection and PII hashing for the interaction ledger."""
import hashlib
import hmac
import os
import sqlite3
from pathlib import Path

SCHEMA = """
-- One row per input file ever ingested. Makes re-running ingest a no-op, and holds
-- the per-file counters that data_quality.json is built from.
CREATE TABLE IF NOT EXISTS ingested_files (
    file_key TEXT PRIMARY KEY,                 -- e.g. drop_1/leads/motor_renewal/batch_001.csv
    kind     TEXT NOT NULL CHECK (kind IN ('leads', 'events')),
    sha256   TEXT NOT NULL,
    stats    TEXT NOT NULL                     -- JSON counters
);

-- One row per (workflow, phone). PII is never stored: the phone is kept only as a keyed
-- hash (for matching) plus its last 4 digits (for masking); names are dropped.
CREATE TABLE IF NOT EXISTS leads (
    workflow_id  TEXT NOT NULL,
    phone_hash   TEXT NOT NULL,
    phone_last4  TEXT NOT NULL,
    batch_id     TEXT NOT NULL,                -- file stem of the first valid row, forever
    external_ref TEXT NOT NULL,                -- the schema's reference field
    attrs        TEXT NOT NULL,                -- JSON of the other non-PII fields
    PRIMARY KEY (workflow_id, phone_hash)
);

-- Rejected lead rows: where and why, never what.
CREATE TABLE IF NOT EXISTS rejections (
    file_key    TEXT    NOT NULL,
    workflow_id TEXT    NOT NULL,
    batch_id    TEXT    NOT NULL,
    row_number  INTEGER NOT NULL,
    reason      TEXT    NOT NULL,
    PRIMARY KEY (file_key, row_number)
);

-- The append-only interaction ledger. Every well-formed, non-duplicate event is stored,
-- including ones that cannot be linked yet. Linking happens when reporting, so an event
-- whose lead arrives in a later drop is picked up automatically.
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,   -- arrival order
    event_id    TEXT    NOT NULL UNIQUE,             -- database-level dedupe
    event_type  TEXT    NOT NULL,
    workflow_id TEXT,
    call_id     TEXT,
    phone_hash  TEXT,                                -- NULL if phone missing/invalid
    occurred_us INTEGER NOT NULL,                    -- microseconds since the UTC epoch
    code        TEXT,                                -- disposition_code, if any
    payload     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_phone ON events (workflow_id, phone_hash);
CREATE INDEX IF NOT EXISTS idx_events_call  ON events (workflow_id, call_id);

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;
"""


def connect(path):
    """Open (creating if needed) the database with the schema in place."""
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def load_key(env=None, dotenv=".env"):
    """PII key from $LEDGER_PII_KEY, else from ./.env. There is deliberately no default."""
    env = os.environ if env is None else env
    key = env.get("LEDGER_PII_KEY")
    if not key and Path(dotenv).exists():
        for line in Path(dotenv).read_text().splitlines():
            name, sep, value = line.partition("=")
            if sep and name.strip() == "LEDGER_PII_KEY":
                key = value.strip()
    if not key:
        raise RuntimeError("LEDGER_PII_KEY is not set (see .env.example)")
    return key


def phone_hash(canonical_phone, key):
    """HMAC-SHA256 of a canonical phone: stable for matching, useless without the key."""
    return hmac.new(key.encode(), canonical_phone.encode(), hashlib.sha256).hexdigest()
