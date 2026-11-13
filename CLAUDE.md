CLAUDE.md — Agentic Universe take-home, Assignment A (Interaction Ledger & Reporting Pipeline)


How to work:  

-Refer Roadmap work point by point after every point mark it as done and give short details
-Change only the files that need changing. No drive-by refactors.
-Plain prose answers, low token use. No decorative formatting.
-Never print or commit .env, secrets or the PII key.
-If the spec is ambiguous, pick a reasonable option, and append it to SUBMISSION.md under "Decisions & assumptions". Do not stall.
-Hard constraints (from the brief)
-Python 3.11+, list libraries in requirements.txt. Prefer stdlib + pytest.
-Do NOT modify original repo files. Our code lives in solution/, tests in tests/ (one command: pytest).
-SQLite is fine. Real DB, designed schema, explained in SUBMISSION.md.
-Never commit generated data (data/, data_large/), .env, *.db, reports/.
-Run commands must work from a clean clone; document them in README/SUBMISSION.md.
-Graders run on a different seed and diff the 5 report files. Exact formats and sort orders matter.
Data

Generate: python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data Drops: data/drop_N/leads/<workflow_id>/batch_XXX.csv, data/drop_N/events/events_XXXX.jsonl. Schemas: assignment-a-data/schemas/{motor_renewal,policy_feedback}.json, dispositions.csv (per workflow: terminal / success flags). Read docs/GLOSSARY.md. Test with several seeds (e.g. 1, 7, 99), not only 42.

CLI to build (interface is ours; document it)
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/
Spec rules (summary; the README in assignment-a-data/ is authoritative)

Leads:

Header normalise: lower, trim, collapse runs of _/whitespace to one space; match field name or aliases (same normalisation). Ignore unmatched columns.
Blank rows skipped and not counted. Row numbers count non-blank data rows per file from 1.
Values trimmed; empty = missing.
phone: strip non-digits; 12 digits starting 91 -> drop 91; 11 digits starting 0 -> drop 0; valid iff 10 digits starting 6-9; store +91XXXXXXXXXX.
date: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY (month name case-insensitive), day-first, impossible dates invalid.
decimal: optional prefix ₹/Rs/Rs./INR (case-insens, optional space), commas = thousands, must be >= 0, round 2dp.
enum: case-insensitive vs values, store lower-case.
Rejection: walk fields in schema order; first failure is the single reason: missing_required:<field> or invalid_phone (phone present but invalid). Rejected rows -> rejections report, not leads.
Optional field present but invalid -> null + 1 field warning (only for non-rejected rows).
Identity = (workflow_id, canonical phone). Process drops in ingest order, files by filename, rows in order. First valid row creates the lead; its file stem (batch_001) is the lead's batch_id forever. Later valid rows for same identity = 1 merged duplicate each; overwrite only fields where new value is non-null (last write wins).
Defaults (e.g. preferred_language -> en) apply after merging to still-null fields.

Events (JSONL), per line in this order:

Malformed (invalid JSON or no event_id) -> count, skip.
Duplicate event_id (seen in any drop) -> count, skip.
Orphan: can't link to a valid lead -> count, exclude from all reports. Link via (workflow_id, normalised phone). disposition.override has no phone: link via call_id to the lead of that call's other events.
Unknown disposition (disposition.* code not in dispositions.csv for that workflow) -> count, ignore.
Timestamps: ...Z (UTC), +05:30 offset, naive YYYY-MM-DD HH:MM:SS = IST. All report dates are IST.
Events may arrive before their lead (earlier drop). Reports reflect everything ingested so far; orphan counts reflect final state.

Derived:

A call counts only if its call.initiated is present and linked. Call date = IST date of call.initiated.
Connected = has call.connected.
Effective disposition = latest disposition.override by occurred_at, else latest disposition.set, else none. Overrides never edit/delete the original event (ledger append-only).
Lead bucket, first match: yet_to_reach_out (no calls) > completed (any call's effective disposition is terminal) > in_progress_telephony_failed (every call has call.telephony_failed) > in_progress_dialed.

Reports (report --out <dir>; header row, comma, no index, booleans true/false):

batch_summary.csv sorted workflow_id,batch_id: workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,in_progress_dialed,completed
daily_overview.csv one row per workflow per IST date with >=1 call, sorted workflow_id,date: workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads (rate = connected/total, exactly 4 decimals; success_leads = distinct leads with a call that date whose effective disposition is_success)
lead_report.csv sorted workflow_id,external_ref: workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,last_call_at,current_disposition,ever_success (external_ref = schema field with "reference": true; phone_masked = +91****** + last 4; last_call_at = latest initiated time ISO-8601 IST like 2026-10-14T17:55:24+05:30, empty if none; current_disposition = effective disposition of most recent call that has one)
rejections.csv sorted workflow_id,batch_id,row_number: workflow_id,batch_id,row_number,reason (NO PII)
data_quality.json: lead_rows_total, lead_rows_rejected, rejections_by_reason{}, duplicate_lead_rows_merged, field_warnings, events_total, malformed_events, duplicate_events_dropped, orphan_events, unknown_disposition_events (events_total counts every non-empty line read incl. malformed and duplicates)
Design decisions (starting point; change in SUBMISSION.md if we deviate)
SQLite, stdlib sqlite3.
PII: fields with "pii": true never stored in plaintext. Phone matched via HMAC-SHA256(key, canonical_phone); key from env LEDGER_PII_KEY (in .env, never committed; document how to generate). Other PII fields: store hashed or drop; only last-4 digits kept for masking (note as trade-off).
Ledger: events table append-only, event_id PRIMARY KEY (DB-level dedupe). Store phone_hash, call_id, type, normalised UTC time, payload JSON (no raw phone).
Linking at report time, not ingest time: store every non-malformed, non-duplicate event; resolve lead via phone_hash/call_id when reporting. This makes late-arriving leads and incremental drops correct by construction, and orphans are simply events that still don't resolve.
Idempotency: ingested_files(path, sha256, stats...); skip already-ingested files. Persist per-file counters so data_quality.json is derived from stored state, not re-reading files. event_id PK is a second guard.
Wrap each file ingest in one transaction.
Aggregation can be Python or SQL; correctness first.

Roadmap (tick as done)
 0. Setup: venv, .gitignore, solution/, tests/, generate data, read schemas + samples
 1. Normalisers + unit tests: header, phone, date, decimal, enum, timestamp->IST
 2. DB schema + PII hashing helper
 3. Lead ingestion: schema-driven, rejections, field warnings, merge, defaults, batch_id
 4. Event ingestion: malformed/duplicate handling, store ledger, per-file counters
 5. Derived facts: effective disposition, calls, buckets (+ unit tests)
 6. Five reports in exact contract format
 7. Idempotency + incremental check: re-ingest drop_1 (no change); drop_1+drop_2 vs all-at-once give identical reports; test on 3+ seeds
 8. Pytest green with one command
 9. SUBMISSION.md (decisions, schema explanation, trade-offs, what's next), AI_USAGE.md, README run commands from a clean clone
 10. Private repo push, reviewer read access, submit via the Microsoft Form from the recruiter email
 Stretch (optional, at most one): large-scale timing, SQL aggregation, production note for 100k leads/day
Definition of done

Clean-clone commands work; pytest passes; reports match the contract on at least 3 seeds; ingesting twice changes nothing; no secrets/data/db committed; SUBMISSION.md and AI_USAGE.md filled honestly; Akshat can explain the design aloud.