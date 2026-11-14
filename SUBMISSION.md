# Submission

## Candidate
- **Name:** Akshat Kumar
- **Assignment chosen:** A (Data): Interaction Ledger & Reporting Pipeline, project name `tally-ai`
- **Time spent (honest estimate):** 9h

## How to run
```bash
git clone <this repo> tally-ai && cd tally-ai
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # the solution itself only needs the standard library + pytest
python3 -c "import secrets; print('LEDGER_PII_KEY=' + secrets.token_hex(32))" > .env   # HMAC key for phone hashing

python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/

pytest -q                                   # 77 tests, ~2 s; needs no .env
```
Options: `--db PATH` (or `$LEDGER_DB`, default `tally.db`) and `--schemas DIR` (default `assignment-a-data/schemas`).
`ingest` can be re-run on any drop; `report` can be run after any ingest. There is deliberately no default PII key:
without `LEDGER_PII_KEY` (env or `.env`) the program stops with an error.

## Design
Three steps with a hard line between **facts** and **derived views**:

```
leads CSV  -> normalise + validate -> leads / rejections   (one transaction per file)
events JSONL -> parse + dedupe     -> events ledger        (append-only)
                                         |
report:  link events to leads -> calls -> effective disposition -> bucket -> 5 files
```
Ingest stores only facts. Everything that depends on *other* data (does this event belong to a lead? is it an
orphan? how many calls does a lead have?) is computed when reporting, from the final state. That is why an
event that arrives in drop 1 for a lead that only appears in drop 2 is counted correctly, why "drop 1 then
drop 2" gives the same result as "both at once" (tested), and why orphan counts reflect the final state.

**Schema (SQLite, `solution/db.py`)**

| Table | Holds | Why this shape |
|---|---|---|
| `ingested_files` | file key (`drop_1/leads/<wf>/batch_001.csv`), sha256, per-file counters (JSON) | Makes re-runs a no-op; `data_quality.json` is built from these counters, not by re-reading files |
| `leads` | PK `(workflow_id, phone_hash)`, `phone_last4`, `batch_id`, `external_ref`, `attrs` (JSON, non-PII fields) | The primary key *is* the lead identity, so the database enforces it. No names, no phones |
| `rejections` | file, workflow, batch, row number, reason | No PII by construction (where and why, never what) |
| `events` | `seq` (arrival order), `event_id` UNIQUE, type, workflow, call_id, `phone_hash`, `occurred_us` (UTC microseconds), `code`, payload | Append-only: triggers abort any UPDATE or DELETE. `UNIQUE(event_id)` is the dedupe, so a repeat is caught by the database itself (`INSERT OR IGNORE` + `rowcount`) |

**PII.** A phone is stored only as `HMAC-SHA256(key, canonical_phone)` (needed to match leads and events) plus its
last 4 digits (needed for the masked column). A plain hash would be brute-forceable (about 4 billion Indian
mobile numbers); the secret key prevents that. Names are dropped because no report needs them. A test scans
the database file bytes for known names and phones.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Link events to leads at report time, not at ingest | Late-arriving leads and incremental drops are then correct by construction | Link at ingest and re-link on every new lead (more state, easy to get wrong) |
| Only `disposition.override` links through `call_id`; other events with an unknown phone are orphans | That is what the spec says; an override has no phone | Link any event by `call_id` (would silently attach unknown numbers to leads) |
| An event whose timestamp cannot be parsed counts as malformed | It cannot be placed on the timeline; spec only lists bad JSON / missing `event_id` | Store it with a null time (every derived rule would need a special case). The generator never emits this |
| Equal `occurred_at` on two dispositions: later arrival wins | Deterministic and explainable | Error out |
| Several `call.initiated` for one call: earliest time is the call time | Spec says "its call.initiated" (singular) | Latest; or count each as a call |
| `rejections_by_reason` lists only reasons that occurred | Natural for a counter | List every possible reason with zeros |
| `lead_rows_total` counts non-blank data rows, rejected ones included | Matches how row numbers are counted | Count only accepted rows |
| Required, non-phone field present but invalid becomes null + one warning | Spec only defines rejection for missing and invalid phone; no schema has such a field | Reject the row |
| Defaults (`preferred_language = en`) are applied when a lead is first inserted | Equivalent to applying them after merging, because a later non-null value overwrites anyway | Apply at report time |
| Same phone in two workflows is two leads | Spec: identity is `(workflow_id, phone)` | Global phone identity |
| `lead_report` ties on `external_ref` break by batch, then phone hash | Output must be deterministic | Leave to database order |
| `connectivity_rate` formatted with Python's `f"{x:.4f}"` | Matches how the expected values are most likely produced | `Decimal` half-up rounding |
| Dispositions catalogue is read at report time | Unknown-disposition counts then always follow the catalogue file | Store it at ingest |
| A file ingested before whose content changed is refused with an error | Silently re-applying would double-count; silently skipping would hide data loss | Re-ingest it |
| `fastapi`, `uvicorn`, `httpx` left in `requirements.txt` | They belong to the template (Assignment B's mock provider); I did not modify original files. Unused here | Remove them |

## Failure modes considered
- **Dirty leads:** six phone formats, renamed/reordered columns (matched by alias after header normalisation), mixed date and currency formats, blank rows (skipped, not counted), invalid optional values (null + warning), invalid required values (rejected with one reason, in schema order).
- **Dirty events:** invalid JSON, missing `event_id`, repeats (within a file and across drops), phones in odd formats, UTC / offset / naive timestamps (naive = IST), events for unknown numbers, events before their lead, unknown disposition codes.
- **Run twice:** each file is keyed by path and sha256; a repeat is skipped, so reports are byte-identical (tested).
- **Crash mid-file:** each file loads in one transaction together with its `ingested_files` row; a failure leaves nothing partial (tested with an undecodable file).
- **Missing key:** stops with an error; no default key exists.
- **Not handled:** key rotation (hashes change, so everything would have to be re-ingested); two ingest processes at once (SQLite locks, not designed for it); a file *renamed* after ingest is treated as new: events still dedupe by `event_id`, but its lead rows count again as merged duplicates; a lead whose phone changes is a different lead; report derivation holds all leads and events in memory (fine at the large scale below, not at 100k leads a day); a real `kill -9` mid-ingest was not exercised, I rely on SQLite's transaction guarantee.

## Testing
77 tests, one command (`pytest -q`):
- 51 unit tests for the normalisers (phone, date, decimal, enum, header, timestamps) using values seen in the real data.
- Tests for each derived rule: override beats a later set, override links through `call_id`, per-workflow disposition codes, unknown codes counted but ignored, bucket order, calls without `call.initiated`, IST call dates.
- Event handling: malformed, duplicates (same file and across drops), late-arriving lead, same phone in two workflows.
- Lead handling: rejection order and row numbers with blank rows, merge rules and first `batch_id`, defaults and warnings, alias headers.
- Run-twice, changed file, partial-failure rollback, append-only triggers, no plaintext PII in the database file, no default key.
- On generated data: two drops in sequence equal one merged drop; report totals are consistent with each other.

Also done during development but **not in the repo**: a separate naive in-memory implementation of the spec was diffed against the pipeline on seeds 1, 7, 42 and 99 (identical output), and I broke seven rules on purpose to confirm the tests fail. Not tested: real process kill, concurrency, performance in the suite.

## Results
Large dataset (seed 42), measured by Claude in its sandbox (timings on other machines will differ): ingest of both drops about 55 s, report about 8 s, 132,903 leads, 241 MB database. Small dataset: under 1 s end to end.

`data_quality.json` for seed 42 (small):
```json
{
  "lead_rows_total": 2057,
  "lead_rows_rejected": 108,
  "rejections_by_reason": {
    "invalid_phone": 42,
    "missing_required:customer_name": 25,
    "missing_required:order_id": 13,
    "missing_required:phone": 14,
    "missing_required:policy_number": 14
  },
  "duplicate_lead_rows_merged": 57,
  "field_warnings": 122,
  "events_total": 8745,
  "malformed_events": 8,
  "duplicate_events_dropped": 237,
  "orphan_events": 462,
  "unknown_disposition_events": 18
}
```

