# AI Usage Disclosure

## Tools used
- **Claude (chat, claude.ai)**: used throughout, for the large majority of the work described below.

## What I used them for
- Drafting the project plan and the instruction/roadmap file I gave Claude Code.
- **Chat Claude wrote the final code:** the normalisers and their tests, the SQLite schema, lead and event ingestion, the report derivation, the CLI, and the test suite (77 tests). It also wrote a naive reference implementation of the spec to diff against, and a mutation check.
- **Claude Code wrote a first version** of ingestion and reports (about 1,500 lines) from the roadmap in `CLAUDE.md`. I did not keep it.

## What I wrote or decided myself
- Chose Assignment A and the project name, and decided to submit a working pipeline over stretch goals.
- When running the Claude Code version on the real data showed it was broken (details below), I decided to discard it and use a rewrite whose design I could explain, instead of patching code I did not understand.
- I decided which design choices to adopt (report-time linking, HMAC phone hashing, append-only ledger with `UNIQUE(event_id)`, one transaction per file) after having each explained to me, and I am responsible for being able to explain them.

## Where the AI was wrong or unhelpful
- **Claude Code's first version looked finished but did not work.** Running it on the generated data exposed: a call to a function that did not exist (`normalise_date`); column-alias matching that only used the canonical field name, so 542 of 546 rows in drop 2 were rejected as "missing customer_name"; event linking that passed a phone number to a function that looks up by hash, so 4,378 of 4,498 events became orphans; orphans decided at ingest time although the spec says they must reflect the final state; a `rejections` table that was read but never created or filled; plaintext names and phones in the database; a "keyed hash" that was a plain `sha256(key + phone)` rather than an HMAC; and a hardcoded fallback PII key committed in the source. Its own fix for the first crash was to delete the failing import, which hid the problem. The 51 passing tests it reported covered only the normalisers, not the pipeline. I noticed by running the pipeline and reading the output, not from the tests.

## How I verified AI-generated code
- Ran the full pipeline on generated data and checked the outputs (`data_quality.json`, the five reports).
- `pytest -q` passes (77 tests), including tests that ingest tiny hand-built drops for each rule, a run-twice test, a "two drops in sequence equal both at once" test on generated data, and a scan of the database file for plaintext names and phones.
- Claude ran, in its own environment and reported to me: a differential test against a separate naive implementation of the spec on four seeds (identical output), seven deliberate code breakages that each made a test fail, and a large-scale timing run. These are not in the repo.

