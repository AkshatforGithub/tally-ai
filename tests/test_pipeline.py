"""End-to-end tests: build tiny drops on disk, ingest them, check the reports."""
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from solution.catalogue import DEFAULT_DIR, load_dispositions, load_schemas
from solution.db import connect, load_key
from solution.ingest import FileChanged, ingest_drop
from solution.reports import build_reports, write_reports

KEY = "test-key"
SCHEMAS = load_schemas()
DISP = load_dispositions()
HEADER = "customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language"
GENERATOR = Path(__file__).resolve().parents[1] / "assignment-a-data" / "tools" / "generate_data.py"


# ------------------------------------------------------------- helpers

def lead(phone, ref, name="Asha Rao", lang=""):
    return f"{name},{phone},{ref},,,{lang}"


def event(eid, etype, call, ts, phone="9932302346", code=None, wf="motor_renewal"):
    e = {"event_id": eid, "event_type": etype, "workflow_id": wf, "call_id": call,
         "occurred_at": ts, "payload": {}}
    if etype == "disposition.override":
        e["payload"] = {"disposition_code": code, "overridden_by": "sup", "reason": "r"}
    else:
        e["phone"] = phone
        if code:
            e["payload"] = {"disposition_code": code}
    return e


def make_drop(root, name, leads=None, events=None):
    """leads: {(workflow, batch_stem): [csv lines]} ; events: {file_stem: [dict | raw str]}"""
    drop = Path(root) / name
    for (wf, batch), lines in (leads or {}).items():
        path = drop / "leads" / wf / f"{batch}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        header = HEADER if wf == "motor_renewal" else "customer_name,phone,order_id,product,order_date,preferred_language"
        path.write_text("\n".join([header, *lines]) + "\n", encoding="utf-8")
    for stem, evs in (events or {}).items():
        path = drop / "events" / f"{stem}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(e if isinstance(e, str) else json.dumps(e) for e in evs) + "\n",
                        encoding="utf-8")
    return drop


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "t.db")
    yield c
    c.close()


def ingest(conn, drop):
    return ingest_drop(conn, drop, SCHEMAS, KEY)


def table(conn, name):
    rows = build_reports(conn, DISP)[name]
    if name.endswith(".json"):
        return rows
    return [dict(zip(rows[0], map(str, r))) for r in rows[1:]]


def one_lead(conn):
    (row,) = table(conn, "lead_report.csv")
    return row


P = "9932302346"
L = lead(P, "POL-1")


# --------------------------------------------------- derived facts

def test_effective_disposition_prefers_latest_override_over_a_later_set(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.override", "c1", "2026-10-01T11:00:00Z", code="renewed"),
        event("3", "disposition.set", "c1", "2026-10-01T12:00:00Z", code="not_interested"),
        event("4", "disposition.override", "c1", "2026-10-01T09:00:00Z", code="dnd"),
    ]}))
    row = one_lead(conn)
    assert row["current_disposition"] == "renewed"      # latest override by time, not arrival
    assert row["ever_success"] == "true"


def test_without_override_the_latest_set_wins(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", code="payment_link_sent"),
        event("3", "disposition.set", "c1", "2026-10-01T10:09:00Z", code="renewed"),
    ]}))
    assert one_lead(conn)["current_disposition"] == "renewed"


def test_override_has_no_phone_and_links_through_call_id(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", code="payment_link_sent"),
        event("3", "disposition.override", "c1", "2026-10-01T11:00:00Z", code="renewed"),
        event("4", "disposition.override", "other_call", "2026-10-01T11:00:00Z", code="dnd"),
    ]}))
    assert one_lead(conn)["current_disposition"] == "renewed"
    assert table(conn, "data_quality.json")["orphan_events"] == 1     # the unlinkable override


def test_unknown_disposition_is_counted_and_ignored_not_masking_the_real_one(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", code="renewed"),
        event("3", "disposition.override", "c1", "2026-10-01T11:00:00Z", code="made_up_code"),
    ]}))
    assert one_lead(conn)["current_disposition"] == "renewed"
    assert table(conn, "data_quality.json")["unknown_disposition_events"] == 1


def test_codes_are_checked_per_workflow(conn, tmp_path):
    # 'refused' exists for policy_feedback only
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", code="refused"),
    ]}))
    assert one_lead(conn)["current_disposition"] == ""
    assert table(conn, "data_quality.json")["unknown_disposition_events"] == 1


def test_buckets(conn, tmp_path):
    leads = [lead(f"99323023{i:02d}", f"POL-{i}") for i in range(4)]
    phones = [f"99323023{i:02d}" for i in range(4)]
    evs = [
        # 1: never called -> yet_to_reach_out
        # 2: terminal disposition -> completed
        event("a", "call.initiated", "c2", "2026-10-01T10:00:00Z", phones[1]),
        event("b", "disposition.set", "c2", "2026-10-01T10:05:00Z", phones[1], code="not_interested"),
        # 3: every call failed -> telephony_failed (even though 'ended' too)
        event("c", "call.initiated", "c3", "2026-10-01T10:00:00Z", phones[2]),
        event("d", "call.telephony_failed", "c3", "2026-10-01T10:00:01Z", phones[2]),
        # 4: one failed, one dialed with a non-terminal disposition -> in_progress_dialed
        event("e", "call.initiated", "c4", "2026-10-01T10:00:00Z", phones[3]),
        event("f", "call.telephony_failed", "c4", "2026-10-01T10:00:01Z", phones[3]),
        event("g", "call.initiated", "c5", "2026-10-02T10:00:00Z", phones[3]),
        event("h", "disposition.set", "c5", "2026-10-02T10:05:00Z", phones[3], code="callback_requested"),
    ]
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): leads}, {"e1": evs}))
    got = {r["external_ref"]: r["bucket"] for r in table(conn, "lead_report.csv")}
    assert got == {"POL-0": "yet_to_reach_out", "POL-1": "completed",
                   "POL-2": "in_progress_telephony_failed", "POL-3": "in_progress_dialed"}
    (summary,) = table(conn, "batch_summary.csv")
    assert summary["total_leads"] == "4" and summary["completed"] == "1"


def test_events_of_a_call_without_initiated_are_not_a_call(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.connected", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", code="renewed"),
    ]}))
    row = one_lead(conn)
    assert (row["bucket"], row["total_calls"], row["ever_success"]) == ("yet_to_reach_out", "0", "false")
    assert table(conn, "daily_overview.csv") == []


def test_call_date_is_the_ist_date(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T20:00:00Z"),      # 01:30 IST on the 2nd
        event("2", "call.connected", "c1", "2026-10-01 01:31:00"),       # naive = IST
    ]}))
    (day,) = table(conn, "daily_overview.csv")
    assert day["date"] == "2026-10-02" and day["connectivity_rate"] == "1.0000"
    assert one_lead(conn)["last_call_at"] == "2026-10-02T01:30:00+05:30"


# ------------------------------------------------------ event handling

def test_malformed_and_duplicate_events_are_counted_and_skipped(conn, tmp_path):
    good = event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z")
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [
        good, json.dumps(good), "{not json", json.dumps({"event_type": "x"}), "",
        event("2", "call.initiated", "c2", "2026-10-02T10:00:00Z"),
    ]}))
    dq = table(conn, "data_quality.json")
    assert (dq["events_total"], dq["malformed_events"], dq["duplicate_events_dropped"]) == (5, 2, 1)
    assert one_lead(conn)["total_calls"] == "2"


def test_duplicate_across_drops_is_dropped(conn, tmp_path):
    e = event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z")
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]}, {"e1": [e]}))
    ingest(conn, make_drop(tmp_path, "drop_2", events={"e2": [e]}))
    assert table(conn, "data_quality.json")["duplicate_events_dropped"] == 1
    assert one_lead(conn)["total_calls"] == "1"


def test_event_for_a_lead_that_arrives_later_counts_once_the_lead_exists(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", events={"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"),
        event("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", code="renewed"),
        event("3", "disposition.override", "c1", "2026-10-01T11:00:00Z", code="not_interested"),
    ]}))
    assert table(conn, "data_quality.json")["orphan_events"] == 3
    assert table(conn, "lead_report.csv") == []

    ingest(conn, make_drop(tmp_path, "drop_2", {("motor_renewal", "batch_002"): [L]}))
    assert table(conn, "data_quality.json")["orphan_events"] == 0
    row = one_lead(conn)
    assert (row["batch_id"], row["total_calls"], row["current_disposition"]) == ("batch_002", "1", "not_interested")


def test_same_phone_in_two_workflows_is_two_leads(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {
        ("motor_renewal", "batch_001"): [L],
        ("policy_feedback", "batch_001"): [f"Asha,{P},ORD-1,Earbuds,,"],
    }, {"e1": [event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z", wf="policy_feedback")]}))
    rows = {r["workflow_id"]: r for r in table(conn, "lead_report.csv")}
    assert rows["policy_feedback"]["total_calls"] == "1"
    assert rows["motor_renewal"]["total_calls"] == "0"


# ------------------------------------------------------ lead handling

def test_rejection_order_follows_schema_and_row_numbers_skip_blank_rows(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [
        lead("12345", "POL-1"),              # row 1: invalid phone
        ",,,,,",                             # blank: not counted
        f",12345,POL-2,,,",                  # row 2: name missing comes first in schema order
        lead(P, ""),                         # row 3: policy_number missing
        lead(P, "POL-3"),                    # row 4: valid
    ]}))
    got = [(r["row_number"], r["reason"]) for r in table(conn, "rejections.csv")]
    assert got == [("1", "invalid_phone"), ("2", "missing_required:customer_name"),
                   ("3", "missing_required:policy_number")]
    dq = table(conn, "data_quality.json")
    assert (dq["lead_rows_total"], dq["lead_rows_rejected"]) == (4, 3)


def test_duplicate_rows_merge_non_null_values_and_keep_first_batch(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {
        ("motor_renewal", "batch_001"): [f"Asha,{P},POL-OLD,,5000,hi"],
        ("motor_renewal", "batch_002"): [f"Asha,+91 {P[:5]} {P[5:]},POL-NEW,,,"],
    }))
    (row,) = table(conn, "lead_report.csv")
    assert (row["external_ref"], row["batch_id"]) == ("POL-NEW", "batch_001")
    attrs = json.loads(conn.execute("SELECT attrs FROM leads").fetchone()[0])
    assert attrs["premium_amount"] == "5000.00" and attrs["preferred_language"] == "hi"
    assert table(conn, "data_quality.json")["duplicate_lead_rows_merged"] == 1


def test_defaults_apply_to_still_null_fields_and_bad_optionals_warn(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [
        f"Asha,{P},POL-1,2026-13-01,abc,Hindi"]}))
    attrs = json.loads(conn.execute("SELECT attrs FROM leads").fetchone()[0])
    assert attrs["preferred_language"] == "en" and attrs["policy_expiry"] is None
    assert table(conn, "data_quality.json")["field_warnings"] == 3


def test_warnings_are_not_counted_for_rejected_rows(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [
        f"Asha,12345,POL-1,not-a-date,abc,Hindi"]}))
    assert table(conn, "data_quality.json")["field_warnings"] == 0


def test_renamed_and_reordered_columns_match_by_alias(conn, tmp_path):
    drop = tmp_path / "drop_1" / "leads" / "motor_renewal"
    drop.mkdir(parents=True)
    (drop / "batch_001.csv").write_text(
        "Language,Insured Name,Policy No,Mobile No,Expiry Date,Premium,agent_notes\n"
        f"en,Asha,POL-1,{P},05/11/2026,INR 18678.3,call after 5\n", encoding="utf-8")
    ingest(conn, drop.parents[1])
    assert one_lead(conn)["external_ref"] == "POL-1"
    assert table(conn, "data_quality.json")["lead_rows_rejected"] == 0


# ------------------------------------------- idempotency and storage

def reports_text(conn):
    return json.dumps(build_reports(conn, DISP), sort_keys=True)


def test_reingesting_a_drop_changes_nothing(conn, tmp_path):
    drop = make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L, L, lead("1", "X")]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z"), "garbage"]})
    first, _ = ingest(conn, drop)
    before = reports_text(conn)
    again, skipped = ingest(conn, drop)
    assert first and not again and len(skipped) == len(first)
    assert reports_text(conn) == before


def test_a_changed_file_is_refused_rather_than_double_applied(conn, tmp_path):
    drop = make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]})
    ingest(conn, drop)
    (drop / "leads" / "motor_renewal" / "batch_001.csv").write_text(HEADER + "\n" + lead(P, "POL-9") + "\n")
    with pytest.raises(FileChanged):
        ingest(conn, drop)


def test_a_failed_file_leaves_no_partial_data(conn, tmp_path):
    drop = make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [L]})
    path = drop / "leads" / "motor_renewal" / "batch_001.csv"
    path.write_bytes(path.read_bytes() + b"\xff\xfe,bad bytes\n")          # undecodable UTF-8
    with pytest.raises(UnicodeDecodeError):
        ingest(conn, drop)
    assert conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0] == 0


def test_no_plaintext_pii_reaches_the_database_file(tmp_path):
    db = tmp_path / "pii.db"
    conn = connect(db)
    ingest(conn, make_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): [
        lead("9876501234", "POL-1", name="Zubinaabo Quentarius")]}, {"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z", phone="+91 98765 01234")]}))
    conn.close()
    raw = db.read_bytes()
    assert b"Zubinaabo" not in raw and b"9876501234" not in raw and b"98765 01234" not in raw


def test_the_ledger_is_append_only(conn, tmp_path):
    ingest(conn, make_drop(tmp_path, "drop_1", events={"e1": [
        event("1", "call.initiated", "c1", "2026-10-01T10:00:00Z")]}))
    for sql in ("UPDATE events SET code = 'x'", "DELETE FROM events"):
        with pytest.raises(Exception, match="append-only"):
            conn.execute(sql)


def test_pii_key_has_no_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError):
        load_key({})


# ------------------------------------- generated data (the real thing)

@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen")
    subprocess.run([sys.executable, str(GENERATOR), "--seed", "7", "--scale", "small", "--out", str(out)],
                   check=True, capture_output=True)
    return out


def run_pipeline(tmp_path, drops):
    conn = connect(tmp_path / "x.db")
    for d in drops:
        ingest(conn, d)
    out = build_reports(conn, DISP)
    conn.close()
    return json.dumps(out, sort_keys=True)


def test_two_drops_in_sequence_equal_everything_at_once(generated, tmp_path):
    merged = tmp_path / "merged"            # both drops' files in one directory
    for src in generated.glob("drop_*/leads/*/*.csv"):
        (merged / "leads" / src.parent.name).mkdir(parents=True, exist_ok=True)
        shutil.copy(src, merged / "leads" / src.parent.name / src.name)
    for src in generated.glob("drop_*/events/*.jsonl"):
        (merged / "events").mkdir(parents=True, exist_ok=True)
        shutil.copy(src, merged / "events" / src.name)
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    in_sequence = run_pipeline(tmp_path / "a", [generated / "drop_1", generated / "drop_2"])
    at_once = run_pipeline(tmp_path / "b", [merged])
    assert '"lead_report.csv": [[' in in_sequence and len(in_sequence) > 10_000   # not trivially empty
    assert in_sequence == at_once


def test_reports_are_internally_consistent(generated, tmp_path):
    conn = connect(tmp_path / "c.db")
    for d in ("drop_1", "drop_2"):
        ingest(conn, generated / d)
    leads, batch, daily, dq = (table(conn, n) for n in (
        "lead_report.csv", "batch_summary.csv", "daily_overview.csv", "data_quality.json"))
    assert len(leads) == sum(int(b["total_leads"]) for b in batch)
    assert sum(int(r["total_calls"]) for r in leads) == sum(int(d["total_calls"]) for d in daily)
    assert sum(int(r["connected_calls"]) for r in leads) == sum(int(d["connected_calls"]) for d in daily)
    assert dq["lead_rows_total"] - dq["lead_rows_rejected"] - dq["duplicate_lead_rows_merged"] == len(leads)
    assert len(table(conn, "rejections.csv")) == dq["lead_rows_rejected"]
    assert sum(dq["rejections_by_reason"].values()) == dq["lead_rows_rejected"]
    assert all(r["phone_masked"].startswith("+91******") and len(r["phone_masked"]) == 13 for r in leads)


def test_write_reports_produces_the_five_files(generated, tmp_path):
    conn = connect(tmp_path / "w.db")
    ingest(conn, generated / "drop_1")
    write_reports(conn, DISP, tmp_path / "out")
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == [
        "batch_summary.csv", "daily_overview.csv", "data_quality.json",
        "lead_report.csv", "rejections.csv"]
