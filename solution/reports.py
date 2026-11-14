"""Derive the five reports from the ledger. Nothing here writes to the database:
linking, orphans, calls, dispositions and buckets are all computed from stored facts,
so the result depends only on what has been ingested, not on the order of drops."""
import csv
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .normalise import IST

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ist(us):
    return (EPOCH + timedelta(microseconds=us)).astimezone(IST)


# ------------------------------------------------------- derive the facts

def link_events(conn, leads):
    """Resolve every stored event to a lead key (workflow, phone_hash), or None (orphan).

    An event links by (workflow, phone hash). A disposition.override has no phone, so it
    links through its call_id to the lead of that call's other events."""
    rows = conn.execute(
        "SELECT seq, event_type, workflow_id, call_id, phone_hash, occurred_us, code "
        "FROM events ORDER BY seq").fetchall()
    call_lead = {}
    linked = []
    for seq, etype, wf, call_id, ph, us, code in rows:
        lead = (wf, ph) if ph is not None and (wf, ph) in leads else None
        if lead is not None and call_id is not None:
            call_lead.setdefault((wf, call_id), lead)
        linked.append([seq, etype, wf, call_id, lead, us, code])
    for ev in linked:
        if ev[4] is None and ev[1] == "disposition.override" and ev[3] is not None:
            ev[4] = call_lead.get((ev[2], ev[3]))
    return linked


def derive(conn, dispositions):
    """Returns (leads, calls_by_lead, counters) where each call is a dict with
    init_us, connected, failed and disposition (effective code or None)."""
    leads = {
        (wf, ph): {"batch_id": batch, "ref": ref, "last4": last4}
        for wf, ph, batch, ref, last4 in conn.execute(
            "SELECT workflow_id, phone_hash, batch_id, external_ref, phone_last4 FROM leads")
    }
    orphans = unknown = 0
    calls = {}
    for seq, etype, wf, call_id, lead, us, code in link_events(conn, leads):
        if lead is None:
            orphans += 1                                    # stays out of every report
            continue
        if etype.startswith("disposition.") and (wf, code) not in dispositions:
            unknown += 1                                    # counted, then ignored
            continue
        if call_id is None:
            continue
        call = calls.setdefault((lead, call_id), {
            "init_us": None, "connected": False, "failed": False, "set": None, "override": None})
        if etype == "call.initiated":
            call["init_us"] = us if call["init_us"] is None else min(call["init_us"], us)
        elif etype == "call.connected":
            call["connected"] = True
        elif etype == "call.telephony_failed":
            call["failed"] = True
        elif etype in ("disposition.set", "disposition.override"):
            slot = "override" if etype == "disposition.override" else "set"
            best = call[slot]
            if best is None or (us, seq) > (best[0], best[1]):   # latest wins; ties -> later arrival
                call[slot] = (us, seq, code)

    calls_by_lead = defaultdict(list)
    for (lead, _), call in calls.items():
        if call["init_us"] is None:
            continue                                        # no call.initiated: not a call
        chosen = call["override"] or call["set"]
        call["disposition"] = chosen[2] if chosen else None
        calls_by_lead[lead].append(call)
    for lst in calls_by_lead.values():
        lst.sort(key=lambda c: c["init_us"])
    return leads, calls_by_lead, {"orphan_events": orphans, "unknown_disposition_events": unknown}


def bucket(calls, wf, dispositions):
    if not calls:
        return "yet_to_reach_out"
    if any(c["disposition"] and dispositions[(wf, c["disposition"])][0] for c in calls):
        return "completed"
    if all(c["failed"] for c in calls):
        return "in_progress_telephony_failed"
    return "in_progress_dialed"


# ------------------------------------------------------------ the reports

def build_reports(conn, dispositions):
    leads, calls_by_lead, counters = derive(conn, dispositions)
    buckets = ("yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed")

    batch = defaultdict(lambda: dict.fromkeys(("total_leads", *buckets), 0))
    daily = defaultdict(lambda: {"calls": 0, "connected": 0, "success": set()})
    lead_rows = []
    for key, lead in leads.items():
        wf = key[0]
        calls = calls_by_lead.get(key, [])
        b = bucket(calls, wf, dispositions)
        batch[(wf, lead["batch_id"])]["total_leads"] += 1
        batch[(wf, lead["batch_id"])][b] += 1

        def success(c):
            return bool(c["disposition"]) and dispositions[(wf, c["disposition"])][1]

        for c in calls:
            day = daily[(wf, _ist(c["init_us"]).date().isoformat())]
            day["calls"] += 1
            day["connected"] += c["connected"]
            if success(c):
                day["success"].add(key)
        with_disp = [c for c in calls if c["disposition"]]
        lead_rows.append((
            wf, lead["ref"], lead["batch_id"], "+91******" + lead["last4"], b,
            len(calls), sum(c["connected"] for c in calls),
            _ist(calls[-1]["init_us"]).isoformat(timespec="seconds") if calls else "",
            with_disp[-1]["disposition"] if with_disp else "",
            str(any(success(c) for c in calls)).lower(), key[1]))

    out = {}
    out["batch_summary.csv"] = [
        ["workflow_id", "batch_id", "total_leads", *buckets]] + [
        [wf, bid, *(v["total_leads"], *(v[x] for x in buckets))]
        for (wf, bid), v in sorted(batch.items())]
    out["daily_overview.csv"] = [
        ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"]] + [
        [wf, day, d["calls"], d["connected"], f"{d['connected'] / d['calls']:.4f}", len(d["success"])]
        for (wf, day), d in sorted(daily.items())]
    lead_rows.sort(key=lambda r: (r[0], r[1], r[2], r[10]))
    out["lead_report.csv"] = [
        ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
         "connected_calls", "last_call_at", "current_disposition", "ever_success"]] + [
        list(r[:10]) for r in lead_rows]
    out["rejections.csv"] = [["workflow_id", "batch_id", "row_number", "reason"]] + [
        list(r) for r in conn.execute(
            "SELECT workflow_id, batch_id, row_number, reason FROM rejections "
            "ORDER BY workflow_id, batch_id, row_number")]
    out["data_quality.json"] = data_quality(conn, counters)
    return out


def data_quality(conn, counters):
    total = defaultdict(int)
    by_reason = defaultdict(int)
    for kind, stats in conn.execute("SELECT kind, stats FROM ingested_files"):
        s = json.loads(stats)
        if kind == "leads":
            total["rows"] += s["rows"]
            total["rejected"] += s["rejected"]
            total["merged"] += s["merged"]
            total["warnings"] += s["warnings"]
            for reason, n in s["by_reason"].items():
                by_reason[reason] += n
        else:
            total["events"] += s["total"]
            total["malformed"] += s["malformed"]
            total["duplicates"] += s["duplicates"]
    return {
        "lead_rows_total": total["rows"],
        "lead_rows_rejected": total["rejected"],
        "rejections_by_reason": dict(sorted(by_reason.items())),
        "duplicate_lead_rows_merged": total["merged"],
        "field_warnings": total["warnings"],
        "events_total": total["events"],
        "malformed_events": total["malformed"],
        "duplicate_events_dropped": total["duplicates"],
        "orphan_events": counters["orphan_events"],
        "unknown_disposition_events": counters["unknown_disposition_events"],
    }


def write_reports(conn, dispositions, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    reports = build_reports(conn, dispositions)
    for name, content in reports.items():
        path = out_dir / name
        if name.endswith(".json"):
            path.write_text(json.dumps(content, indent=2) + "\n", encoding="utf-8")
        else:
            with open(path, "w", newline="", encoding="utf-8") as fh:
                csv.writer(fh, lineterminator="\n").writerows(content)
    return reports
