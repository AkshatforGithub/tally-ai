"""CLI entry point for the Interaction Ledger & Reporting Pipeline.

Usage:
    python -m solution ingest data/drop_1
    python -m solution ingest data/drop_2
    python -m solution report --out reports/
"""

import argparse
import os
import sys

from .db import init_db, set_pii_key
from .ingest import ingest_leads, ingest_events
from .reports import (
    generate_batch_summary,
    generate_daily_overview,
    generate_lead_report,
    generate_rejections,
    generate_data_quality,
)


# ──────────────────────────────────────────────────────────────────────
# CLI configuration
# ──────────────────────────────────────────────────────────────────────

DEF_PII_KEY = "ledger_pii_key_default_change_me"


def main(argv: list | None = None) -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="solution",
        description="Interaction Ledger & Reporting Pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=False)

    # ── ingest subcommand ────────────────────────────────────────────
    ingest_parser = subparsers.add_parser("ingest", help="Ingest data drops")
    ingest_parser.add_argument(
        "drop_path",
        help="Path to the drop directory (e.g. data/drop_1)",
    )

    # ── report subcommand ────────────────────────────────────────────
    report_parser = subparsers.add_parser("report", help="Generate reports")
    report_parser.add_argument(
        "--out",
        required=True,
        help="Output directory for report files",
    )

    args = parser.parse_args(argv)

    if args.command == "ingest":
        return cmd_ingest(args.drop_path)
    elif args.command == "report":
        return cmd_report(args.out)
    else:
        parser.print_help()
        return 1


def cmd_ingest(drop_path: str) -> int:
    """Handle the ingest command."""
    # Determine the drop name from the path
    drop_name = os.path.basename(drop_path).rstrip("/")

    # Determine the DB path (simple approach: use a local .ledger.db)
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".ledger.db")

    # Initialize the database
    pii_key = os.environ.get("LEDGER_PII_KEY", DEF_PII_KEY)
    set_pii_key(pii_key)
    db = init_db(db_path, pii_key)

    # Check if this is the first ingest or a re-run
    # For idempotency, we check file counters
    # But first, let me just ingest everything (the DB will handle dedupe)

    print(f"Ingesting drop: {drop_name} from path: {drop_path}")

    # Ingest leads
    lead_counters = ingest_leads(drop_path, db, drop_name)
    print(
        f"Leads: total_rows={lead_counters['total_rows']}, "
        f"rejected={lead_counters['rejected_rows']}, "
        f"by_reason={lead_counters['by_reason']}, "
        f"duplicate_merges={lead_counters['duplicate_merges']}, "
        f"field_warnings={lead_counters['field_warnings']}"
    )

    # Ingest events
    event_counters = ingest_events(drop_path, db, drop_name)
    print(
        f"Events: total={event_counters['events_total']}, "
        f"malformed={event_counters['malformed_events']}, "
        f"duplicates={event_counters['duplicate_events_dropped']}, "
        f"orphans={event_counters['orphan_events']}, "
        f"unknown_disp={event_counters['unknown_disposition_events']}"
    )

    # Apply defaults to leads (e.g. preferred_language -> en)
    from .ingest import apply_defaults
    apply_defaults(db)

    # Commit the transaction
    db.commit()

    # Close
    db.close()

    print(f"Ingest completed for {drop_name}.")
    return 0


def cmd_report(out_dir: str) -> int:
    """Handle the report command."""
    # Determine the DB path
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".ledger.db")

    # Initialize the database (just to open it, no key needed if already set)
    from .db import set_pii_key, init_db
    pii_key = os.environ.get("LEDGER_PII_KEY", DEF_PII_KEY)
    set_pii_key(pii_key)
    db = init_db(db_path, pii_key)

    # Ensure output directory exists
    os.makedirs(out_dir, exist_ok=True)

    # 1. Generate batch_summary.csv
    batch_summary = generate_batch_summary(db)
    batch_path = os.path.join(out_dir, "batch_summary.csv")
    with open(batch_path, "w", encoding="utf-8", newline="") as f:
        f.write(batch_summary + "\n")
    print(f"Written: {batch_path}")

    # 2. Generate daily_overview.csv
    daily_overview = generate_daily_overview(db)
    daily_path = os.path.join(out_dir, "daily_overview.csv")
    with open(daily_path, "w", encoding="utf-8", newline="") as f:
        f.write(daily_overview + "\n")
    print(f"Written: {daily_path}")

    # 3. Generate lead_report.csv
    lead_report = generate_lead_report(db)
    lead_path = os.path.join(out_dir, "lead_report.csv")
    with open(lead_path, "w", encoding="utf-8", newline="") as f:
        f.write(lead_report + "\n")
    print(f"Written: {lead_path}")

    # 4. Generate rejections.csv
    rejections = generate_rejections(db)
    rejections_path = os.path.join(out_dir, "rejections.csv")
    with open(rejections_path, "w", encoding="utf-8", newline="") as f:
        f.write(rejections + "\n")
    print(f"Written: {rejections_path}")

    # 5. Generate data_quality.json
    quality = generate_data_quality(db)
    quality_path = os.path.join(out_dir, "data_quality.json")
    with open(quality_path, "w", encoding="utf-8") as f:
        json.dump(quality, f, indent=2)
    print(f"Written: {quality_path}")

    # Close
    db.close()

    print("Report generation completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())