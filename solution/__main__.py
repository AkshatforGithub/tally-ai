"""Command line: python -m solution ingest <drop_dir> | report --out <dir>"""
import argparse
import os
import sys

from .catalogue import DEFAULT_DIR, load_dispositions, load_schemas
from .db import connect, load_key
from .ingest import FileChanged, ingest_drop
from .reports import write_reports


def main(argv=None):
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=os.environ.get("LEDGER_DB", "tally.db"),
                        help="SQLite file (default: $LEDGER_DB or tally.db)")
    common.add_argument("--schemas", default=str(DEFAULT_DIR),
                        help="directory with the lead schemas and dispositions.csv")
    parser = argparse.ArgumentParser(prog="python -m solution",
                                     description="Interaction ledger and reporting pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    p_ingest = sub.add_parser("ingest", parents=[common], help="ingest one drop directory")
    p_ingest.add_argument("drop_dir")
    p_report = sub.add_parser("report", parents=[common], help="write the five report files")
    p_report.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    try:
        conn = connect(args.db)
        if args.command == "ingest":
            ingested, skipped = ingest_drop(conn, args.drop_dir, load_schemas(args.schemas), load_key())
            print(f"ingested {len(ingested)} file(s), skipped {len(skipped)} already ingested")
        else:
            write_reports(conn, load_dispositions(args.schemas), args.out)
            print(f"reports written to {args.out}")
    except (RuntimeError, FileChanged, ValueError) as exc:
        sys.exit(f"error: {exc}")
    finally:
        conn.close() if "conn" in locals() else None


if __name__ == "__main__":
    main()
