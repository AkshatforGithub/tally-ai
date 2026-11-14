"""Load the lead schemas and the disposition catalogue shipped with the assignment."""
import csv
import json
from pathlib import Path

DEFAULT_DIR = Path(__file__).resolve().parents[1] / "assignment-a-data" / "schemas"


def load_schemas(directory=DEFAULT_DIR):
    """workflow_id -> lead schema (the *.json files)."""
    schemas = {}
    for path in sorted(Path(directory).glob("*.json")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        schemas[schema["workflow_id"]] = schema
    return schemas


def load_dispositions(directory=DEFAULT_DIR):
    """(workflow_id, code) -> (is_terminal, is_success)."""
    out = {}
    with open(Path(directory) / "dispositions.csv", newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            out[(row["workflow_id"], row["code"])] = (
                row["is_terminal"].strip().lower() == "true",
                row["is_success"].strip().lower() == "true",
            )
    return out
