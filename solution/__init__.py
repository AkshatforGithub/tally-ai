"""Solution for Assignment A: Interaction Ledger & Reporting Pipeline."""
from .ingest import ingest_leads, ingest_events
from .reports import generate_reports

__all__ = ["ingest_leads", "ingest_events", "generate_reports"]