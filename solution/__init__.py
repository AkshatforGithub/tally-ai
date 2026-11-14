"""Solution for Assignment A: Interaction Ledger & Reporting Pipeline."""
from .ingest import ingest_leads, ingest_events

__all__ = ["ingest_leads", "ingest_events"]