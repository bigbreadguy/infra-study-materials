"""Pure helpers turning a scraper result envelope into BigQuery-ready NDJSON.

The scraper writes one pretty-printed envelope object per recipe
(``{recipe, params, status, data: [...], ...}``). BigQuery external tables need
newline-delimited JSON, and the records' keys are Korean and contain spaces
(e.g. ``"국내수입 물량"``), which are awkward as external-table column names. So
each record is wrapped under an ASCII ``row`` JSON column; the transform SQL then
reads fields via ``JSON_VALUE(row, '$["국내수입 물량"]')``.

Kept free of Airflow/GCP imports so it is unit-testable in isolation, mirroring
common/scrape_request.py.
"""

from __future__ import annotations

import json
from typing import Any


def envelope_records(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the envelope's data records, or [] when there are none.

    Only ``status == "success"`` envelopes contribute rows; a failed or empty
    envelope yields no records so the caller can skip the load cleanly.
    """
    if not isinstance(envelope, dict):
        raise ValueError("result envelope must be a json object")
    if envelope.get("status") != "success":
        return []
    data = envelope.get("data")
    if not data:
        return []
    if not isinstance(data, list):
        raise ValueError("result envelope data must be a json array")
    records = []
    for index, record in enumerate(data):
        if not isinstance(record, dict):
            raise ValueError(f"result envelope data[{index}] must be a json object")
        records.append(record)
    return records


def records_to_ndjson(records: list[dict[str, Any]]) -> str:
    """Wrap each record under an ASCII ``row`` key and join as NDJSON.

    ensure_ascii=False keeps the Korean values readable in GCS and lets BigQuery
    parse them as UTF-8. Returns "" for no records.
    """
    lines = [
        json.dumps({"row": record}, ensure_ascii=False, separators=(",", ":"))
        for record in records
    ]
    return "\n".join(lines)


def envelope_to_ndjson(envelope: dict[str, Any]) -> str:
    """Convenience: envelope -> wrapped NDJSON (empty string when no rows)."""
    return records_to_ndjson(envelope_records(envelope))
