"""Execution glue for the cosmetics landing + star transform-load.

Pairs the pure SQL builders in ``cosmetics_bigquery_sql`` with a job-scoped temporary
external table over one run's staged envelope rows, so the whole transform (bootstrap DDL
+ landing MERGE + conformed dims + SCD2 dim_item + fact MERGE + derived view) runs as a
single BigQuery job — the pod only orchestrates BigQuery (design §7, CLAUDE.md pod-economy).

The external-table plumbing is shared with dl_materials (``raw_external_table_definition`` /
``raw_query_job_config``): each staged record is ``{"row": {<envelope row + lineage>}}`` so
the merge SQL reads fields via ``JSON_VALUE`` over the ``row`` column, identically for the
live NDJSON path and any Parquet re-stage.

``cosmetics_staging_ndjson`` is the pure envelope->NDJSON step (json only, unit-testable):
it flattens an envelope's ``data`` rows and stamps each with the envelope lineage
(``__snapshot_id`` / ``__envelope_uri`` / ``__scraped_at``) the ``stg`` projection reads.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from external_data.common.materials_bigquery import (
    raw_query_job_config,
)
from external_data.common.materials_result import envelope_records, _json_safe
from external_data.common.cosmetics_bigquery_sql import (
    RAW_RECORDS_TABLE,
    combined_cosmetics_transform_sql,
)


# --- staging (pure) --------------------------------------------------------

# Lineage keys stamped onto every staged row; double-underscored so they never collide
# with a scraper column and read cleanly via JSON_VALUE(row, '$."__snapshot_id"').
LINEAGE_SNAPSHOT_ID = "__snapshot_id"
LINEAGE_ENVELOPE_URI = "__envelope_uri"
LINEAGE_SCRAPED_AT = "__scraped_at"


def cosmetics_staging_ndjson(
    envelope: Mapping[str, Any],
    *,
    snapshot_id: str,
    envelope_uri: str,
) -> str:
    """One envelope -> wrapped NDJSON with per-row lineage stamped (empty when no rows).

    ``snapshot_id`` is the fact's degenerate lineage: the ``run_id`` for a live envelope or
    ``cnp_backfill/{YYYYMMDD}T{HHMMSS}`` for a backfill envelope (the DAG derives it from the
    envelope's GCS path). ``scraped_at`` comes from the envelope. Only ``status=="success"``
    envelopes with rows contribute (``envelope_records`` enforces this).
    """
    if not snapshot_id:
        raise ValueError("snapshot_id must be a non-empty string")
    if not envelope_uri:
        raise ValueError("envelope_uri must be a non-empty string")
    scraped_at = (envelope or {}).get("scraped_at")
    lines = []
    for record in envelope_records(dict(envelope)):
        stamped = dict(record)
        stamped[LINEAGE_SNAPSHOT_ID] = snapshot_id
        stamped[LINEAGE_ENVELOPE_URI] = envelope_uri
        stamped[LINEAGE_SCRAPED_AT] = scraped_at
        lines.append(
            json.dumps({"row": _json_safe(stamped)}, ensure_ascii=False, separators=(",", ":"))
        )
    return "\n".join(lines)


# --- transform (BigQuery) --------------------------------------------------


def _child_statement_stats(client: Any, job: Any) -> list[dict[str, Any]]:
    """Ordered per-statement stats for the multi-statement script's child jobs.

    Each statement (DDL, stg, checks, dim/fact MERGEs, view) surfaces as a child job;
    walking them in creation order gives per-step DML counts. Defensive: a client without
    ``list_jobs`` (a test fake) or any failure yields an empty list.
    """
    try:
        children = sorted(
            client.list_jobs(parent_job=job),
            key=lambda child: getattr(child, "created", None) or 0,
        )
    except Exception:
        return []
    return [
        {
            "job_id": getattr(child, "job_id", None),
            "statement_type": getattr(child, "statement_type", None),
            "dml_affected_rows": getattr(child, "num_dml_affected_rows", None),
        }
        for child in children
    ]


def run_cosmetics_transform(
    client: Any,
    *,
    project_id: str,
    landing_dataset_id: str,
    star_dataset_id: str,
    region: str,
    raw_gcs_uri: str,
    expected_row_count: int | None = None,
    source_format: str = "NEWLINE_DELIMITED_JSON",
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, Any]:
    """Run the whole cosmetics transform over a run's staged object(s) as one job.

    Returns the parent job id, location, terminal DML row count (the fact MERGE, ordered
    last), and per-statement child stats so the single load task can rebuild per-step
    observability (the dpanda/materials convention).
    """
    sql = combined_cosmetics_transform_sql(
        project_id=project_id,
        landing_dataset_id=landing_dataset_id,
        star_dataset_id=star_dataset_id,
        expected_row_count=expected_row_count,
    )
    job = client.query(
        sql,
        location=region,
        job_config=raw_query_job_config(
            raw_gcs_uri,
            labels=labels,
            source_format=source_format,
            table_name=RAW_RECORDS_TABLE,
        ),
        job_id_prefix=job_id_prefix,
    )
    job.result()
    statements = _child_statement_stats(client, job)
    dml_rows = [s["dml_affected_rows"] for s in statements if s["dml_affected_rows"] is not None]
    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", region),
        "dml_affected_rows": dml_rows[-1] if dml_rows else getattr(job, "num_dml_affected_rows", None),
        "statements": statements,
    }
