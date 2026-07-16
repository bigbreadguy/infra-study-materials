"""Execution glue for the dl_materials transform-load.

Pairs the pure SQL builders in common/materials_bigquery_sql.py with a job-scoped
temporary external table over one run's raw object(s), so the whole transform-load
(including the idempotent CREATE TABLE IF NOT EXISTS bootstrap) runs as a single
BigQuery job and no persistent raw table is created.

Two writers share this glue: the KOSA scrape pipeline (one NDJSON object per
recipe) and the dpanda Bloomberg ingest (a run-scoped wildcard of NDJSON or
Parquet objects). Both shape each record as ``{"row": {<flat fields>}}`` so the
merge SQL reads fields with JSON_VALUE over the ``row`` column. NDJSON keeps
``row`` as a JSON column; Parquet stores it as a STRING holding the JSON text
(JSON_VALUE accepts STRING), so the merge SQL is identical across both.
"""

from __future__ import annotations

from typing import Any, Mapping

from external_data.common.materials_bigquery_sql import (
    RAW_RECORDS_TABLE,
    combined_materials_transform_sql,
    materials_transform_sql,
)


SUPPORTED_RAW_SOURCE_FORMATS = ("NEWLINE_DELIMITED_JSON", "PARQUET")


def raw_external_table_definition(
    raw_gcs_uri: str,
    *,
    source_format: str = "NEWLINE_DELIMITED_JSON",
) -> dict:
    """ExternalConfig API representation for one run's raw object(s).

    Each record is shaped ``{"row": {<scraped/extracted record>}}``. NDJSON keeps
    ``row`` as JSON so the merge SQL reads Korean, space-containing fields with
    JSON_VALUE; Parquet stores ``row`` as a STRING holding the same JSON text
    (JSON_VALUE accepts STRING), so the downstream SQL is identical. ``raw_gcs_uri``
    may be a single object or a run-scoped wildcard.
    """
    if not raw_gcs_uri:
        raise ValueError("raw_gcs_uri must be a non-empty string")
    if not raw_gcs_uri.startswith("gs://"):
        raise ValueError("raw_gcs_uri must be a gs:// URI")
    if source_format not in SUPPORTED_RAW_SOURCE_FORMATS:
        raise ValueError(
            "source_format must be one of "
            f"{SUPPORTED_RAW_SOURCE_FORMATS}, got {source_format!r}"
        )
    if source_format == "PARQUET":
        return {
            "sourceFormat": "PARQUET",
            "sourceUris": [raw_gcs_uri],
            "schema": {"fields": [{"name": "row", "type": "STRING"}]},
        }
    return {
        "sourceFormat": "NEWLINE_DELIMITED_JSON",
        "ignoreUnknownValues": True,
        "sourceUris": [raw_gcs_uri],
        "schema": {"fields": [{"name": "row", "type": "JSON"}]},
    }


def raw_query_job_config(
    raw_gcs_uri: str,
    labels: dict[str, str] | None = None,
    *,
    source_format: str = "NEWLINE_DELIMITED_JSON",
    table_name: str = RAW_RECORDS_TABLE,
) -> Any:
    """Job config resolving ``table_name`` to one run's GCS object(s).

    ``table_name`` must match the name the caller's transform SQL reads from —
    a mismatch leaves the SQL referencing an unqualified permanent table
    (BigQuery 400: must be qualified with a dataset).
    """
    # Deferred import keeps this module importable in test environments without
    # google-cloud-bigquery; the pure SQL builders need no GCP libs.
    from google.cloud import bigquery

    external_config = bigquery.ExternalConfig.from_api_repr(
        raw_external_table_definition(raw_gcs_uri, source_format=source_format)
    )
    job_config = bigquery.QueryJobConfig(
        table_definitions={table_name: external_config}
    )
    if labels:
        job_config.labels = labels
    return job_config


def _child_statement_stats(client: Any, job: Any) -> list[dict[str, Any]]:
    """Ordered per-statement stats for the multi-statement script's child jobs.

    The combined transform runs as one parent script job; each statement (the DDL,
    validate, the dim/fact MERGEs, the ASSERTs) surfaces as a child job. Walking
    them in creation order lets the single task report per-step DML counts.
    Defensive: a client without job listing (a test fake) or any failure yields an
    empty list so the run still succeeds.
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


def _terminal_dml_rows(statements: list[dict[str, Any]], job: Any) -> int | None:
    dml_rows = [
        statement["dml_affected_rows"]
        for statement in statements
        if statement["dml_affected_rows"] is not None
    ]
    if dml_rows:
        return dml_rows[-1]
    return getattr(job, "num_dml_affected_rows", None)


def _run_transform_job(
    client: Any,
    sql: str,
    *,
    region: str,
    raw_gcs_uri: str,
    source_format: str,
    labels: dict[str, str] | None,
    job_id_prefix: str | None,
) -> dict[str, Any]:
    """Run one transform script over a run's raw object(s) and report stats.

    Returns the parent job id, location, terminal DML row count, and per-statement
    child stats so a single task can rebuild per-step observability.
    """
    job = client.query(
        sql,
        location=region,
        job_config=raw_query_job_config(
            raw_gcs_uri, labels=labels, source_format=source_format
        ),
        job_id_prefix=job_id_prefix,
    )
    job.result()
    statements = _child_statement_stats(client, job)
    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", region),
        "dml_affected_rows": _terminal_dml_rows(statements, job),
        "statements": statements,
    }


def run_materials_transform(
    client: Any,
    *,
    project_id: str,
    dataset_id: str,
    region: str,
    raw_gcs_uri: str,
    config: Mapping[str, Any],
    expected_row_count: int | None = None,
    source_format: str = "NEWLINE_DELIMITED_JSON",
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, Any]:
    """Run a single recipe's whole transform-load (with table bootstrap) as one job."""
    sql = materials_transform_sql(
        project_id=project_id,
        dataset_id=dataset_id,
        config=config,
        expected_row_count=expected_row_count,
    )
    return _run_transform_job(
        client,
        sql,
        region=region,
        raw_gcs_uri=raw_gcs_uri,
        source_format=source_format,
        labels=labels,
        job_id_prefix=job_id_prefix,
    )


def run_combined_materials_transform(
    client: Any,
    *,
    project_id: str,
    dataset_id: str,
    region: str,
    raw_gcs_uri: str,
    configs: list[Mapping[str, Any]],
    expected_row_count: int | None = None,
    source_format: str = "NEWLINE_DELIMITED_JSON",
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, Any]:
    """Transform several recipes' raw (one per-source wildcard) in a single job.

    Collapses N per-recipe transform jobs into one over the run's per-source
    wildcard external table, so the shared dims/fact are MERGEd once per source per
    run instead of once per recipe -- staying under BigQuery's per-table update rate
    limit. ``configs`` must share a datasource (see
    :func:`combined_materials_transform_sql`).
    """
    sql = combined_materials_transform_sql(
        project_id=project_id,
        dataset_id=dataset_id,
        configs=configs,
        expected_row_count=expected_row_count,
    )
    return _run_transform_job(
        client,
        sql,
        region=region,
        raw_gcs_uri=raw_gcs_uri,
        source_format=source_format,
        labels=labels,
        job_id_prefix=job_id_prefix,
    )
