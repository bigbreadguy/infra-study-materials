"""Execution glue for the dl_materials transform-load.

Pairs the pure SQL builders in common/materials_bigquery_sql.py with a job-scoped
temporary external table over one recipe's NDJSON object, so the whole
transform-load runs as a single BigQuery job and no persistent raw table is
created. Mirrors common/bigquery_market_index.py.
"""

from __future__ import annotations

from typing import Any, Mapping

from common.materials_bigquery_sql import (
    RAW_RECORDS_TABLE,
    materials_transform_sql,
)


def raw_external_table_definition(raw_gcs_uri: str) -> dict:
    """ExternalConfig API representation for one recipe's NDJSON object.

    Each record is one line shaped ``{"row": {<scraped record>}}``; the wrapped
    ``row`` column is JSON so the merge SQL reads Korean, space-containing fields
    with JSON_VALUE. ignoreUnknownValues tolerates any extra top-level keys.
    """
    if not raw_gcs_uri:
        raise ValueError("raw_gcs_uri must be a non-empty string")
    if not raw_gcs_uri.startswith("gs://"):
        raise ValueError("raw_gcs_uri must be a gs:// URI")
    return {
        "sourceFormat": "NEWLINE_DELIMITED_JSON",
        "ignoreUnknownValues": True,
        "sourceUris": [raw_gcs_uri],
        "schema": {"fields": [{"name": "row", "type": "JSON"}]},
    }


def raw_query_job_config(raw_gcs_uri: str) -> Any:
    """Job config resolving RAW_RECORDS_TABLE to one recipe's GCS object."""
    # Deferred import keeps this module importable in test environments without
    # google-cloud-bigquery; the pure SQL builders need no GCP libs.
    # pyrefly: ignore [missing-import]
    from google.cloud import bigquery

    external_config = bigquery.ExternalConfig.from_api_repr(
        raw_external_table_definition(raw_gcs_uri)
    )
    return bigquery.QueryJobConfig(
        table_definitions={RAW_RECORDS_TABLE: external_config}
    )


def run_materials_transform(
    client: Any,
    *,
    project_id: str,
    dataset_id: str,
    region: str,
    raw_gcs_uri: str,
    config: Mapping[str, Any],
    expected_row_count: int | None = None,
) -> dict[str, str | None]:
    """Run the whole transform-load for one recipe as a single BigQuery job."""
    sql = materials_transform_sql(
        project_id=project_id,
        dataset_id=dataset_id,
        config=config,
        expected_row_count=expected_row_count,
    )
    job = client.query(
        sql,
        location=region,
        job_config=raw_query_job_config(raw_gcs_uri),
    )
    job.result()
    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", region),
    }
