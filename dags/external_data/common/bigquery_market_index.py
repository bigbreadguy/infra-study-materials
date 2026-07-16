from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from external_data.common.bigquery_market_index_sql import (
    RAW_DATA_SAMPLES_TABLE,
    combined_transform_sql,
    dim_grains_merge_sql,
    dim_metrics_merge_sql,
    fact_values_merge_sql,
    raw_data_samples_check_sql,
    raw_external_table_definition,
)


@dataclass(frozen=True)
class BigQueryTransformConfig:
    project_id: str
    dataset_id: str
    region: str
    raw_gcs_uri: str
    expected_raw_row_count: int | None = None
    # Per-grain time grain and description now ride in on raw columns, so the
    # transform config only needs to know how to read the raw objects and
    # whether to run the one-time legacy-duplicate sweep.
    raw_source_format: str = "NEWLINE_DELIMITED_JSON"
    dedup_cleanup: bool = False


QueryBuilder = Callable[[BigQueryTransformConfig], str]


def _validate_config(config: BigQueryTransformConfig) -> None:
    required_values = {
        "project_id": config.project_id,
        "dataset_id": config.dataset_id,
        "region": config.region,
        "raw_gcs_uri": config.raw_gcs_uri,
    }
    for name, value in required_values.items():
        if not value:
            raise ValueError(f"{name} must be a non-empty string")


def raw_query_job_config(
    raw_gcs_uri: str,
    labels: dict[str, str] | None = None,
    *,
    source_format: str = "NEWLINE_DELIMITED_JSON",
) -> Any:
    """Job config resolving RAW_DATA_SAMPLES_TABLE to one run's GCS objects.

    The temporary external table definition lives only inside the job, so no
    persistent raw table is created and concurrent runs cannot clobber each
    other's URI pointer. ``raw_gcs_uri`` is the run-scoped wildcard that
    captures every object the run uploaded, so one job reads all grains.
    ``source_format`` selects NDJSON (scheduled) or PARQUET (backfill); the
    merge SQL is identical for both. Optional labels ride the job for
    observability (filterable in the BigQuery console and INFORMATION_SCHEMA).
    """
    # Deferred import keeps this module importable in test environments
    # without google-cloud-bigquery; the pure SQL builders need no GCP libs.
    # pyrefly: ignore [missing-import]
    from google.cloud import bigquery

    external_config = bigquery.ExternalConfig.from_api_repr(
        raw_external_table_definition(raw_gcs_uri, source_format=source_format)
    )
    job_config = bigquery.QueryJobConfig(
        table_definitions={RAW_DATA_SAMPLES_TABLE: external_config}
    )
    if labels:
        job_config.labels = labels
    return job_config


def _dml_affected_rows(client: Any, job: Any) -> int | None:
    """Best-effort count of rows the terminal DML statement touched.

    Multi-statement scripts (the dim merges run MERGE + ASSERT; fact_values
    runs DECLARE/CREATE TEMP/SET/ASSERT/DELETE/MERGE) report no
    num_dml_affected_rows on the parent job, so fall back to the last child
    job that performed DML — the terminal MERGE, i.e. the net rows upserted
    into the target table. Defensive throughout: a client without job listing
    (e.g. a test fake) or any failure yields None so observability degrades to
    'n/a' rather than breaking the run.
    """
    affected = getattr(job, "num_dml_affected_rows", None)
    if affected is not None:
        return affected
    try:
        children = sorted(
            client.list_jobs(parent_job=job),
            key=lambda child: getattr(child, "created", None) or 0,
        )
        dml_rows = [
            child.num_dml_affected_rows
            for child in children
            if getattr(child, "num_dml_affected_rows", None) is not None
        ]
        return dml_rows[-1] if dml_rows else None
    except Exception:
        return None


def _child_statement_stats(client: Any, job: Any) -> list[dict[str, Any]]:
    """Ordered per-statement stats for a multi-statement script's child jobs.

    The combined transform runs as one parent script job; each statement
    (validate, the dim/fact MERGEs, the ASSERTs) surfaces as a child job.
    Walking them in creation order lets observability report per-step DML
    counts. Defensive: a client without job listing (a test fake) or any
    failure yields an empty list so the run still succeeds.
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


def execute_bigquery_script(
    client: Any,
    sql: str,
    *,
    region: str,
    job_config: Any = None,
    job_id_prefix: str | None = None,
) -> dict[str, str | int | None]:
    job = client.query(
        sql,
        location=region,
        job_config=job_config,
        job_id_prefix=job_id_prefix,
    )
    job.result()

    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", region),
        "dml_affected_rows": _dml_affected_rows(client, job),
    }


def validate_raw_data_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return raw_data_samples_check_sql(
        expected_row_count=config.expected_raw_row_count,
    )


def merge_dim_grains_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return dim_grains_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
    )


def merge_dim_metrics_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return dim_metrics_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
    )


def merge_fact_values_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return fact_values_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dedup_cleanup=config.dedup_cleanup,
    )


def transform_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return combined_transform_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        expected_row_count=config.expected_raw_row_count,
        dedup_cleanup=config.dedup_cleanup,
    )


def _execute_with_raw_definition(
    client: Any,
    config: BigQueryTransformConfig,
    sql: str,
    *,
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, str | int | None]:
    return execute_bigquery_script(
        client,
        sql,
        region=config.region,
        job_config=raw_query_job_config(
            config.raw_gcs_uri,
            labels=labels,
            source_format=config.raw_source_format,
        ),
        job_id_prefix=job_id_prefix,
    )


def run_combined_transform(
    client: Any,
    config: BigQueryTransformConfig,
    *,
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, Any]:
    """Run the whole transform for a run as ONE BigQuery job.

    Submits the combined multi-statement script over the run-scoped raw
    wildcard and returns the parent job id plus per-statement child stats so
    the single task can rebuild the per-step observability the per-grain task
    chain used to give for free.
    """
    job = client.query(
        transform_sql(config),
        location=config.region,
        job_config=raw_query_job_config(
            config.raw_gcs_uri,
            labels=labels,
            source_format=config.raw_source_format,
        ),
        job_id_prefix=job_id_prefix,
    )
    job.result()

    statements = _child_statement_stats(client, job)
    dml_rows = [
        statement["dml_affected_rows"]
        for statement in statements
        if statement["dml_affected_rows"] is not None
    ]
    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", config.region),
        "dml_affected_rows": (
            dml_rows[-1]
            if dml_rows
            else getattr(job, "num_dml_affected_rows", None)
        ),
        "statements": statements,
    }


def run_validate_raw_data(
    client: Any,
    config: BigQueryTransformConfig,
    *,
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, str | int | None]:
    return _execute_with_raw_definition(
        client,
        config,
        validate_raw_data_sql(config),
        labels=labels,
        job_id_prefix=job_id_prefix,
    )


def run_merge_dim_grains(
    client: Any,
    config: BigQueryTransformConfig,
    *,
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, str | int | None]:
    return _execute_with_raw_definition(
        client,
        config,
        merge_dim_grains_sql(config),
        labels=labels,
        job_id_prefix=job_id_prefix,
    )


def run_merge_dim_metrics(
    client: Any,
    config: BigQueryTransformConfig,
    *,
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, str | int | None]:
    return _execute_with_raw_definition(
        client,
        config,
        merge_dim_metrics_sql(config),
        labels=labels,
        job_id_prefix=job_id_prefix,
    )


def run_merge_fact_values(
    client: Any,
    config: BigQueryTransformConfig,
    *,
    labels: dict[str, str] | None = None,
    job_id_prefix: str | None = None,
) -> dict[str, str | int | None]:
    return _execute_with_raw_definition(
        client,
        config,
        merge_fact_values_sql(config),
        labels=labels,
        job_id_prefix=job_id_prefix,
    )
