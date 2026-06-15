from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from common.bigquery_market_index_sql import (
    RAW_DATA_SAMPLES_TABLE,
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
    grain_description: str | None = None
    time_grain: str | None = None


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


def raw_query_job_config(raw_gcs_uri: str) -> Any:
    """Job config resolving RAW_DATA_SAMPLES_TABLE to one run's GCS object.

    The temporary external table definition lives only inside the job, so no
    persistent raw table is created and concurrent grain chains cannot
    clobber each other's URI pointer.
    """
    # Deferred import keeps this module importable in test environments
    # without google-cloud-bigquery; the pure SQL builders need no GCP libs.
    # pyrefly: ignore [missing-import]
    from google.cloud import bigquery

    external_config = bigquery.ExternalConfig.from_api_repr(
        raw_external_table_definition(raw_gcs_uri)
    )
    return bigquery.QueryJobConfig(
        table_definitions={RAW_DATA_SAMPLES_TABLE: external_config}
    )


def execute_bigquery_script(
    client: Any,
    sql: str,
    *,
    region: str,
    job_config: Any = None,
) -> dict[str, str | None]:
    job = client.query(sql, location=region, job_config=job_config)
    job.result()

    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", region),
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
        grain_description=config.grain_description,
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
        time_grain=config.time_grain or "D",
    )


def _execute_with_raw_definition(
    client: Any,
    config: BigQueryTransformConfig,
    sql: str,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        sql,
        region=config.region,
        job_config=raw_query_job_config(config.raw_gcs_uri),
    )


def run_validate_raw_data(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return _execute_with_raw_definition(
        client,
        config,
        validate_raw_data_sql(config),
    )


def run_merge_dim_grains(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return _execute_with_raw_definition(
        client,
        config,
        merge_dim_grains_sql(config),
    )


def run_merge_dim_metrics(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return _execute_with_raw_definition(
        client,
        config,
        merge_dim_metrics_sql(config),
    )


def run_merge_fact_values(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return _execute_with_raw_definition(
        client,
        config,
        merge_fact_values_sql(config),
    )
