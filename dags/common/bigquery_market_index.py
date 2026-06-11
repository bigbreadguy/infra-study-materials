from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from common.bigquery_market_index_sql import (
    RAW_DATA_SAMPLES_TABLE,
    dim_grains_merge_sql,
    dim_metrics_merge_sql,
    fact_values_merge_sql,
    raw_data_samples_sql,
)


@dataclass(frozen=True)
class BigQueryTransformConfig:
    project_id: str
    dataset_id: str
    region: str
    raw_gcs_uri: str
    raw_table_id: str = RAW_DATA_SAMPLES_TABLE
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
        "raw_table_id": config.raw_table_id,
    }
    for name, value in required_values.items():
        if not value:
            raise ValueError(f"{name} must be a non-empty string")


def execute_bigquery_script(
    client: Any,
    sql: str,
    *,
    region: str,
) -> dict[str, str | None]:
    job = client.query(sql, location=region)
    job.result()

    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", region),
    }


def create_raw_data_samples_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return raw_data_samples_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        raw_gcs_uri=config.raw_gcs_uri,
        raw_table_id=config.raw_table_id,
        expected_row_count=config.expected_raw_row_count,
    )


def merge_dim_grains_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return dim_grains_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        raw_table_id=config.raw_table_id,
        grain_description=config.grain_description,
    )


def merge_dim_metrics_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return dim_metrics_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        raw_table_id=config.raw_table_id,
    )


def merge_fact_values_sql(config: BigQueryTransformConfig) -> str:
    _validate_config(config)
    return fact_values_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        raw_table_id=config.raw_table_id,
        time_grain=config.time_grain or "D",
    )


def run_create_raw_data_samples(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        create_raw_data_samples_sql(config),
        region=config.region,
    )


def run_merge_dim_grains(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        merge_dim_grains_sql(config),
        region=config.region,
    )


def run_merge_dim_metrics(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        merge_dim_metrics_sql(config),
        region=config.region,
    )


def run_merge_fact_values(
    client: Any,
    config: BigQueryTransformConfig,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        merge_fact_values_sql(config),
        region=config.region,
    )
