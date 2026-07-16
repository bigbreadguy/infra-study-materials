from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from external_data.common.bigquery_grain_metadata_sql import (
    dim_grain_metadata_merge_sql,
    raw_grain_catalog_sql,
)
from external_data.common.bigquery_market_index import execute_bigquery_script


@dataclass(frozen=True)
class GrainMetadataTransformConfig:
    project_id: str
    dataset_id: str
    region: str
    raw_gcs_uri: str
    expected_raw_row_count: int | None = None


def _validate_config(config: GrainMetadataTransformConfig) -> None:
    required_values = {
        "project_id": config.project_id,
        "dataset_id": config.dataset_id,
        "region": config.region,
        "raw_gcs_uri": config.raw_gcs_uri,
    }
    for name, value in required_values.items():
        if not value:
            raise ValueError(f"{name} must be a non-empty string")


def create_raw_grain_catalog_sql(config: GrainMetadataTransformConfig) -> str:
    _validate_config(config)
    return raw_grain_catalog_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        raw_gcs_uri=config.raw_gcs_uri,
        expected_row_count=config.expected_raw_row_count,
    )


def merge_dim_grain_metadata_sql(config: GrainMetadataTransformConfig) -> str:
    _validate_config(config)
    return dim_grain_metadata_merge_sql(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
    )


def run_create_raw_grain_catalog(
    client: Any,
    config: GrainMetadataTransformConfig,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        create_raw_grain_catalog_sql(config),
        region=config.region,
    )


def run_merge_dim_grain_metadata(
    client: Any,
    config: GrainMetadataTransformConfig,
) -> dict[str, str | None]:
    return execute_bigquery_script(
        client,
        merge_dim_grain_metadata_sql(config),
        region=config.region,
    )
