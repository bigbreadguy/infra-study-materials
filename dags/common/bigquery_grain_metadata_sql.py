from __future__ import annotations

from common.bigquery_market_index_sql import qualified_table


RAW_GRAIN_CATALOG_TABLE = "raw_grain_catalog"
DIM_GRAIN_METADATA_TABLE = "dim_grain_metadata"


def _sql_string(value: str, label: str) -> str:
    if not value:
        raise ValueError(f"{label} must be a non-empty string")
    if "'" in value or "\\" in value:
        raise ValueError(f"{label} must not contain quotes or backslashes")

    return f"'{value}'"


def raw_grain_catalog_sql(
    *,
    project_id: str,
    dataset_id: str,
    raw_gcs_uri: str,
    expected_row_count: int | None = None,
) -> str:
    raw_table = qualified_table(project_id, dataset_id, RAW_GRAIN_CATALOG_TABLE)
    raw_uri = _sql_string(raw_gcs_uri, "raw_gcs_uri")

    sql = f"""-- The extract task builds each NDJSON line itself with plain string keys, so
-- only the preserved catalog info object stays JSON at the boundary.
CREATE OR REPLACE EXTERNAL TABLE {raw_table} (
  datasetId STRING,
  grainId STRING,
  catalogId STRING,
  catalogName STRING,
  info JSON
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  ignore_unknown_values = true,
  uris = [{raw_uri}]
);"""

    if expected_row_count is None:
        return sql

    if not isinstance(expected_row_count, int) or expected_row_count < 0:
        raise ValueError("expected_row_count must be a non-negative integer")

    return f"""{sql}

ASSERT (
  SELECT COUNT(*)
  FROM {raw_table}
) = {expected_row_count} AS 'Raw external table row count must match the extracted document count.';"""


def dim_grain_metadata_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
) -> str:
    raw_table = qualified_table(project_id, dataset_id, RAW_GRAIN_CATALOG_TABLE)
    metadata_table = qualified_table(
        project_id,
        dataset_id,
        DIM_GRAIN_METADATA_TABLE,
    )

    return f"""MERGE {metadata_table} AS target
USING (
  -- The extract task already emits one line per dataset id and grain id
  -- pair; the grouping only guards the merge against a hand-edited raw
  -- object carrying duplicates.
  SELECT
    datasetId AS id,
    grainId AS name,
    ARRAY_AGG(catalogId ORDER BY catalogId LIMIT 1)[OFFSET(0)] AS catalog_id,
    ARRAY_AGG(catalogName ORDER BY catalogId LIMIT 1)[OFFSET(0)] AS catalog_name,
    ARRAY_AGG(info ORDER BY catalogId LIMIT 1)[OFFSET(0)] AS info
  FROM {raw_table}
  WHERE datasetId IS NOT NULL
    AND grainId IS NOT NULL
  GROUP BY datasetId, grainId
) AS source
-- The id and name pair mirrors dim_grains, so dim_grain_metadata joins
-- dim_grains one to one on (id, name).
ON target.id = source.id
  AND target.name = source.name
WHEN MATCHED THEN
  UPDATE SET
    catalog_id = source.catalog_id,
    catalog_name = source.catalog_name,
    info = source.info,
    ingested_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (id, name, catalog_id, catalog_name, info, ingested_at)
  VALUES (
    source.id,
    source.name,
    source.catalog_id,
    source.catalog_name,
    source.info,
    CURRENT_TIMESTAMP()
  );

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT id, name
    FROM {metadata_table}
    GROUP BY id, name
    HAVING COUNT(*) > 1
  )
) = 0 AS 'dim_grain_metadata must keep one row per dataset id and grain name pair.';"""
