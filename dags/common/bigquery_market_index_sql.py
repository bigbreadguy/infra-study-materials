from __future__ import annotations

from dataclasses import dataclass


RAW_DATA_SAMPLES_TABLE = "raw_data_samples"
DIM_GRAINS_TABLE = "dim_grains"
DIM_METRICS_TABLE = "dim_metrics"
FACT_VALUES_TABLE = "fact_values"


@dataclass(frozen=True)
class MarketIndexTables:
    project_id: str
    dataset_id: str
    raw_table_id: str = RAW_DATA_SAMPLES_TABLE

    @property
    def raw_data_samples(self) -> str:
        return qualified_table(
            self.project_id,
            self.dataset_id,
            self.raw_table_id,
        )

    @property
    def dim_grains(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, DIM_GRAINS_TABLE)

    @property
    def dim_metrics(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, DIM_METRICS_TABLE)

    @property
    def fact_values(self) -> str:
        return qualified_table(
            self.project_id,
            self.dataset_id,
            FACT_VALUES_TABLE,
        )


def _validate_identifier_part(value: str, label: str) -> str:
    if not value:
        raise ValueError(f"{label} must be a non-empty string")
    if "`" in value or "." in value:
        raise ValueError(f"{label} must not contain backticks or dots")

    return value


def qualified_table(project_id: str, dataset_id: str, table_id: str) -> str:
    parts = (
        _validate_identifier_part(project_id, "project_id"),
        _validate_identifier_part(dataset_id, "dataset_id"),
        _validate_identifier_part(table_id, "table_id"),
    )
    return f"`{'.'.join(parts)}`"


def _sql_string(value: str, label: str) -> str:
    if not value:
        raise ValueError(f"{label} must be a non-empty string")
    if "'" in value or "\\" in value:
        raise ValueError(f"{label} must not contain quotes or backslashes")

    return f"'{value}'"


def raw_data_samples_sql(
    *,
    project_id: str,
    dataset_id: str,
    raw_gcs_uri: str,
    raw_table_id: str = RAW_DATA_SAMPLES_TABLE,
    expected_row_count: int | None = None,
) -> str:
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
        raw_table_id=raw_table_id,
    )
    raw_uri = _sql_string(raw_gcs_uri, "raw_gcs_uri")

    sql = f"""-- Mongo exports use Extended JSON for ObjectId and Date fields. Keep those
-- fields as JSON at the external-table boundary and normalize them downstream.
CREATE OR REPLACE EXTERNAL TABLE {tables.raw_data_samples} (
  _id JSON,
  datasetId JSON,
  ts JSON,
  grainId STRING,
  _schema STRING,
  description STRING,
  createdAt JSON,
  data JSON,
  updatedAt JSON
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
  FROM {tables.raw_data_samples}
) = {expected_row_count} AS 'Raw external table row count must match the extracted document count.';"""


def dim_grains_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
    raw_table_id: str = RAW_DATA_SAMPLES_TABLE,
    grain_description: str | None = None,
) -> str:
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
        raw_table_id=raw_table_id,
    )

    # The curated description from the grain targets variable wins over
    # whatever the raw Mongo documents carry, which is usually nothing.
    raw_description_expr = (
        "NULLIF(ARRAY_AGG(COALESCE(description, '') "
        "ORDER BY source_file_name DESC LIMIT 1)[OFFSET(0)], '')"
    )
    if grain_description is None:
        description_expr = raw_description_expr
    else:
        description_expr = (
            f"COALESCE({_sql_string(grain_description, 'grain_description')}, "
            f"{raw_description_expr})"
        )

    return f"""MERGE {tables.dim_grains} AS target
USING (
  WITH normalized AS (
    SELECT
      COALESCE(JSON_VALUE(datasetId, '$."$oid"'), JSON_VALUE(datasetId, '$')) AS id,
      COALESCE(grainId, JSON_VALUE(data, '$.grainId')) AS name,
      COALESCE(description, JSON_VALUE(data, '$.description')) AS description,
      _FILE_NAME AS source_file_name
    FROM {tables.raw_data_samples}
  )
  SELECT
    id,
    name,
    {description_expr} AS description
  FROM normalized
  WHERE id IS NOT NULL
    AND name IS NOT NULL
  GROUP BY id, name
) AS source
-- A dataset holds several grains, so grain identity is the dataset id and
-- grain name pair; keying on the dataset id alone made sibling grains fight
-- over one row.
ON target.id = source.id
  AND target.name = source.name
WHEN MATCHED THEN
  UPDATE SET
    description = source.description
WHEN NOT MATCHED THEN
  INSERT (id, name, description)
  VALUES (source.id, source.name, source.description);

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT id, name
    FROM {tables.dim_grains}
    GROUP BY id, name
    HAVING COUNT(*) > 1
  )
) = 0 AS 'dim_grains must keep one row per dataset id and grain name pair.';"""


def dim_metrics_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
    raw_table_id: str = RAW_DATA_SAMPLES_TABLE,
) -> str:
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
        raw_table_id=raw_table_id,
    )

    return f"""MERGE {tables.dim_metrics} AS target
USING (
  WITH raw_samples AS (
    SELECT
      COALESCE(JSON_VALUE(raw.datasetId, '$."$oid"'), JSON_VALUE(raw.datasetId, '$')) AS grain_id,
      COALESCE(raw.grainId, JSON_VALUE(raw.data, '$.grainId')) AS grain_name,
      COALESCE(raw.description, JSON_VALUE(raw.data, '$.description')) AS grain_description,
      raw.data
    FROM {tables.raw_data_samples} AS raw
  ),
  metric_rows AS (
    SELECT
      raw.grain_id,
      raw.grain_name,
      raw.grain_description,
      metric.metric_suffix,
      metric.description_suffix,
      metric.metric_value
    FROM raw_samples AS raw
    JOIN {tables.dim_grains} AS grain
      ON grain.id = raw.grain_id
      AND grain.name = raw.grain_name
    CROSS JOIN UNNEST([
      STRUCT(
        'Open' AS metric_suffix,
        'open value' AS description_suffix,
        COALESCE(JSON_VALUE(raw.data, '$.open'), JSON_VALUE(raw.data, '$.Open')) AS metric_value
      ),
      STRUCT(
        'High' AS metric_suffix,
        'high value' AS description_suffix,
        COALESCE(JSON_VALUE(raw.data, '$.high'), JSON_VALUE(raw.data, '$.High')) AS metric_value
      ),
      STRUCT(
        'Low' AS metric_suffix,
        'low value' AS description_suffix,
        COALESCE(JSON_VALUE(raw.data, '$.low'), JSON_VALUE(raw.data, '$.Low')) AS metric_value
      ),
      STRUCT(
        'Close' AS metric_suffix,
        'close value' AS description_suffix,
        COALESCE(JSON_VALUE(raw.data, '$.close'), JSON_VALUE(raw.data, '$.Close')) AS metric_value
      ),
      STRUCT(
        'Volume' AS metric_suffix,
        'volume value' AS description_suffix,
        COALESCE(JSON_VALUE(raw.data, '$.volume'), JSON_VALUE(raw.data, '$.Volume')) AS metric_value
      ),
      STRUCT(
        'OpenInterest' AS metric_suffix,
        'open interest value' AS description_suffix,
        COALESCE(
          JSON_VALUE(raw.data, '$.oi'),
          JSON_VALUE(raw.data, '$.openInterest'),
          JSON_VALUE(raw.data, '$.OpenInterest')
        ) AS metric_value
      ),
      STRUCT(
        'Value' AS metric_suffix,
        'single numeric value' AS description_suffix,
        JSON_VALUE(raw.data, '$.value') AS metric_value
      )
    ]) AS metric
    WHERE raw.grain_id IS NOT NULL
      AND raw.grain_name IS NOT NULL
      AND NULLIF(metric.metric_value, '') IS NOT NULL
  )
  SELECT
    grain_id,
    CONCAT(ARRAY_AGG(grain_name IGNORE NULLS ORDER BY grain_name LIMIT 1)[OFFSET(0)], '_', metric_suffix) AS name,
    ARRAY_AGG(
      CONCAT(COALESCE(grain_description, grain_name), ' ', description_suffix)
      ORDER BY grain_name
      LIMIT 1
    )[OFFSET(0)] AS description
  FROM metric_rows
  GROUP BY grain_id, metric_suffix
) AS source
ON target.grain_id = source.grain_id
  AND target.name = source.name
WHEN MATCHED THEN
  UPDATE SET
    description = source.description
WHEN NOT MATCHED THEN
  INSERT (id, grain_id, name, description)
  VALUES (GENERATE_UUID(), source.grain_id, source.name, source.description);

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT grain_id, name
    FROM {tables.dim_metrics}
    GROUP BY grain_id, name
    HAVING COUNT(*) > 1
  )
) = 0 AS 'dim_metrics must keep one row per grain id and metric name pair.';"""


def fact_values_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
    raw_table_id: str = RAW_DATA_SAMPLES_TABLE,
) -> str:
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
        raw_table_id=raw_table_id,
    )

    return f"""DECLARE min_candidate_logical_date DATE DEFAULT NULL;
DECLARE max_candidate_logical_date DATE DEFAULT NULL;

CREATE TEMP TABLE fact_candidates AS
WITH raw_samples AS (
  SELECT
    COALESCE(JSON_VALUE(raw._id, '$."$oid"'), JSON_VALUE(raw._id, '$')) AS sample_id,
    COALESCE(JSON_VALUE(raw.datasetId, '$."$oid"'), JSON_VALUE(raw.datasetId, '$')) AS grain_id,
    COALESCE(raw.grainId, JSON_VALUE(raw.data, '$.grainId')) AS grain_name,
    COALESCE(JSON_VALUE(raw.data, '$.dt'), JSON_VALUE(raw.data, '$.date')) AS source_logical_date,
    COALESCE(JSON_VALUE(raw.ts, '$."$date"'), JSON_VALUE(raw.ts, '$')) AS source_ts,
    raw._FILE_NAME AS source_file_name,
    COALESCE(JSON_VALUE(raw.updatedAt, '$."$date"'), JSON_VALUE(raw.updatedAt, '$')) AS source_updated_at,
    raw.data
  FROM {tables.raw_data_samples} AS raw
),
metric_rows AS (
  SELECT
    raw.sample_id,
    raw.grain_id,
    raw.grain_name,
    raw.source_logical_date,
    raw.source_ts,
    raw.source_file_name,
    raw.source_updated_at,
    metric.metric_suffix,
    metric.metric_value
  FROM raw_samples AS raw
  CROSS JOIN UNNEST([
    STRUCT(
      'Open' AS metric_suffix,
      COALESCE(JSON_VALUE(raw.data, '$.open'), JSON_VALUE(raw.data, '$.Open')) AS metric_value
    ),
    STRUCT(
      'High' AS metric_suffix,
      COALESCE(JSON_VALUE(raw.data, '$.high'), JSON_VALUE(raw.data, '$.High')) AS metric_value
    ),
    STRUCT(
      'Low' AS metric_suffix,
      COALESCE(JSON_VALUE(raw.data, '$.low'), JSON_VALUE(raw.data, '$.Low')) AS metric_value
    ),
    STRUCT(
      'Close' AS metric_suffix,
      COALESCE(JSON_VALUE(raw.data, '$.close'), JSON_VALUE(raw.data, '$.Close')) AS metric_value
    ),
    STRUCT(
      'Volume' AS metric_suffix,
      COALESCE(JSON_VALUE(raw.data, '$.volume'), JSON_VALUE(raw.data, '$.Volume')) AS metric_value
    ),
    STRUCT(
      'OpenInterest' AS metric_suffix,
      COALESCE(
        JSON_VALUE(raw.data, '$.oi'),
        JSON_VALUE(raw.data, '$.openInterest'),
        JSON_VALUE(raw.data, '$.OpenInterest')
      ) AS metric_value
    ),
    STRUCT(
      'Value' AS metric_suffix,
      JSON_VALUE(raw.data, '$.value') AS metric_value
    )
  ]) AS metric
  WHERE raw.sample_id IS NOT NULL
    AND raw.grain_id IS NOT NULL
    AND raw.grain_name IS NOT NULL
    AND NULLIF(metric.metric_value, '') IS NOT NULL
),
typed_rows AS (
  SELECT
    sample_id,
    grain_id,
    grain_name,
    COALESCE(
      SAFE_CAST(source_logical_date AS DATE),
      DATE(SAFE_CAST(source_logical_date AS TIMESTAMP)),
      DATE(SAFE_CAST(source_ts AS TIMESTAMP)),
      SAFE.PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(source_file_name, r'raw-([0-9]{{8}})T')),
      SAFE.PARSE_DATE('%Y/%m/%d', REGEXP_EXTRACT(source_file_name, r'/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'))
    ) AS logical_date,
    'D' AS time_grain,
    CONCAT(grain_name, '_', metric_suffix) AS metric_name,
    metric_value,
    source_file_name,
    COALESCE(
      SAFE_CAST(source_updated_at AS TIMESTAMP),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', source_updated_at),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', source_updated_at),
      SAFE_CAST(source_ts AS TIMESTAMP),
      CURRENT_TIMESTAMP()
    ) AS updated_at
  FROM metric_rows
)
SELECT *
FROM typed_rows
WHERE logical_date IS NOT NULL
-- Stale raw exports can carry the same Mongo document in multiple files;
-- keep only the freshest row per merge key so MERGE never sees duplicates.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sample_id, grain_id, metric_name, logical_date, time_grain
  ORDER BY updated_at DESC, source_file_name DESC
) = 1;

SET min_candidate_logical_date = COALESCE(
  (SELECT MIN(logical_date) FROM fact_candidates),
  DATE '1900-01-01'
);
SET max_candidate_logical_date = COALESCE(
  (SELECT MAX(logical_date) FROM fact_candidates),
  DATE '1900-01-01'
);

ASSERT (
  SELECT COUNT(*)
  FROM fact_candidates AS fact
  LEFT JOIN {tables.dim_grains} AS grain
    ON grain.id = fact.grain_id
    AND grain.name = fact.grain_name
  LEFT JOIN {tables.dim_metrics} AS metric
    ON metric.grain_id = fact.grain_id
    AND metric.name = fact.metric_name
  WHERE grain.id IS NULL
    OR metric.id IS NULL
) = 0 AS 'Every fact candidate must resolve dim_grains and dim_metrics before merging.';

-- Earlier runs inserted duplicate fact rows before source dedup existed; drop
-- every duplicate but the latest ingested row so MERGE matches one target row.
DELETE FROM {tables.fact_values}
WHERE logical_date BETWEEN min_candidate_logical_date AND max_candidate_logical_date
  AND id IN (
    SELECT id
    FROM (
      SELECT
        id,
        ROW_NUMBER() OVER (
          PARTITION BY sample_id, grain_id, metric_id, logical_date, time_grain
          ORDER BY ingested_at DESC, id
        ) AS row_rank
      FROM {tables.fact_values}
      WHERE logical_date BETWEEN min_candidate_logical_date AND max_candidate_logical_date
    )
    WHERE row_rank > 1
  );

MERGE {tables.fact_values} AS target
USING (
  SELECT
    fact.sample_id,
    fact.grain_id,
    fact.grain_name,
    fact.logical_date,
    fact.time_grain,
    metric.id AS metric_id,
    fact.metric_value,
    fact.updated_at
  FROM fact_candidates AS fact
  -- Both dim joins must carry the grain name: a dataset holds several
  -- grains, so joining on the dataset id alone fans one candidate into
  -- multiple source rows and breaks MERGE.
  JOIN {tables.dim_grains} AS grain
    ON grain.id = fact.grain_id
    AND grain.name = fact.grain_name
  JOIN {tables.dim_metrics} AS metric
    ON metric.grain_id = fact.grain_id
    AND metric.name = fact.metric_name
) AS source
ON target.sample_id = source.sample_id
  AND target.grain_id = source.grain_id
  AND target.metric_id = source.metric_id
  AND target.logical_date = source.logical_date
  AND target.logical_date BETWEEN min_candidate_logical_date AND max_candidate_logical_date
  AND target.time_grain = source.time_grain
WHEN MATCHED THEN
  UPDATE SET
    grain_name = source.grain_name,
    metric_value = source.metric_value,
    updated_at = source.updated_at,
    ingested_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    id,
    sample_id,
    grain_id,
    grain_name,
    logical_date,
    time_grain,
    metric_id,
    metric_value,
    updated_at,
    ingested_at
  )
  VALUES (
    GENERATE_UUID(),
    source.sample_id,
    source.grain_id,
    source.grain_name,
    source.logical_date,
    source.time_grain,
    source.metric_id,
    source.metric_value,
    source.updated_at,
    CURRENT_TIMESTAMP()
  );"""
