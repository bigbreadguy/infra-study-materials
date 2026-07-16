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

    @property
    def raw_data_samples(self) -> str:
        # Bare name: each query job resolves it through a temporary external
        # table definition keyed by RAW_DATA_SAMPLES_TABLE, so no persistent
        # raw table exists and concurrent grain chains cannot clobber each
        # other's URI pointer.
        return RAW_DATA_SAMPLES_TABLE

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


# Order of the raw external-table columns. The Mongo-exported Extended-JSON
# fields land as JSON for NDJSON and STRING for Parquet (see below); the
# curated per-grain fields the extractor injects (_target_freq,
# _target_description) carry each grain's freq and description into the merge
# so one combined job can serve grains with different time grains/descriptions.
_RAW_FIELD_ORDER = (
    "_id",
    "datasetId",
    "ts",
    "grainId",
    "_schema",
    "description",
    "createdAt",
    "data",
    "updatedAt",
    "_target_freq",
    "_target_description",
)

# Public alias so the extractor writes Parquet columns in exactly this order.
RAW_FIELD_ORDER = _RAW_FIELD_ORDER

# Extended-JSON fields kept as JSON at the NDJSON external-table boundary.
_RAW_JSON_FIELDS = frozenset(
    {"_id", "datasetId", "ts", "createdAt", "data", "updatedAt"}
)

SUPPORTED_RAW_SOURCE_FORMATS = ("NEWLINE_DELIMITED_JSON", "PARQUET")


def raw_external_table_definition(
    raw_gcs_uri: str,
    *,
    source_format: str = "NEWLINE_DELIMITED_JSON",
) -> dict:
    """ExternalConfig API representation for one run's raw objects.

    Attached to each query job as a temporary table definition keyed by
    RAW_DATA_SAMPLES_TABLE; the job-scoped definition replaces the persistent
    per-grain external tables that used to clutter the dataset. ``raw_gcs_uri``
    is a wildcard that captures every object the run uploaded, so one job reads
    all grains at once.

    Two source formats, one merge SQL: scheduled runs write NDJSON (the
    Extended-JSON fields stay JSON), manual backfill writes Parquet (the same
    fields are STRING columns holding the Extended-JSON text). JSON_VALUE and
    JSON_QUERY accept a STRING argument, so the downstream merge SQL is
    identical across both definitions.
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
        # Parquet has no JSON type; the extractor writes every field as the
        # Extended-JSON STRING the NDJSON path emits, so JSON_VALUE reads them
        # the same way.
        fields = [{"name": name, "type": "STRING"} for name in _RAW_FIELD_ORDER]
        return {
            "sourceFormat": "PARQUET",
            "sourceUris": [raw_gcs_uri],
            "schema": {"fields": fields},
        }

    fields = [
        {"name": name, "type": "JSON" if name in _RAW_JSON_FIELDS else "STRING"}
        for name in _RAW_FIELD_ORDER
    ]
    return {
        "sourceFormat": "NEWLINE_DELIMITED_JSON",
        "ignoreUnknownValues": True,
        "sourceUris": [raw_gcs_uri],
        "schema": {"fields": fields},
    }


def raw_data_samples_check_sql(*, expected_row_count: int | None = None) -> str:
    if expected_row_count is None:
        # Without an expected count the read still proves the run's object is
        # present and parseable through the temporary table definition.
        return f"""SELECT COUNT(*) AS row_count
FROM {RAW_DATA_SAMPLES_TABLE};"""

    if not isinstance(expected_row_count, int) or expected_row_count < 0:
        raise ValueError("expected_row_count must be a non-negative integer")

    return f"""ASSERT (
  SELECT COUNT(*)
  FROM {RAW_DATA_SAMPLES_TABLE}
) = {expected_row_count} AS 'Raw external table row count must match the extracted document count.';"""


def dim_grains_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
) -> str:
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
    )

    # The curated per-grain description the extractor injected (_target_description)
    # wins over whatever the raw Mongo documents carry, which is usually nothing.
    # It is constant within a grain, so any aggregate over the group recovers it;
    # one combined job serves many grains, each with its own curated description.
    raw_description_expr = (
        "NULLIF(ARRAY_AGG(COALESCE(description, '') "
        "ORDER BY source_file_name DESC LIMIT 1)[OFFSET(0)], '')"
    )
    description_expr = (
        f"COALESCE(NULLIF(MAX(curated_description), ''), {raw_description_expr})"
    )

    return f"""MERGE {tables.dim_grains} AS target
USING (
  WITH normalized AS (
    SELECT
      COALESCE(JSON_VALUE(datasetId, '$."$oid"'), JSON_VALUE(datasetId, '$')) AS id,
      COALESCE(grainId, JSON_VALUE(data, '$.grainId')) AS name,
      COALESCE(description, JSON_VALUE(data, '$.description')) AS description,
      _target_description AS curated_description,
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
) -> str:
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
    )

    # The curated per-grain description the extractor injected wins over the
    # raw Mongo description, mirroring the dim grains merge. Same-named grains
    # across datasets emit identical metric names, so the curated description
    # is what tells their dim_metrics rows apart.
    base_description_expr = (
        "COALESCE(NULLIF(curated_description, ''), grain_description, grain_name)"
    )

    return f"""MERGE {tables.dim_metrics} AS target
USING (
  WITH raw_samples AS (
    SELECT
      COALESCE(JSON_VALUE(raw.datasetId, '$."$oid"'), JSON_VALUE(raw.datasetId, '$')) AS grain_id,
      COALESCE(raw.grainId, JSON_VALUE(raw.data, '$.grainId')) AS grain_name,
      COALESCE(raw.description, JSON_VALUE(raw.data, '$.description')) AS grain_description,
      raw._target_description AS curated_description,
      raw.data
    FROM {tables.raw_data_samples} AS raw
  ),
  metric_rows AS (
    SELECT
      raw.grain_id,
      raw.grain_name,
      raw.grain_description,
      raw.curated_description,
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
      CONCAT({base_description_expr}, ' ', description_suffix)
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
    dedup_cleanup: bool = False,
) -> str:
    """Standalone fact merge: the two DECLAREs followed by the merge body."""
    return (
        "DECLARE min_candidate_logical_date DATE DEFAULT NULL;\n"
        "DECLARE max_candidate_logical_date DATE DEFAULT NULL;\n\n"
        + fact_values_body_sql(
            project_id=project_id,
            dataset_id=dataset_id,
            dedup_cleanup=dedup_cleanup,
        )
    )


def fact_values_body_sql(
    *,
    project_id: str,
    dataset_id: str,
    dedup_cleanup: bool = False,
) -> str:
    """Fact merge statements WITHOUT the two DECLAREs.

    Hoisting the DECLAREs out lets the combined single-job script declare the
    min/max candidate-date variables once at the top (BigQuery requires DECLARE
    at the start of a block) while reusing this body verbatim. The per-grain
    time grain rides in on the ``_target_freq`` column the extractor injected,
    so one combined job serves grains with different time grains.

    ``dedup_cleanup`` gates a one-time legacy-duplicate sweep (opt-in via the
    backfill DAG). Steady state never needs it: source is deduped here and
    writes are serialized by the per-category pool, so MERGE alone keeps one
    row per key. The post-merge uniqueness ASSERT guards that invariant cheaply
    over the touched partition range.
    """
    tables = MarketIndexTables(
        project_id=project_id,
        dataset_id=dataset_id,
    )

    cleanup_statement = ""
    if dedup_cleanup:
        cleanup_statement = f"""
-- One-time remediation (opt-in): earlier runs inserted duplicate fact rows
-- before source dedup existed; MERGE can update but never delete, so drop
-- every duplicate but the latest ingested row. Steady-state runs skip this.
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
"""

    return f"""CREATE TEMP TABLE fact_candidates AS
WITH raw_samples AS (
  SELECT
    COALESCE(JSON_VALUE(raw._id, '$."$oid"'), JSON_VALUE(raw._id, '$')) AS sample_id,
    COALESCE(JSON_VALUE(raw.datasetId, '$."$oid"'), JSON_VALUE(raw.datasetId, '$')) AS grain_id,
    COALESCE(raw.grainId, JSON_VALUE(raw.data, '$.grainId')) AS grain_name,
    COALESCE(JSON_VALUE(raw.data, '$.dt'), JSON_VALUE(raw.data, '$.date')) AS source_logical_date,
    COALESCE(JSON_VALUE(raw.ts, '$."$date"'), JSON_VALUE(raw.ts, '$')) AS source_ts,
    raw._FILE_NAME AS source_file_name,
    COALESCE(JSON_VALUE(raw.updatedAt, '$."$date"'), JSON_VALUE(raw.updatedAt, '$')) AS source_updated_at,
    COALESCE(NULLIF(raw._target_freq, ''), 'D') AS time_grain,
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
    raw.time_grain,
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
    time_grain,
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
{cleanup_statement}
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
  );

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT sample_id, grain_id, metric_id, logical_date, time_grain
    FROM {tables.fact_values}
    WHERE logical_date BETWEEN min_candidate_logical_date AND max_candidate_logical_date
    GROUP BY sample_id, grain_id, metric_id, logical_date, time_grain
    HAVING COUNT(*) > 1
  )
) = 0 AS 'fact_values must keep one row per sample, grain, metric, date, and time grain in the merged range.';"""


# The two fact DECLAREs, hoisted to the very top of the combined script because
# BigQuery only allows variable declarations at the start of a block.
_HOISTED_DECLARES = (
    "DECLARE min_candidate_logical_date DATE DEFAULT NULL;\n"
    "DECLARE max_candidate_logical_date DATE DEFAULT NULL;"
)


def combined_transform_sql(
    *,
    project_id: str,
    dataset_id: str,
    expected_row_count: int | None = None,
    dedup_cleanup: bool = False,
) -> str:
    """One multi-statement script that transforms every grain in a run.

    Order: hoisted DECLAREs -> raw row-count validate -> dim_grains MERGE+ASSERT
    -> dim_metrics MERGE+ASSERT -> fact body (CREATE TEMP / SET / dims-resolvable
    ASSERT / optional cleanup DELETE / MERGE / uniqueness ASSERT). Dims precede
    fact because the fact ASSERT and MERGE join both dim tables. Per-grain time
    grain and description ride in on the raw ``_target_*`` columns, so this one
    job serves every grain at once instead of one job per grain.

    Submitted as a single BigQuery job; its statements surface as child jobs
    whose per-statement DML counts feed observability.
    """
    return "\n\n".join(
        [
            _HOISTED_DECLARES,
            raw_data_samples_check_sql(expected_row_count=expected_row_count),
            dim_grains_merge_sql(project_id=project_id, dataset_id=dataset_id),
            dim_metrics_merge_sql(project_id=project_id, dataset_id=dataset_id),
            fact_values_body_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                dedup_cleanup=dedup_cleanup,
            ),
        ]
    )
