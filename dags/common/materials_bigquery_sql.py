"""Builders for the dl_materials transform-load SQL (one multi-statement job).

Mirrors the single-job ELT design of the dfml-airflow market-index pipeline: raw
records land in GCS, are read through a job-scoped temporary external table, and a
single multi-statement script merges dim_datasources, then dim_metrics, then
fact_metals. Compute stays in BigQuery; the Airflow task only orchestrates I/O.

The script is generated from one recipe's gitignored mapping config (see
common/materials_metrics.py): each metric contributes a candidate SELECT that
filters rows by its ``match`` and extracts ``measure_column``; a
``previous_year_column`` adds a second candidate at logical_date - 1 year. Korean
columns (with spaces) are read with JSON_VALUE over the wrapped ``row`` JSON
column. Idempotency is by natural key (GENERATE_UUID only on insert); previous-year
splits insert only into gaps and never overwrite a directly scraped value.

Kept free of GCP imports so the pure SQL is unit-testable; the external-table job
config lives in common/materials_bigquery.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


RAW_RECORDS_TABLE = "raw_materials_records"
DIM_DATASOURCES_TABLE = "dim_datasources"
DIM_METRICS_TABLE = "dim_metrics"
FACT_METALS_TABLE = "fact_metals"


@dataclass(frozen=True)
class MaterialsTables:
    project_id: str
    dataset_id: str

    @property
    def raw_records(self) -> str:
        # Bare name: resolved per query job through a temporary external table
        # definition keyed by RAW_RECORDS_TABLE, so no persistent raw table.
        return RAW_RECORDS_TABLE

    @property
    def dim_datasources(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, DIM_DATASOURCES_TABLE)

    @property
    def dim_metrics(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, DIM_METRICS_TABLE)

    @property
    def fact_metals(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, FACT_METALS_TABLE)


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


def _safe_literal(value: str) -> str:
    if "'" in value or '"' in value or "\\" in value:
        raise ValueError(
            f"value {value!r} must not contain quotes or backslashes"
        )
    return f"'{value}'"


def _json_value(column: str) -> str:
    # Read a (possibly Korean, space-containing) field from the wrapped row JSON
    # via bracket-quoted JSON path, e.g. JSON_VALUE(row, '$["국내수입 물량"]').
    if '"' in column or "'" in column or "\\" in column:
        raise ValueError(f"column {column!r} must not contain quotes or backslashes")
    return f"JSON_VALUE(row, '$[\"{column}\"]')"


def _numeric_expr(column: str) -> str:
    # Strip thousands separators ("51,946") then cast; SAFE_CAST yields NULL for
    # non-numeric or empty cells, which the candidate filter drops.
    return f"SAFE_CAST(REPLACE({_json_value(column)}, ',', '') AS NUMERIC)"


def _match_predicate(match: Mapping[str, str]) -> str:
    return " AND ".join(
        f"{_json_value(key)} = {_safe_literal(value)}"
        for key, value in match.items()
    )


def raw_records_check_sql(*, expected_row_count: int | None = None) -> str:
    if expected_row_count is None:
        return f"SELECT COUNT(*) AS row_count FROM {RAW_RECORDS_TABLE};"
    if not isinstance(expected_row_count, int) or expected_row_count < 0:
        raise ValueError("expected_row_count must be a non-negative integer")
    return (
        "ASSERT (\n"
        f"  SELECT COUNT(*) FROM {RAW_RECORDS_TABLE}\n"
        f") = {expected_row_count} AS "
        "'Raw external table row count must match the scraped record count.';"
    )


def dim_datasources_merge_sql(
    *, project_id: str, dataset_id: str, name: str, description: str
) -> str:
    tables = MaterialsTables(project_id=project_id, dataset_id=dataset_id)
    return f"""MERGE {tables.dim_datasources} AS target
USING (
  SELECT {_safe_literal(name)} AS name, {_safe_literal(description)} AS description
) AS source
ON target.name = source.name
WHEN MATCHED THEN
  UPDATE SET description = source.description
WHEN NOT MATCHED THEN
  INSERT (id, name, description)
  VALUES (GENERATE_UUID(), source.name, source.description);"""


def _metric_struct(metric: Mapping[str, Any]) -> str:
    unit = metric.get("unit")
    unit_expr = _safe_literal(unit) if unit else "CAST(NULL AS STRING)"
    return (
        f"    STRUCT({_safe_literal(metric['name'])} AS name, "
        f"{_safe_literal(metric['description'])} AS description, "
        f"{unit_expr} AS unit)"
    )


def dim_metrics_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
    datasource_name: str,
    metrics: list[Mapping[str, Any]],
) -> str:
    tables = MaterialsTables(project_id=project_id, dataset_id=dataset_id)
    struct_list = ",\n".join(_metric_struct(metric) for metric in metrics)
    return f"""MERGE {tables.dim_metrics} AS target
USING (
  SELECT
    datasource.id AS datasource_id,
    metric.name,
    metric.description,
    metric.unit
  FROM {tables.dim_datasources} AS datasource
  CROSS JOIN UNNEST([
{struct_list}
  ]) AS metric
  WHERE datasource.name = {_safe_literal(datasource_name)}
) AS source
ON target.datasource_id = source.datasource_id
  AND target.name = source.name
WHEN MATCHED THEN
  UPDATE SET description = source.description, unit = source.unit
WHEN NOT MATCHED THEN
  INSERT (id, datasource_id, name, description, unit)
  VALUES (GENERATE_UUID(), source.datasource_id, source.name, source.description, source.unit);

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT datasource_id, name
    FROM {tables.dim_metrics}
    GROUP BY datasource_id, name
    HAVING COUNT(*) > 1
  )
) = 0 AS 'dim_metrics must keep one row per datasource id and metric name pair.';"""


def _candidate_select(
    *,
    metric: Mapping[str, Any],
    period_column: str,
    period_format: str,
    is_previous_year: bool,
) -> str:
    column = (
        metric["previous_year_column"]
        if is_previous_year
        else metric["measure_column"]
    )
    period_expr = (
        f"SAFE.PARSE_DATE({_safe_literal(period_format)}, {_json_value(period_column)})"
    )
    date_expr = (
        f"DATE_SUB({period_expr}, INTERVAL 1 YEAR)"
        if is_previous_year
        else period_expr
    )
    flag = "TRUE" if is_previous_year else "FALSE"
    return f"""    SELECT
      {_safe_literal(metric['name'])} AS metric_name,
      {date_expr} AS logical_date,
      {_numeric_expr(column)} AS metric_value,
      {flag} AS is_previous_year
    FROM raw
    WHERE {_match_predicate(metric['match'])}
      AND NULLIF({_json_value(column)}, '') IS NOT NULL"""


def _candidate_selects(
    *, metrics: list[Mapping[str, Any]], period_column: str, period_format: str
) -> list[str]:
    selects = []
    for metric in metrics:
        selects.append(
            _candidate_select(
                metric=metric,
                period_column=period_column,
                period_format=period_format,
                is_previous_year=False,
            )
        )
        if metric.get("previous_year_column"):
            selects.append(
                _candidate_select(
                    metric=metric,
                    period_column=period_column,
                    period_format=period_format,
                    is_previous_year=True,
                )
            )
    return selects


def fact_metals_body_sql(
    *,
    project_id: str,
    dataset_id: str,
    datasource_name: str,
    period_column: str,
    period_format: str,
    time_grain: str,
    metrics: list[Mapping[str, Any]],
) -> str:
    """Fact statements WITHOUT the hoisted DECLAREs (combined script adds them)."""
    tables = MaterialsTables(project_id=project_id, dataset_id=dataset_id)
    union = "\n    UNION ALL\n".join(
        _candidate_selects(
            metrics=metrics,
            period_column=period_column,
            period_format=period_format,
        )
    )
    datasource_literal = _safe_literal(datasource_name)
    return f"""CREATE TEMP TABLE fact_candidates AS
WITH raw AS (
  SELECT row FROM {tables.raw_records}
),
candidates AS (
{union}
)
SELECT
  metric_name,
  logical_date,
  metric_value,
  is_previous_year
FROM candidates
WHERE logical_date IS NOT NULL
  AND metric_value IS NOT NULL
-- Within one run a current-period row wins over a previous-year split for the
-- same metric and date.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY metric_name, logical_date
  ORDER BY is_previous_year ASC
) = 1;

SET min_logical_date = COALESCE(
  (SELECT MIN(logical_date) FROM fact_candidates), DATE '1900-01-01'
);
SET max_logical_date = COALESCE(
  (SELECT MAX(logical_date) FROM fact_candidates), DATE '1900-01-01'
);

ASSERT (
  SELECT COUNT(*)
  FROM fact_candidates AS fact
  LEFT JOIN {tables.dim_datasources} AS datasource
    ON datasource.name = {datasource_literal}
  LEFT JOIN {tables.dim_metrics} AS metric
    ON metric.datasource_id = datasource.id
    AND metric.name = fact.metric_name
  WHERE metric.id IS NULL
) = 0 AS 'Every materials fact candidate must resolve a dim_metrics row before merging.';

MERGE {tables.fact_metals} AS target
USING (
  SELECT
    metric.id AS metric_id,
    fact.logical_date,
    {_safe_literal(time_grain)} AS time_grain,
    fact.metric_value,
    fact.is_previous_year
  FROM fact_candidates AS fact
  JOIN {tables.dim_datasources} AS datasource
    ON datasource.name = {datasource_literal}
  JOIN {tables.dim_metrics} AS metric
    ON metric.datasource_id = datasource.id
    AND metric.name = fact.metric_name
) AS source
ON target.metric_id = source.metric_id
  AND target.logical_date = source.logical_date
  AND target.time_grain = source.time_grain
  AND target.logical_date BETWEEN min_logical_date AND max_logical_date
-- Current-period rows upsert; previous-year splits only fill gaps and never
-- overwrite a directly scraped value (the user requirement).
WHEN MATCHED AND source.is_previous_year = FALSE THEN
  UPDATE SET
    metric_value = source.metric_value,
    ingested_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (id, metric_id, logical_date, time_grain, metric_value, ingested_at)
  VALUES (
    GENERATE_UUID(),
    source.metric_id,
    source.logical_date,
    source.time_grain,
    source.metric_value,
    CURRENT_TIMESTAMP()
  );

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT metric_id, logical_date, time_grain
    FROM {tables.fact_metals}
    WHERE logical_date BETWEEN min_logical_date AND max_logical_date
    GROUP BY metric_id, logical_date, time_grain
    HAVING COUNT(*) > 1
  )
) = 0 AS 'fact_metals must keep one row per metric, date, and time grain in the merged range.';"""


_HOISTED_DECLARES = (
    "DECLARE min_logical_date DATE DEFAULT NULL;\n"
    "DECLARE max_logical_date DATE DEFAULT NULL;"
)


def materials_transform_sql(
    *,
    project_id: str,
    dataset_id: str,
    config: Mapping[str, Any],
    expected_row_count: int | None = None,
) -> str:
    """One multi-statement script for a recipe's whole transform-load.

    Order: hoisted DECLAREs -> raw read validate -> dim_datasources MERGE ->
    dim_metrics MERGE+ASSERT -> fact body (CREATE TEMP / SET / dims-resolvable
    ASSERT / MERGE / uniqueness ASSERT). Dims precede fact because the fact ASSERT
    and MERGE join both dim tables.
    """
    datasource = config["datasource"]
    metrics = list(config["metrics"])
    return "\n\n".join(
        [
            _HOISTED_DECLARES,
            raw_records_check_sql(expected_row_count=expected_row_count),
            dim_datasources_merge_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                name=datasource["name"],
                description=datasource["description"],
            ),
            dim_metrics_merge_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                datasource_name=datasource["name"],
                metrics=metrics,
            ),
            fact_metals_body_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                datasource_name=datasource["name"],
                period_column=config["period_column"],
                period_format=config["period_format"],
                time_grain=config["time_grain"],
                metrics=metrics,
            ),
        ]
    )
