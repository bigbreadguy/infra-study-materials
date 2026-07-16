"""Builders for the dl_materials transform-load SQL (one multi-statement job).

Single-job ELT: raw records land in GCS, are read through a job-scoped temporary
external table, and a single multi-statement script (1) ensures the durable
tables exist, then merges (2) dim_datasources, (3) dim_categories, (4) dim_metrics,
and (5) fact_values. Compute stays in BigQuery; the Airflow task only orchestrates
I/O.

Tables are owned by the DAGs, not Terraform: the script opens with idempotent
``CREATE TABLE IF NOT EXISTS`` DDL (see :func:`create_tables_ddl`) so a fresh
dataset bootstraps itself on first run and steady-state runs no-op. Terraform owns
only the ``dl_materials`` dataset. The dims/fact are accumulating MERGE targets, so
the DDL is ``IF NOT EXISTS`` (never ``OR REPLACE``) to preserve data across runs.

The script is generated from one recipe's materials mapping config (see
common/materials_metrics.py): each metric contributes a candidate SELECT that
filters rows by its ``match`` and extracts ``measure_column``; a
``previous_year_column`` adds a second candidate at logical_date - 1 year. Korean
columns (with spaces) are read with JSON_VALUE over the wrapped ``row`` JSON
column. Idempotency is by natural key (GENERATE_UUID only on insert); previous-year
splits insert only into gaps and never overwrite a directly scraped value.

Each metric carries a category (category_0/1/2 hierarchy) and an optional
per-metric ``time_grain`` (defaulting to the recipe ``time_grain``), so one config
can mix daily and monthly series under different categories -- the shape both the
KOSA scrape and the dpanda Bloomberg ingest write through.

Kept free of GCP imports so the pure SQL is unit-testable; the external-table job
config lives in common/materials_bigquery.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


RAW_RECORDS_TABLE = "raw_materials_records"
DIM_DATASOURCES_TABLE = "dim_datasources"
DIM_CATEGORIES_TABLE = "dim_categories"
DIM_METRICS_TABLE = "dim_metrics"
FACT_VALUES_TABLE = "fact_values"


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
    def dim_categories(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, DIM_CATEGORIES_TABLE)

    @property
    def dim_metrics(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, DIM_METRICS_TABLE)

    @property
    def fact_values(self) -> str:
        return qualified_table(self.project_id, self.dataset_id, FACT_VALUES_TABLE)


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


def _optional_literal(value: str | None) -> str:
    # NULLABLE dimension columns: a missing value becomes a typed NULL so the
    # MERGE source column keeps its STRING type.
    return _safe_literal(value) if value else "CAST(NULL AS STRING)"


def _json_value(column: str) -> str:
    # Read a (possibly Korean, space-containing) field from the wrapped row JSON.
    # BigQuery JSONPath escapes special-character keys with a dot + double quotes
    # (e.g. JSON_VALUE(row, '$."국내수입 물량"')); it rejects bracket notation
    # ($["..."]) with "Invalid token in JSONPath".
    if '"' in column or "'" in column or "\\" in column:
        raise ValueError(f"column {column!r} must not contain quotes or backslashes")
    return f"JSON_VALUE(row, '$.\"{column}\"')"


def _numeric_expr(column: str) -> str:
    # Strip thousands separators ("51,946") then cast; SAFE_CAST yields NULL for
    # non-numeric or empty cells, which the candidate filter drops.
    return f"SAFE_CAST(REPLACE({_json_value(column)}, ',', '') AS NUMERIC)"


def _match_predicate(match: Mapping[str, str]) -> str:
    return " AND ".join(
        f"{_json_value(key)} = {_safe_literal(value)}"
        for key, value in match.items()
    )


def create_tables_ddl(*, project_id: str, dataset_id: str) -> str:
    """Idempotent DDL that bootstraps the durable dl_materials tables.

    The DAGs own these tables (Terraform owns only the dataset), so every
    transform run opens with ``CREATE TABLE IF NOT EXISTS``: a fresh dataset
    self-bootstraps on first run and steady-state runs no-op. The dims and fact
    are accumulating MERGE targets, so this is never ``CREATE OR REPLACE`` --
    that would drop the data. ``fact_values`` is yearly-partitioned
    (``DATE_TRUNC(logical_date, YEAR)``), clustered, and requires a partition
    filter, matching the merge SQL's partition-scoped predicates. Yearly (not
    daily) granularity keeps multi-decade history under BigQuery's 10,000
    partitions-per-table cap -- daily partitioning tops out near 27 years, so a
    decades-long backfill would otherwise overflow the table. ``logical_date``
    stays a daily-resolution DATE; only the partition boundary is yearly.

    NOTE: ``CREATE TABLE IF NOT EXISTS`` never repartitions an existing table,
    and partitioning cannot be altered in place. A table created under the old
    daily scheme must be recreated once by hand (CTAS into a yearly-partitioned
    copy, then swap) -- changing this DDL alone does not migrate live data.
    """
    tables = MaterialsTables(project_id=project_id, dataset_id=dataset_id)
    return f"""CREATE TABLE IF NOT EXISTS {tables.dim_datasources} (
  id STRING NOT NULL,
  name STRING NOT NULL,
  description STRING
);

CREATE TABLE IF NOT EXISTS {tables.dim_categories} (
  id STRING NOT NULL,
  name STRING NOT NULL,
  category_0 STRING,
  category_1 STRING,
  category_2 STRING,
  description STRING
);

CREATE TABLE IF NOT EXISTS {tables.dim_metrics} (
  id STRING NOT NULL,
  datasource_id STRING NOT NULL,
  category_id STRING NOT NULL,
  name STRING NOT NULL,
  description STRING,
  unit STRING
);

CREATE TABLE IF NOT EXISTS {tables.fact_values} (
  id STRING NOT NULL,
  metric_id STRING NOT NULL,
  logical_date DATE NOT NULL,
  time_grain STRING NOT NULL,
  metric_value NUMERIC NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE_TRUNC(logical_date, YEAR)
CLUSTER BY metric_id, time_grain
OPTIONS (require_partition_filter = TRUE);"""


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


def _category_struct(category: Mapping[str, Any]) -> str:
    return (
        f"    STRUCT({_safe_literal(category['name'])} AS name, "
        f"{_optional_literal(category.get('category_0'))} AS category_0, "
        f"{_optional_literal(category.get('category_1'))} AS category_1, "
        f"{_optional_literal(category.get('category_2'))} AS category_2, "
        f"{_optional_literal(category.get('description'))} AS description)"
    )


def dim_categories_merge_sql(
    *,
    project_id: str,
    dataset_id: str,
    categories: list[Mapping[str, Any]],
) -> str:
    """Upsert the distinct category hierarchies a recipe's metrics reference.

    Categories are keyed by ``name`` (a stable slug); category_0/1/2 are the
    hierarchy levels and may be NULL. Runs before dim_metrics because the metric
    merge resolves each metric's category_id by joining this table.
    """
    if not categories:
        raise ValueError("categories must be a non-empty list")
    tables = MaterialsTables(project_id=project_id, dataset_id=dataset_id)
    struct_list = ",\n".join(_category_struct(category) for category in categories)
    return f"""MERGE {tables.dim_categories} AS target
USING (
  SELECT category.name, category.category_0, category.category_1,
    category.category_2, category.description
  FROM UNNEST([
{struct_list}
  ]) AS category
) AS source
ON target.name = source.name
WHEN MATCHED THEN
  UPDATE SET
    category_0 = source.category_0,
    category_1 = source.category_1,
    category_2 = source.category_2,
    description = source.description
WHEN NOT MATCHED THEN
  INSERT (id, name, category_0, category_1, category_2, description)
  VALUES (
    GENERATE_UUID(),
    source.name,
    source.category_0,
    source.category_1,
    source.category_2,
    source.description
  );"""


def _metric_struct(metric: Mapping[str, Any]) -> str:
    unit = metric.get("unit")
    unit_expr = _safe_literal(unit) if unit else "CAST(NULL AS STRING)"
    return (
        f"    STRUCT({_safe_literal(metric['name'])} AS name, "
        f"{_safe_literal(metric['description'])} AS description, "
        f"{unit_expr} AS unit, "
        f"{_safe_literal(metric['category'])} AS category_name)"
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
    category.id AS category_id,
    metric.name,
    metric.description,
    metric.unit
  FROM {tables.dim_datasources} AS datasource
  CROSS JOIN UNNEST([
{struct_list}
  ]) AS metric
  JOIN {tables.dim_categories} AS category
    ON category.name = metric.category_name
  WHERE datasource.name = {_safe_literal(datasource_name)}
) AS source
ON target.datasource_id = source.datasource_id
  AND target.name = source.name
WHEN MATCHED THEN
  UPDATE SET
    category_id = source.category_id,
    description = source.description,
    unit = source.unit
WHEN NOT MATCHED THEN
  INSERT (id, datasource_id, category_id, name, description, unit)
  VALUES (
    GENERATE_UUID(),
    source.datasource_id,
    source.category_id,
    source.name,
    source.description,
    source.unit
  );

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
    is_previous_year: bool,
) -> str:
    # period_column/period_format ride on each metric (attached by the transform
    # builder from its source config), so one fact body can UNION metrics drawn
    # from several recipes that differ in period shape -- e.g. EIA's monthly
    # ``%Y-%m`` series alongside weekly/quarterly ``%Y-%m-%d`` series.
    column = (
        metric["previous_year_column"]
        if is_previous_year
        else metric["measure_column"]
    )
    period_expr = (
        f"SAFE.PARSE_DATE({_safe_literal(metric['period_format'])}, "
        f"{_json_value(metric['period_column'])})"
    )
    date_expr = (
        f"DATE_SUB({period_expr}, INTERVAL 1 YEAR)"
        if is_previous_year
        else period_expr
    )
    flag = "TRUE" if is_previous_year else "FALSE"
    return f"""    SELECT
      {_safe_literal(metric['name'])} AS metric_name,
      {_safe_literal(metric['time_grain'])} AS time_grain,
      {date_expr} AS logical_date,
      {_numeric_expr(column)} AS metric_value,
      {flag} AS is_previous_year
    FROM raw
    WHERE {_match_predicate(metric['match'])}
      AND NULLIF({_json_value(column)}, '') IS NOT NULL"""


def _candidate_selects(*, metrics: list[Mapping[str, Any]]) -> list[str]:
    selects = []
    for metric in metrics:
        selects.append(_candidate_select(metric=metric, is_previous_year=False))
        if metric.get("previous_year_column"):
            selects.append(_candidate_select(metric=metric, is_previous_year=True))
    return selects


def fact_values_body_sql(
    *,
    project_id: str,
    dataset_id: str,
    datasource_name: str,
    metrics: list[Mapping[str, Any]],
) -> str:
    """Fact statements WITHOUT the hoisted DECLAREs (combined script adds them).

    Each metric's ``time_grain``, ``period_column``, and ``period_format`` ride in
    on its candidate SELECT, so one job serves metrics that differ in time grain
    AND period shape (the latter is how several recipes of one source combine into
    a single job). The merge is partition-scoped to the candidate date range so it
    never scans the whole table.
    """
    tables = MaterialsTables(project_id=project_id, dataset_id=dataset_id)
    union = "\n    UNION ALL\n".join(_candidate_selects(metrics=metrics))
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
  time_grain,
  logical_date,
  metric_value,
  is_previous_year
FROM candidates
WHERE logical_date IS NOT NULL
  AND metric_value IS NOT NULL
-- Within one run a current-period row wins over a previous-year split for the
-- same metric, date, and time grain.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY metric_name, time_grain, logical_date
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

MERGE {tables.fact_values} AS target
USING (
  SELECT
    metric.id AS metric_id,
    fact.logical_date,
    fact.time_grain,
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
    FROM {tables.fact_values}
    WHERE logical_date BETWEEN min_logical_date AND max_logical_date
    GROUP BY metric_id, logical_date, time_grain
    HAVING COUNT(*) > 1
  )
) = 0 AS 'fact_values must keep one row per metric, date, and time grain in the merged range.';"""


_HOISTED_DECLARES = (
    "DECLARE min_logical_date DATE DEFAULT NULL;\n"
    "DECLARE max_logical_date DATE DEFAULT NULL;"
)


def _metrics_with_period(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Attach a config's period_column/period_format onto each of its metrics.

    The fact candidate SELECT reads period from the metric (not a single shared
    literal), so this is what lets metrics from several recipes -- each with its
    own period shape -- be unioned into one fact body.
    """
    period_column = config["period_column"]
    period_format = config["period_format"]
    return [
        {**metric, "period_column": period_column, "period_format": period_format}
        for metric in config["metrics"]
    ]


def _combined_datasource(configs: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    # A combined job's dim_datasources/dim_metrics merges are datasource-scoped, so
    # every config in one job must name the same datasource. The scrape DAG groups
    # by source (one datasource per source), so this only fails on a misconfigured
    # combine -- and then loudly.
    datasource = configs[0]["datasource"]
    for config in configs[1:]:
        if config["datasource"]["name"] != datasource["name"]:
            raise ValueError(
                "combined transform requires a single datasource; got "
                f"{datasource['name']!r} and {config['datasource']['name']!r}"
            )
    return datasource


def _combined_metrics(configs: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    seen: set[str] = set()
    for config in configs:
        for metric in _metrics_with_period(config):
            if metric["name"] in seen:
                raise ValueError(
                    f"duplicate metric name {metric['name']!r} across combined configs"
                )
            seen.add(metric["name"])
            metrics.append(metric)
    return metrics


def _combined_categories(configs: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    # Dedup by name across all configs (each config's categories are already
    # distinct), preserving first-seen order. A name reused with a conflicting
    # hierarchy/description would make the single dim_categories row ambiguous, so
    # fail loudly -- the same rule _collect_categories enforces within one config.
    by_name: dict[str, Mapping[str, Any]] = {}
    ordered: list[Mapping[str, Any]] = []
    for config in configs:
        for category in config["categories"]:
            existing = by_name.get(category["name"])
            if existing is None:
                by_name[category["name"]] = category
                ordered.append(category)
            elif existing != category:
                raise ValueError(
                    f"category {category['name']!r} defined with conflicting "
                    "hierarchy/description across combined configs"
                )
    return ordered


def combined_materials_transform_sql(
    *,
    project_id: str,
    dataset_id: str,
    configs: list[Mapping[str, Any]],
    expected_row_count: int | None = None,
) -> str:
    """One multi-statement script transforming several recipes' raw in one job.

    All configs must share a datasource (the scrape DAG combines per source). Their
    metrics are unioned (each carrying its own period_column/period_format and
    time_grain) and their categories deduped, so a single job's dim_datasources,
    dim_categories, dim_metrics, and fact_values MERGEs cover every recipe at once
    -- collapsing N per-recipe jobs (and their N x 4 per-table MERGEs) into one, to
    stay under BigQuery's per-table update rate limit. The raw is read through a
    job-scoped external table over the run's per-source wildcard; the natural-key
    ``match`` on each candidate keeps the recipes' rows disjoint.

    Order: hoisted DECLAREs -> CREATE TABLE IF NOT EXISTS (DAG-owned tables) ->
    raw read validate -> dim_datasources MERGE -> dim_categories MERGE ->
    dim_metrics MERGE+ASSERT -> fact body (CREATE TEMP / SET / dims-resolvable
    ASSERT / MERGE / uniqueness ASSERT). Dims precede fact because the fact ASSERT
    and MERGE join the dim tables; dim_categories precedes dim_metrics because the
    metric merge resolves category_id from it.
    """
    if not configs:
        raise ValueError("configs must be a non-empty list")
    datasource = _combined_datasource(configs)
    metrics = _combined_metrics(configs)
    categories = _combined_categories(configs)
    return "\n\n".join(
        [
            _HOISTED_DECLARES,
            create_tables_ddl(project_id=project_id, dataset_id=dataset_id),
            raw_records_check_sql(expected_row_count=expected_row_count),
            dim_datasources_merge_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                name=datasource["name"],
                description=datasource["description"],
            ),
            dim_categories_merge_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                categories=categories,
            ),
            dim_metrics_merge_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                datasource_name=datasource["name"],
                metrics=metrics,
            ),
            fact_values_body_sql(
                project_id=project_id,
                dataset_id=dataset_id,
                datasource_name=datasource["name"],
                metrics=metrics,
            ),
        ]
    )


def materials_transform_sql(
    *,
    project_id: str,
    dataset_id: str,
    config: Mapping[str, Any],
    expected_row_count: int | None = None,
) -> str:
    """One multi-statement script for a single recipe's whole transform-load.

    Thin wrapper over :func:`combined_materials_transform_sql` with one config; the
    dpanda Bloomberg ingest (one config per category) uses this path.
    """
    return combined_materials_transform_sql(
        project_id=project_id,
        dataset_id=dataset_id,
        configs=[config],
        expected_row_count=expected_row_count,
    )
