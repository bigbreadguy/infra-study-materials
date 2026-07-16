from __future__ import annotations

from unittest import TestCase

from external_data.common.materials_bigquery_sql import (
    combined_materials_transform_sql,
    create_tables_ddl,
    materials_transform_sql,
    qualified_table,
    raw_records_check_sql,
)
from external_data.common.materials_bigquery import raw_external_table_definition


_CATEGORY = {
    "name": "metal_heavy_melting_steel_scrap",
    "category_0": "metal",
    "category_1": "steel",
    "category_2": "heavy_melting_scrap",
    "description": "금속 - 용해용철스크랩",
}


def _config(previous_year: bool = True) -> dict:
    metric = {
        "match": {"국가": "일본", "품목명": "용해용철스크랩"},
        "measure_column": "국내수입 물량",
        "name": "steel_scrap_import_from_japan_volume",
        "description": "용해용철스크랩 일본 국내수입 물량",
        "unit": "천톤",
        "time_grain": "M",
        "category": _CATEGORY["name"],
    }
    if previous_year:
        metric["previous_year_column"] = "전년 물량"
    return {
        "datasource": {"name": "kosa_steeldata", "description": "한국철강협회 STEEL DATA"},
        "period_column": "시점",
        "period_format": "%Y.%m",
        "time_grain": "M",
        "categories": [_CATEGORY],
        "metrics": [metric],
    }


def _sql(**kwargs) -> str:
    return materials_transform_sql(
        project_id="proj", dataset_id="dl_materials", config=_config(**kwargs)
    )


class QualifiedTableTests(TestCase):
    def test_backticked(self):
        self.assertEqual(
            qualified_table("proj", "dl_materials", "fact_values"),
            "`proj.dl_materials.fact_values`",
        )

    def test_rejects_dotted_part(self):
        with self.assertRaises(ValueError):
            qualified_table("proj.evil", "dl_materials", "fact_values")


class CreateTablesDdlTests(TestCase):
    def test_bootstraps_all_four_tables_idempotently(self):
        ddl = create_tables_ddl(project_id="proj", dataset_id="dl_materials")
        for table in ("dim_datasources", "dim_categories", "dim_metrics", "fact_values"):
            self.assertIn(
                f"CREATE TABLE IF NOT EXISTS `proj.dl_materials.{table}`", ddl
            )
        # Accumulating MERGE targets are never replaced.
        self.assertNotIn("CREATE OR REPLACE", ddl)

    def test_fact_values_is_partitioned_clustered_and_filtered(self):
        ddl = create_tables_ddl(project_id="proj", dataset_id="dl_materials")
        # Yearly (not daily) partitioning keeps multi-decade history under
        # BigQuery's 10,000-partitions-per-table cap; logical_date stays a
        # daily-resolution DATE, only the partition boundary is yearly.
        self.assertIn("PARTITION BY DATE_TRUNC(logical_date, YEAR)", ddl)
        self.assertIn("CLUSTER BY metric_id, time_grain", ddl)
        self.assertIn("require_partition_filter = TRUE", ddl)


class TransformSqlTests(TestCase):
    def test_merges_all_four_tables(self):
        sql = _sql()
        self.assertIn("MERGE `proj.dl_materials.dim_datasources`", sql)
        self.assertIn("MERGE `proj.dl_materials.dim_categories`", sql)
        self.assertIn("MERGE `proj.dl_materials.dim_metrics`", sql)
        self.assertIn("MERGE `proj.dl_materials.fact_values`", sql)

    def test_bootstraps_tables_before_merging(self):
        sql = _sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS `proj.dl_materials.fact_values`", sql)
        self.assertLess(
            sql.index("CREATE TABLE IF NOT EXISTS"),
            sql.index("MERGE `proj.dl_materials.dim_datasources`"),
        )

    def test_dim_metrics_resolves_category_id(self):
        sql = _sql()
        self.assertIn("category_id", sql)
        self.assertIn("ON category.name = metric.category_name", sql)

    def test_generates_uuid_on_insert(self):
        # One INSERT ... GENERATE_UUID() per dim/fact table (4 total).
        self.assertEqual(_sql().count("GENERATE_UUID()"), 4)

    def test_reads_korean_columns_via_json_path(self):
        # BigQuery escapes special-char keys with dot + double quotes, not brackets.
        self.assertIn('JSON_VALUE(row, \'$."국내수입 물량"\')', _sql())
        self.assertNotIn('$["', _sql())

    def test_strips_thousands_separator(self):
        self.assertIn("REPLACE(JSON_VALUE(row, '$.\"국내수입 물량\"'), ',', '')", _sql())

    def test_previous_year_splits_one_year_back(self):
        sql = _sql(previous_year=True)
        self.assertIn("DATE_SUB(SAFE.PARSE_DATE('%Y.%m'", sql)
        self.assertIn("INTERVAL 1 YEAR", sql)
        self.assertIn('JSON_VALUE(row, \'$."전년 물량"\')', sql)

    def test_no_previous_year_when_unset(self):
        sql = _sql(previous_year=False)
        self.assertNotIn("INTERVAL 1 YEAR", sql)

    def test_previous_year_is_insert_only(self):
        # Current rows upsert; previous-year rows must not overwrite existing rows.
        self.assertIn("WHEN MATCHED AND source.is_previous_year = FALSE THEN", _sql())

    def test_fact_merge_is_partition_scoped(self):
        self.assertIn(
            "target.logical_date BETWEEN min_logical_date AND max_logical_date",
            _sql(),
        )

    def test_per_metric_time_grain_rides_into_fact(self):
        # time_grain is a per-candidate column, not a single shared literal.
        self.assertIn("AS time_grain", _sql())
        self.assertIn("target.time_grain = source.time_grain", _sql())

    def test_declares_hoisted_to_top(self):
        self.assertTrue(_sql().startswith("DECLARE min_logical_date DATE"))


_CATEGORY_B = {
    "name": "energy_petroleum_us_inventory",
    "category_0": "energy",
    "category_1": "petroleum",
    "category_2": "us_inventory",
    "description": "에너지 - 석유 - 미국 재고",
}


def _eia_config(*, name, series_id, period_format, time_grain, category):
    # Two EIA recipes share the datasource but differ in period_format (monthly vs
    # weekly) -- the heterogeneity the combined job must serve in one fact body.
    return {
        "datasource": {"name": "eia", "description": "U.S. EIA"},
        "period_column": "period",
        "period_format": period_format,
        "time_grain": time_grain,
        "categories": [category],
        "metrics": [{
            "match": {"seriesId": series_id},
            "measure_column": "value",
            "name": name,
            "description": f"{name} desc",
            "unit": "u",
            "time_grain": time_grain,
            "category": category["name"],
        }],
    }


_EIA_MONTHLY = _eia_config(
    name="prod_world", series_id="COPR_WORLD", period_format="%Y-%m",
    time_grain="M", category=_CATEGORY,
)
_EIA_WEEKLY = _eia_config(
    name="us_crude_stocks", series_id="WCESTUS1", period_format="%Y-%m-%d",
    time_grain="W", category=_CATEGORY_B,
)


class CombinedTransformSqlTests(TestCase):
    def _combined(self, configs, **kwargs):
        return combined_materials_transform_sql(
            project_id="proj", dataset_id="dl_materials", configs=configs, **kwargs
        )

    def test_unions_per_metric_period_formats_in_one_job(self):
        sql = self._combined([_EIA_MONTHLY, _EIA_WEEKLY])
        # Both recipes' period shapes ride in on their own candidate SELECTs.
        self.assertIn("PARSE_DATE('%Y-%m'", sql)
        self.assertIn("PARSE_DATE('%Y-%m-%d'", sql)
        # Disjoint natural keys keep the recipes' rows from cross-contaminating.
        self.assertIn("COPR_WORLD", sql)
        self.assertIn("WCESTUS1", sql)

    def test_shared_tables_merged_once_for_the_whole_source(self):
        sql = self._combined([_EIA_MONTHLY, _EIA_WEEKLY])
        for table in ("dim_datasources", "dim_categories", "dim_metrics", "fact_values"):
            self.assertEqual(
                sql.count(f"MERGE `proj.dl_materials.{table}`"), 1, table
            )
        # One INSERT ... GENERATE_UUID() per dim/fact table, regardless of recipe count.
        self.assertEqual(sql.count("GENERATE_UUID()"), 4)

    def test_expected_row_count_is_the_combined_total(self):
        self.assertIn("= 99 AS", self._combined([_EIA_MONTHLY, _EIA_WEEKLY],
                                                expected_row_count=99))

    def test_rejects_mixed_datasources(self):
        other = {**_EIA_WEEKLY, "datasource": {"name": "kosa", "description": "x"}}
        with self.assertRaises(ValueError):
            self._combined([_EIA_MONTHLY, other])

    def test_rejects_duplicate_metric_name_across_configs(self):
        with self.assertRaises(ValueError):
            self._combined([_EIA_MONTHLY, _EIA_MONTHLY])

    def test_rejects_empty_configs(self):
        with self.assertRaises(ValueError):
            self._combined([])

    def test_single_config_matches_materials_transform_sql(self):
        # The single-recipe wrapper (dpanda path) is just the one-config combine.
        config = _config()
        self.assertEqual(
            materials_transform_sql(
                project_id="proj", dataset_id="dl_materials", config=config
            ),
            combined_materials_transform_sql(
                project_id="proj", dataset_id="dl_materials", configs=[config]
            ),
        )


class RawCheckSqlTests(TestCase):
    def test_plain_count_without_expected(self):
        self.assertIn("SELECT COUNT(*)", raw_records_check_sql())

    def test_assert_with_expected(self):
        self.assertIn("= 5 AS", raw_records_check_sql(expected_row_count=5))

    def test_rejects_negative_expected(self):
        with self.assertRaises(ValueError):
            raw_records_check_sql(expected_row_count=-1)


class RawExternalTableTests(TestCase):
    def test_single_json_row_column(self):
        definition = raw_external_table_definition("gs://bucket/x.ndjson")
        self.assertEqual(definition["schema"]["fields"], [{"name": "row", "type": "JSON"}])
        self.assertEqual(definition["sourceFormat"], "NEWLINE_DELIMITED_JSON")

    def test_parquet_row_is_string_column(self):
        definition = raw_external_table_definition(
            "gs://bucket/run/*", source_format="PARQUET"
        )
        self.assertEqual(
            definition["schema"]["fields"], [{"name": "row", "type": "STRING"}]
        )
        self.assertEqual(definition["sourceFormat"], "PARQUET")

    def test_rejects_non_gcs_uri(self):
        with self.assertRaises(ValueError):
            raw_external_table_definition("/local/path.ndjson")
