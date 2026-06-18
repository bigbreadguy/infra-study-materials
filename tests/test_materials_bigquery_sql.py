from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.materials_bigquery_sql import (
    materials_transform_sql,
    qualified_table,
    raw_records_check_sql,
)
from common.materials_bigquery import raw_external_table_definition


def _config(previous_year: bool = True) -> dict:
    metric = {
        "match": {"국가": "일본", "품목명": "용해용철스크랩"},
        "measure_column": "국내수입 물량",
        "name": "steel_scrap_import_from_japan_volume",
        "description": "용해용철스크랩 일본 국내수입 물량",
        "unit": "천톤",
    }
    if previous_year:
        metric["previous_year_column"] = "전년 물량"
    return {
        "datasource": {"name": "kosa_steeldata", "description": "한국철강협회 STEEL DATA"},
        "period_column": "시점",
        "period_format": "%Y.%m",
        "time_grain": "M",
        "metrics": [metric],
    }


def _sql(**kwargs) -> str:
    return materials_transform_sql(
        project_id="proj", dataset_id="dl_materials", config=_config(**kwargs)
    )


class QualifiedTableTests(TestCase):
    def test_backticked(self):
        self.assertEqual(
            qualified_table("proj", "dl_materials", "fact_metals"),
            "`proj.dl_materials.fact_metals`",
        )

    def test_rejects_dotted_part(self):
        with self.assertRaises(ValueError):
            qualified_table("proj.evil", "dl_materials", "fact_metals")


class TransformSqlTests(TestCase):
    def test_merges_all_three_tables(self):
        sql = _sql()
        self.assertIn("MERGE `proj.dl_materials.dim_datasources`", sql)
        self.assertIn("MERGE `proj.dl_materials.dim_metrics`", sql)
        self.assertIn("MERGE `proj.dl_materials.fact_metals`", sql)

    def test_generates_uuid_on_insert(self):
        self.assertEqual(_sql().count("GENERATE_UUID()"), 3)

    def test_reads_korean_columns_via_json_path(self):
        self.assertIn('JSON_VALUE(row, \'$["국내수입 물량"]\')', _sql())

    def test_strips_thousands_separator(self):
        self.assertIn("REPLACE(JSON_VALUE(row, '$[\"국내수입 물량\"]'), ',', '')", _sql())

    def test_previous_year_splits_one_year_back(self):
        sql = _sql(previous_year=True)
        self.assertIn("DATE_SUB(SAFE.PARSE_DATE('%Y.%m'", sql)
        self.assertIn("INTERVAL 1 YEAR", sql)
        self.assertIn('JSON_VALUE(row, \'$["전년 물량"]\')', sql)

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

    def test_declares_hoisted_to_top(self):
        self.assertTrue(_sql().startswith("DECLARE min_logical_date DATE"))


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

    def test_rejects_non_gcs_uri(self):
        with self.assertRaises(ValueError):
            raw_external_table_definition("/local/path.ndjson")
