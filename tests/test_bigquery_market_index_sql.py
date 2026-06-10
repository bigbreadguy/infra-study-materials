from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.bigquery_market_index import (
    BigQueryTransformConfig,
    run_merge_dim_grains,
)
from common.bigquery_market_index_sql import (
    dim_grains_merge_sql,
    dim_metrics_merge_sql,
    fact_values_merge_sql,
    raw_data_samples_sql,
)


PROJECT_ID = "example-study-proj"
DATASET_ID = "dl_bloomberg_data"
RAW_GCS_URI = "gs://example-raw-bucket/bloomberg/raw/*"


class BigQueryMarketIndexSqlTest(TestCase):
    def test_raw_external_table_sql_includes_json_columns_and_raw_uri(self):
        sql = raw_data_samples_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_gcs_uri=RAW_GCS_URI,
        )

        self.assertIn(
            "`example-study-proj.dl_bloomberg_data.raw_data_samples`",
            sql,
        )
        self.assertIn("_id JSON", sql)
        self.assertIn("datasetId JSON", sql)
        self.assertIn("ts JSON", sql)
        self.assertIn("data JSON", sql)
        self.assertIn(f"uris = ['{RAW_GCS_URI}']", sql)

    def test_raw_external_table_sql_appends_row_count_assertion(self):
        sql = raw_data_samples_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_gcs_uri=RAW_GCS_URI,
            expected_row_count=3,
        )

        self.assertIn("CREATE OR REPLACE EXTERNAL TABLE", sql)
        self.assertIn(") = 3 AS 'Raw external table row count must match", sql)

        with self.assertRaises(ValueError):
            raw_data_samples_sql(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_gcs_uri=RAW_GCS_URI,
                expected_row_count=-1,
            )

    def test_sql_builders_accept_custom_raw_table_id(self):
        raw_table_id = "raw_data_samples_SX5E_Index"
        qualified = (
            "`example-study-proj.dl_bloomberg_data.raw_data_samples_SX5E_Index`"
        )

        raw_sql = raw_data_samples_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_gcs_uri=RAW_GCS_URI,
            raw_table_id=raw_table_id,
        )
        grain_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_table_id=raw_table_id,
        )
        metric_sql = dim_metrics_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_table_id=raw_table_id,
        )
        fact_sql = fact_values_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_table_id=raw_table_id,
        )

        for sql in (raw_sql, grain_sql, metric_sql, fact_sql):
            self.assertIn(qualified, sql)

    def test_dimension_merge_sql_preserves_merge_keys(self):
        grain_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        metric_sql = dim_metrics_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

        # A dataset holds several grains, so grain identity is the dataset
        # id and grain name pair; keying dim_grains on the dataset id alone
        # made sibling grains fight over one row.
        self.assertIn(
            "ON target.id = source.id\n  AND target.name = source.name",
            grain_sql,
        )
        self.assertIn("GROUP BY id, name", grain_sql)
        self.assertIn("_FILE_NAME AS source_file_name", grain_sql)
        self.assertIn("ON target.grain_id = source.grain_id", metric_sql)
        self.assertIn("AND target.name = source.name", metric_sql)
        self.assertIn(
            "ON grain.id = raw.grain_id\n      AND grain.name = raw.grain_name",
            metric_sql,
        )
        self.assertIn("VALUES (GENERATE_UUID(), source.grain_id", metric_sql)
        self.assertIn("'OpenInterest' AS metric_suffix", metric_sql)
        self.assertIn("'Value' AS metric_suffix", metric_sql)

    def test_dim_grains_merge_sql_prefers_target_variable_description(self):
        sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            grain_description="euro stoxx 50 index daily ohlcv",
        )

        # The curated description from the grain targets variable wins over
        # whatever the raw Mongo documents carry, which is usually nothing.
        self.assertIn(
            "COALESCE('euro stoxx 50 index daily ohlcv', NULLIF(ARRAY_AGG(",
            sql,
        )

        default_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        self.assertNotIn("COALESCE('", default_sql)

        with self.assertRaises(ValueError):
            dim_grains_merge_sql(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                grain_description="bad 'quoted' description",
            )

    def test_dimension_merge_sql_asserts_key_uniqueness_after_merge(self):
        grain_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        metric_sql = dim_metrics_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

        # Key duplication must fail loudly at the dim merge that caused it,
        # not three tasks later when the fact merge fans out its source.
        self.assertIn(
            "'dim_grains must keep one row per dataset id and grain name pair.'",
            grain_sql,
        )
        self.assertIn(
            "'dim_metrics must keep one row per grain id and metric name pair.'",
            metric_sql,
        )
        for sql in (grain_sql, metric_sql):
            self.assertLess(
                sql.index("MERGE `"),
                sql.index("ASSERT ("),
                "the uniqueness assert must run after the merge",
            )

    def test_fact_sql_includes_assertion_dates_and_partition_filter(self):
        sql = fact_values_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

        self.assertIn("DECLARE min_candidate_logical_date DATE DEFAULT NULL", sql)
        self.assertIn("DECLARE max_candidate_logical_date DATE DEFAULT NULL", sql)
        self.assertIn("ASSERT (", sql)
        self.assertIn(
            "Every fact candidate must resolve dim_grains and dim_metrics",
            sql,
        )
        self.assertIn(
            "target.logical_date BETWEEN min_candidate_logical_date "
            "AND max_candidate_logical_date",
            sql,
        )
        self.assertIn(
            "ON target.sample_id = source.sample_id\n"
            "  AND target.grain_id = source.grain_id\n"
            "  AND target.metric_id = source.metric_id\n"
            "  AND target.logical_date = source.logical_date",
            sql,
        )
        self.assertIn("VALUES (\n    GENERATE_UUID()", sql)
        self.assertIn("metric_value", sql)
        # Both dim_grains joins must carry the grain name; joining on the
        # shared dataset id alone fans one candidate into multiple source
        # rows and breaks MERGE.
        self.assertEqual(
            sql.count(
                "ON grain.id = fact.grain_id\n    AND grain.name = fact.grain_name"
            ),
            2,
        )

    def test_fact_sql_deduplicates_source_and_target_rows(self):
        sql = fact_values_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

        self.assertIn(
            "QUALIFY ROW_NUMBER() OVER (\n"
            "  PARTITION BY sample_id, grain_id, metric_name, "
            "logical_date, time_grain\n"
            "  ORDER BY updated_at DESC, source_file_name DESC\n"
            ") = 1;",
            sql,
        )
        self.assertIn(
            "DELETE FROM `example-study-proj.dl_bloomberg_data.fact_values`",
            sql,
        )
        self.assertIn(
            "PARTITION BY sample_id, grain_id, metric_id, "
            "logical_date, time_grain",
            sql,
        )
        self.assertIn("ORDER BY ingested_at DESC, id", sql)
        self.assertIn("WHERE row_rank > 1", sql)
        delete_index = sql.index("DELETE FROM")
        merge_index = sql.index("MERGE `")
        self.assertLess(
            delete_index,
            merge_index,
            "target dedup must run before the merge",
        )

    def test_runner_executes_script_with_configured_region(self):
        config = BigQueryTransformConfig(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            region="asia-northeast3",
            raw_gcs_uri=RAW_GCS_URI,
        )
        client = FakeBigQueryClient()

        result = run_merge_dim_grains(client, config)

        self.assertEqual(
            result,
            {"job_id": "job-example-001", "location": "asia-northeast3"},
        )
        self.assertEqual(client.queries[0]["location"], "asia-northeast3")
        self.assertIn(
            "MERGE `example-study-proj.dl_bloomberg_data.dim_grains`",
            client.queries[0]["sql"],
        )


class FakeBigQueryJob:
    job_id = "job-example-001"
    location = "asia-northeast3"

    def result(self):
        return None


class FakeBigQueryClient:
    def __init__(self):
        self.queries = []

    def query(self, sql, *, location):
        self.queries.append({"sql": sql, "location": location})
        return FakeBigQueryJob()
