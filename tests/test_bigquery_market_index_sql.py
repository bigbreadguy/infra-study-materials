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
    raw_data_samples_check_sql,
    raw_external_table_definition,
)


PROJECT_ID = "example-study-proj"
DATASET_ID = "dl_bloomberg_data"
RAW_GCS_URI = "gs://example-raw-bucket/bloomberg/raw/*"


class BigQueryMarketIndexSqlTest(TestCase):
    def test_raw_external_table_definition_keeps_json_columns_and_uri(self):
        definition = raw_external_table_definition(RAW_GCS_URI)

        self.assertEqual(definition["sourceFormat"], "NEWLINE_DELIMITED_JSON")
        self.assertTrue(definition["ignoreUnknownValues"])
        self.assertEqual(definition["sourceUris"], [RAW_GCS_URI])
        # Mongo exports use Extended JSON for ObjectId and Date fields, which
        # must stay JSON at the external-table boundary.
        fields = {
            field["name"]: field["type"]
            for field in definition["schema"]["fields"]
        }
        self.assertEqual(len(fields), 9)
        self.assertEqual(fields["_id"], "JSON")
        self.assertEqual(fields["datasetId"], "JSON")
        self.assertEqual(fields["ts"], "JSON")
        self.assertEqual(fields["data"], "JSON")
        self.assertEqual(fields["grainId"], "STRING")

        with self.assertRaises(ValueError):
            raw_external_table_definition("s3://example-raw-bucket/raw/*")
        with self.assertRaises(ValueError):
            raw_external_table_definition("")

    def test_raw_check_sql_asserts_extracted_row_count(self):
        sql = raw_data_samples_check_sql(expected_row_count=3)

        self.assertIn("FROM raw_data_samples", sql)
        self.assertIn(") = 3 AS 'Raw external table row count must match", sql)

        count_sql = raw_data_samples_check_sql()
        self.assertIn("SELECT COUNT(*) AS row_count", count_sql)
        self.assertNotIn("ASSERT", count_sql)

        with self.assertRaises(ValueError):
            raw_data_samples_check_sql(expected_row_count=-1)

    def test_sql_builders_read_raw_through_bare_temp_table_name(self):
        grain_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        metric_sql = dim_metrics_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        fact_sql = fact_values_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

        # The raw name must stay bare so each job resolves it through its
        # temporary external table definition; only dim and fact tables are
        # persistent and qualified.
        self.assertIn("FROM raw_data_samples\n", grain_sql)
        self.assertIn("FROM raw_data_samples AS raw", metric_sql)
        self.assertIn("FROM raw_data_samples AS raw", fact_sql)
        for sql in (grain_sql, metric_sql, fact_sql):
            self.assertNotIn(f"{DATASET_ID}.raw_data_samples", sql)

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

    def test_fact_sql_takes_time_grain_from_the_target_freq(self):
        sql = fact_values_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            time_grain="W",
        )

        self.assertIn("'W' AS time_grain,", sql)

        default_sql = fact_values_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        self.assertIn("'D' AS time_grain,", default_sql)

        with self.assertRaises(ValueError):
            fact_values_merge_sql(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                time_grain="W'; DROP TABLE x",
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

    def test_runner_attaches_temp_raw_definition_to_each_job(self):
        try:
            # pyrefly: ignore [missing-import]
            from google.cloud import bigquery  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest(
                "google-cloud-bigquery is not installed in the local test "
                "environment"
            )

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
        query = client.queries[0]
        self.assertEqual(query["location"], "asia-northeast3")
        self.assertIn(
            "MERGE `example-study-proj.dl_bloomberg_data.dim_grains`",
            query["sql"],
        )
        # Every job must carry its own temporary raw table definition scoped
        # to the run's GCS object; no persistent raw table exists.
        definitions = query["job_config"].table_definitions
        self.assertEqual(list(definitions), ["raw_data_samples"])
        self.assertEqual(
            definitions["raw_data_samples"].source_uris,
            [RAW_GCS_URI],
        )


class FakeBigQueryJob:
    job_id = "job-example-001"
    location = "asia-northeast3"

    def result(self):
        return None


class FakeBigQueryClient:
    def __init__(self):
        self.queries = []

    def query(self, sql, *, location, job_config=None):
        self.queries.append(
            {"sql": sql, "location": location, "job_config": job_config}
        )
        return FakeBigQueryJob()
