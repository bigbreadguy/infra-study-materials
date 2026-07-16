from __future__ import annotations

from unittest import TestCase

from external_data.common.bigquery_market_index import (
    BigQueryTransformConfig,
    run_combined_transform,
    run_merge_dim_grains,
)
from external_data.common.bigquery_market_index_sql import (
    RAW_FIELD_ORDER,
    combined_transform_sql,
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
        fields = {
            field["name"]: field["type"]
            for field in definition["schema"]["fields"]
        }
        # The Mongo Extended-JSON fields stay JSON; the curated per-grain fields
        # the extractor injects are plain STRING.
        self.assertEqual(len(fields), len(RAW_FIELD_ORDER))
        self.assertEqual(fields["_id"], "JSON")
        self.assertEqual(fields["datasetId"], "JSON")
        self.assertEqual(fields["ts"], "JSON")
        self.assertEqual(fields["data"], "JSON")
        self.assertEqual(fields["grainId"], "STRING")
        self.assertEqual(fields["_target_freq"], "STRING")
        self.assertEqual(fields["_target_description"], "STRING")

        with self.assertRaises(ValueError):
            raw_external_table_definition("s3://example-raw-bucket/raw/*")
        with self.assertRaises(ValueError):
            raw_external_table_definition("")

    def test_parquet_external_table_definition_is_all_string(self):
        definition = raw_external_table_definition(
            RAW_GCS_URI, source_format="PARQUET"
        )

        self.assertEqual(definition["sourceFormat"], "PARQUET")
        # Parquet has no JSON type; the merge SQL reads the Extended-JSON text
        # through JSON_VALUE on STRING columns, so every column is STRING.
        self.assertNotIn("ignoreUnknownValues", definition)
        fields = {
            field["name"]: field["type"]
            for field in definition["schema"]["fields"]
        }
        self.assertEqual(list(fields), list(RAW_FIELD_ORDER))
        self.assertTrue(all(ftype == "STRING" for ftype in fields.values()))

        with self.assertRaises(ValueError):
            raw_external_table_definition(RAW_GCS_URI, source_format="CSV")

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

    def test_dim_merges_read_curated_description_from_target_column(self):
        # Per-grain curated description is no longer a SQL scalar; it rides in on
        # the _target_description column so one combined job serves many grains,
        # each with its own description.
        grain_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID, dataset_id=DATASET_ID
        )
        self.assertIn("_target_description AS curated_description", grain_sql)
        self.assertIn(
            "COALESCE(NULLIF(MAX(curated_description), ''), NULLIF(ARRAY_AGG(",
            grain_sql,
        )
        # No grain identity is baked in as a SQL literal anymore.
        self.assertNotIn("COALESCE('", grain_sql)

        metric_sql = dim_metrics_merge_sql(
            project_id=PROJECT_ID, dataset_id=DATASET_ID
        )
        self.assertIn(
            "raw._target_description AS curated_description", metric_sql
        )
        self.assertIn(
            "COALESCE(NULLIF(curated_description, ''), grain_description, "
            "grain_name)",
            metric_sql,
        )
        self.assertNotIn("COALESCE('", metric_sql)

    def test_dimension_merge_sql_asserts_key_uniqueness_after_merge(self):
        grain_sql = dim_grains_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        metric_sql = dim_metrics_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

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
        self.assertEqual(
            sql.count(
                "ON grain.id = fact.grain_id\n    AND grain.name = fact.grain_name"
            ),
            2,
        )

    def test_fact_sql_reads_time_grain_from_target_freq_column(self):
        # Per-grain time grain rides in on the _target_freq column, so one
        # combined job serves grains with different time grains.
        sql = fact_values_merge_sql(project_id=PROJECT_ID, dataset_id=DATASET_ID)
        self.assertIn(
            "COALESCE(NULLIF(raw._target_freq, ''), 'D') AS time_grain,", sql
        )
        self.assertIn("raw.time_grain,", sql)
        # No time grain is baked in as a SQL literal anymore.
        self.assertNotIn("'W' AS time_grain", sql)

    def test_fact_sql_post_merge_uniqueness_assert(self):
        sql = fact_values_merge_sql(project_id=PROJECT_ID, dataset_id=DATASET_ID)
        # The merge keeps one row per key going forward; a cheap post-merge
        # ASSERT over the touched range guards the invariant.
        self.assertIn(
            "fact_values must keep one row per sample, grain, metric, date, "
            "and time grain in the merged range.",
            sql,
        )
        self.assertGreater(
            sql.rindex("ASSERT ("),
            sql.index("MERGE `"),
            "the uniqueness assert must run after the merge",
        )

    def test_fact_sql_dedup_cleanup_is_opt_in(self):
        sql = fact_values_merge_sql(project_id=PROJECT_ID, dataset_id=DATASET_ID)

        # Source dedup always runs; one bad raw export must not duplicate.
        self.assertIn(
            "QUALIFY ROW_NUMBER() OVER (\n"
            "  PARTITION BY sample_id, grain_id, metric_name, "
            "logical_date, time_grain\n"
            "  ORDER BY updated_at DESC, source_file_name DESC\n"
            ") = 1;",
            sql,
        )
        # The legacy-duplicate DELETE is gated off by default: steady state
        # never needs it (source deduped + writes serialized).
        self.assertNotIn("DELETE FROM", sql)

        cleanup_sql = fact_values_merge_sql(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, dedup_cleanup=True
        )
        self.assertIn(
            "DELETE FROM `example-study-proj.dl_bloomberg_data.fact_values`",
            cleanup_sql,
        )
        self.assertIn("WHERE row_rank > 1", cleanup_sql)
        self.assertLess(
            cleanup_sql.index("DELETE FROM"),
            cleanup_sql.index("MERGE `"),
            "the one-time cleanup must run before the merge",
        )

    def test_combined_transform_sql_is_one_ordered_script(self):
        sql = combined_transform_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            expected_row_count=7,
        )

        # Both DECLAREs hoisted to the literal top of the script (BigQuery
        # requires declarations at the start of a block).
        self.assertTrue(
            sql.lstrip().startswith("DECLARE min_candidate_logical_date")
        )
        self.assertEqual(sql.count("DECLARE "), 2)

        # Validate -> dim_grains -> dim_metrics -> fact (temp then merge).
        order = [
            sql.index(") = 7 AS 'Raw external table row count"),
            sql.index("MERGE `example-study-proj.dl_bloomberg_data.dim_grains`"),
            sql.index(
                "MERGE `example-study-proj.dl_bloomberg_data.dim_metrics`"
            ),
            sql.index("CREATE TEMP TABLE fact_candidates"),
            sql.index(
                "MERGE `example-study-proj.dl_bloomberg_data.fact_values`"
            ),
        ]
        self.assertEqual(order, sorted(order))

        # validate + 2 dim uniqueness + fact dims-resolvable + fact uniqueness.
        self.assertEqual(sql.count("ASSERT ("), 5)
        # Three real MERGE statements (one per target table).
        self.assertEqual(
            sql.count("MERGE `example-study-proj.dl_bloomberg_data."), 3
        )
        self.assertNotIn("DELETE FROM", sql)

        cleanup_sql = combined_transform_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            expected_row_count=7,
            dedup_cleanup=True,
        )
        self.assertEqual(cleanup_sql.count("DECLARE "), 2)
        self.assertIn("DELETE FROM", cleanup_sql)

    def test_runner_attaches_temp_raw_definition_to_each_job(self):
        if not _bigquery_available():
            self.skipTest("google-cloud-bigquery is not installed")

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
            {
                "job_id": "job-example-001",
                "location": "asia-northeast3",
                "dml_affected_rows": 42,
            },
        )
        query = client.queries[0]
        self.assertEqual(query["location"], "asia-northeast3")
        self.assertIn(
            "MERGE `example-study-proj.dl_bloomberg_data.dim_grains`",
            query["sql"],
        )
        definitions = query["job_config"].table_definitions
        self.assertEqual(list(definitions), ["raw_data_samples"])
        self.assertEqual(
            definitions["raw_data_samples"].source_uris,
            [RAW_GCS_URI],
        )
        self.assertIsNone(query["job_id_prefix"])

        run_merge_dim_grains(
            client,
            config,
            labels={"category": "copper", "step": "dim_grains"},
            job_id_prefix="dpanda_copper_dim_grains_",
        )
        labeled_query = client.queries[1]
        self.assertEqual(
            labeled_query["job_config"].labels,
            {"category": "copper", "step": "dim_grains"},
        )
        self.assertEqual(
            labeled_query["job_id_prefix"], "dpanda_copper_dim_grains_"
        )

    def test_run_combined_transform_reports_per_statement_child_stats(self):
        if not _bigquery_available():
            self.skipTest("google-cloud-bigquery is not installed")

        config = BigQueryTransformConfig(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            region="asia-northeast3",
            raw_gcs_uri=RAW_GCS_URI,
            expected_raw_row_count=12,
            raw_source_format="PARQUET",
        )
        client = FakeScriptClient(
            children=[
                _FakeChild("c1", "ASSERT", None, 0),
                _FakeChild("c2", "MERGE", 5, 1),
                _FakeChild("c3", "MERGE", 9, 2),
                _FakeChild("c4", "MERGE", 30, 3),
            ]
        )

        result = run_combined_transform(
            client,
            config,
            labels={"category": "copper", "step": "transform"},
            job_id_prefix="dpanda_copper_transform_",
        )

        # One submitted job; the whole transform is one BigQuery job.
        self.assertEqual(len(client.queries), 1)
        self.assertIn("CREATE TEMP TABLE fact_candidates", client.queries[0]["sql"])
        # Parquet source format flows into the external definition.
        definitions = client.queries[0]["job_config"].table_definitions
        self.assertEqual(
            definitions["raw_data_samples"].source_format, "PARQUET"
        )
        # Terminal DML rows = last child performing DML (the fact merge).
        self.assertEqual(result["dml_affected_rows"], 30)
        self.assertEqual(
            [statement["statement_type"] for statement in result["statements"]],
            ["ASSERT", "MERGE", "MERGE", "MERGE"],
        )
        self.assertEqual(
            [statement["dml_affected_rows"] for statement in result["statements"]],
            [None, 5, 9, 30],
        )


def _bigquery_available() -> bool:
    try:
        # pyrefly: ignore [missing-import]
        from google.cloud import bigquery  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


class FakeBigQueryJob:
    job_id = "job-example-001"
    location = "asia-northeast3"
    # The single-statement runner surfaces the job's affected-row count directly.
    num_dml_affected_rows = 42

    def result(self):
        return None


class FakeBigQueryClient:
    def __init__(self):
        self.queries = []

    def query(self, sql, *, location, job_config=None, job_id_prefix=None):
        self.queries.append(
            {
                "sql": sql,
                "location": location,
                "job_config": job_config,
                "job_id_prefix": job_id_prefix,
            }
        )
        return FakeBigQueryJob()


class _FakeChild:
    def __init__(self, job_id, statement_type, num_dml_affected_rows, created):
        self.job_id = job_id
        self.statement_type = statement_type
        self.num_dml_affected_rows = num_dml_affected_rows
        self.created = created


class FakeScriptJob:
    job_id = "script-job-001"
    location = "asia-northeast3"
    # A multi-statement script reports no DML on the parent; the runner walks
    # child jobs instead.
    num_dml_affected_rows = None

    def result(self):
        return None


class FakeScriptClient:
    def __init__(self, children):
        self.queries = []
        self._children = children

    def query(self, sql, *, location, job_config=None, job_id_prefix=None):
        self.queries.append(
            {
                "sql": sql,
                "location": location,
                "job_config": job_config,
                "job_id_prefix": job_id_prefix,
            }
        )
        return FakeScriptJob()

    def list_jobs(self, parent_job=None):
        return list(self._children)
