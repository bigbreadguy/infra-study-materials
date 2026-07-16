from __future__ import annotations

from unittest import TestCase

from external_data.common.bigquery_grain_metadata import (
    GrainMetadataTransformConfig,
    run_merge_dim_grain_metadata,
)
from external_data.common.bigquery_grain_metadata_sql import (
    dim_grain_metadata_merge_sql,
    raw_grain_catalog_sql,
)


PROJECT_ID = "example-study-proj"
DATASET_ID = "dl_materials"
RAW_GCS_URI = "gs://example-raw-bucket/bloomberg/raw/catalog/raw.ndjson"


class _FakeQueryJob:
    job_id = "job-123"
    location = "asia-northeast3"

    def result(self):
        return None


class _FakeBigQueryClient:
    def __init__(self):
        self.queries = []

    def query(self, sql, location=None, job_config=None, job_id_prefix=None):
        self.queries.append((sql, location))
        return _FakeQueryJob()


class BigQueryGrainMetadataSqlTest(TestCase):
    def test_raw_external_table_sql_keeps_info_json_and_string_keys(self):
        sql = raw_grain_catalog_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_gcs_uri=RAW_GCS_URI,
        )

        self.assertIn(
            "`example-study-proj.dl_materials.raw_grain_catalog`",
            sql,
        )
        self.assertIn("datasetId STRING", sql)
        self.assertIn("grainId STRING", sql)
        self.assertIn("catalogId STRING", sql)
        self.assertIn("catalogName STRING", sql)
        self.assertIn("info JSON", sql)
        self.assertIn(f"uris = ['{RAW_GCS_URI}']", sql)

    def test_raw_external_table_sql_appends_row_count_assertion(self):
        sql = raw_grain_catalog_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            raw_gcs_uri=RAW_GCS_URI,
            expected_row_count=93,
        )

        self.assertIn("CREATE OR REPLACE EXTERNAL TABLE", sql)
        self.assertIn(") = 93 AS 'Raw external table row count must match", sql)

        with self.assertRaises(ValueError):
            raw_grain_catalog_sql(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_gcs_uri=RAW_GCS_URI,
                expected_row_count=-1,
            )

    def test_merge_sql_bootstraps_table_keys_on_grain_pair_and_asserts_uniqueness(self):
        sql = dim_grain_metadata_merge_sql(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )

        # The DAG owns the table; the merge opens with idempotent DDL.
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS "
            "`example-study-proj.dl_materials.dim_grain_metadata`",
            sql,
        )
        self.assertLess(
            sql.index("CREATE TABLE IF NOT EXISTS"),
            sql.index("MERGE `example-study-proj.dl_materials.dim_grain_metadata`"),
        )
        self.assertIn(
            "MERGE `example-study-proj.dl_materials.dim_grain_metadata`",
            sql,
        )
        self.assertIn(
            "`example-study-proj.dl_materials.raw_grain_catalog`",
            sql,
        )
        # Grain identity is the (dataset_id, grain_id) pair.
        self.assertIn("ON target.id = source.id", sql)
        self.assertIn("AND target.name = source.name", sql)
        self.assertIn("GROUP BY datasetId, grainId", sql)
        self.assertIn(
            "'dim_grain_metadata must keep one row per dataset id and grain name pair.'",
            sql,
        )

    def test_sql_rejects_unsafe_identifiers_and_uris(self):
        with self.assertRaises(ValueError):
            raw_grain_catalog_sql(
                project_id="bad`project",
                dataset_id=DATASET_ID,
                raw_gcs_uri=RAW_GCS_URI,
            )
        with self.assertRaises(ValueError):
            raw_grain_catalog_sql(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_gcs_uri="gs://bucket/object'with-quote",
            )

    def test_runner_executes_merge_in_configured_region(self):
        client = _FakeBigQueryClient()
        config = GrainMetadataTransformConfig(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            region="asia-northeast3",
            raw_gcs_uri=RAW_GCS_URI,
        )

        result = run_merge_dim_grain_metadata(client, config)

        self.assertEqual("job-123", result["job_id"])
        (sql, location), = client.queries
        self.assertIn("MERGE", sql)
        self.assertEqual("asia-northeast3", location)

    def test_runner_rejects_empty_config_values(self):
        config = GrainMetadataTransformConfig(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            region="asia-northeast3",
            raw_gcs_uri="",
        )

        with self.assertRaises(ValueError):
            run_merge_dim_grain_metadata(_FakeBigQueryClient(), config)
