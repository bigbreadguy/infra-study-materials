from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = REPO_ROOT / "dags" / "mongo-data-ingestion.py"


class MongoDataIngestionDagTest(unittest.TestCase):
    def test_dag_no_longer_depends_on_dataform_hook(self):
        dag_source = DAG_FILE.read_text()

        self.assertNotIn("DataformHook", dag_source)
        self.assertNotIn('Variable.get("raw_gcs_uri"', dag_source)
        self.assertIn("BigQueryHook", dag_source)

    def test_extraction_targets_grains_through_structured_variable(self):
        dag_source = DAG_FILE.read_text()

        self.assertNotIn("mongo_grain_id", dag_source)
        self.assertIn("GRAIN_TARGETS_VARIABLE", dag_source)
        self.assertIn("parse_grain_targets(raw)", dag_source)
        # The find criteria must include datasetId so the query stays on the
        # compound index over datasetId, grainId, and ts.
        self.assertIn('"datasetId": dataset_id,', dag_source)
        self.assertIn("ingest_grain.expand(target=grain_targets)", dag_source)
        # The curated description rides the extract result so the dim
        # grains merge can prefer it over the raw document description.
        self.assertIn('"grain_description": target["description"],', dag_source)
        self.assertIn(
            'config["grain_description"] = raw_location.get("grain_description")',
            dag_source,
        )

    def test_dag_serializes_runs_and_extracts_full_utc_day(self):
        dag_source = DAG_FILE.read_text()

        self.assertIn("max_active_runs=1", dag_source)
        self.assertIn('"retries": 2', dag_source)
        # Cron trigger timetables derive the data interval from the trigger
        # wall clock, so the logical date must drive the extraction window
        # and expand to the full utc day.
        self.assertIn(
            'context["logical_date"] or context["data_interval_start"]',
            dag_source,
        )
        self.assertIn('in_timezone("UTC").start_of("day")', dag_source)
        self.assertIn("start_date.add(days=1)", dag_source)
        self.assertIn('start_date=datetime(1996, 4, 1, tz="UTC")', dag_source)

    def test_extraction_skips_grain_with_no_samples_in_interval(self):
        dag_source = DAG_FILE.read_text()

        # A grain with no samples at the logical date must skip, not fail,
        # so the per index transform and load tasks sit out the run while
        # other grains proceed. The point probe for a wrong dataset id to
        # grain id mapping must keep failing loudly.
        self.assertIn("from airflow.sdk.exceptions import AirflowSkipException", dag_source)
        self.assertIn("if not lines:", dag_source)
        self.assertIn("raise AirflowSkipException(", dag_source)
        self.assertIn("raise ValueError(", dag_source)
        # Trigger rules only narrow a skipped upstream to the matching map
        # index when both tasks share a mapped task group; without it one
        # empty grain skips the transform and load tasks for every grain.
        self.assertIn("@task_group()", dag_source)
        self.assertIn("def ingest_grain(target: dict):", dag_source)

    def test_dag_scopes_raw_inputs_through_config_without_module_patch(self):
        dag_source = DAG_FILE.read_text()

        self.assertNotIn("bq_sql.RAW_DATA_SAMPLES_TABLE", dag_source)
        self.assertIn('config["raw_table_id"] = _grain_raw_table_id(grain_id)', dag_source)
        self.assertIn(
            "gs://{raw_location['bucket']}/{raw_location['object']}",
            dag_source,
        )

    def test_dag_imports_with_expected_tasks_when_airflow_is_available(self):
        try:
            import airflow  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("Airflow is not installed in the local test environment")

        spec = importlib.util.spec_from_file_location(
            "mongo_data_ingestion_dag",
            DAG_FILE,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        dag = module.dag
        task_ids = set(dag.task_ids)

        self.assertEqual(dag.dag_id, "mongo-data-ingestion")
        self.assertEqual(
            {
                "get_grain_targets",
                "ingest_grain.extract_raw_data_to_gcs",
                "ingest_grain.raw_data_samples",
                "ingest_grain.dim_grains",
                "ingest_grain.dim_metrics",
                "ingest_grain.fact_values",
            },
            task_ids,
        )
