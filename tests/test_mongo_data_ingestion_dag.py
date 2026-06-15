from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = REPO_ROOT / "dags" / "mongo-data-ingestion.py"

sys.path.append(str(REPO_ROOT / "dags"))

from common.grain_targets import list_grain_target_categories


class MongoDataIngestionDagTest(unittest.TestCase):
    def test_dag_no_longer_depends_on_dataform_hook(self):
        dag_source = DAG_FILE.read_text()

        self.assertNotIn("DataformHook", dag_source)
        self.assertNotIn('Variable.get("raw_gcs_uri"', dag_source)
        self.assertIn("BigQueryHook", dag_source)

    def test_extraction_targets_grains_through_file_based_config(self):
        dag_source = DAG_FILE.read_text()

        self.assertNotIn("mongo_grain_id", dag_source)
        self.assertIn("load_grain_targets(", dag_source)
        self.assertNotIn("GRAIN_TARGETS_VARIABLE", dag_source)
        self.assertNotIn("Variable.get(GRAIN", dag_source)
        # The find criteria must include datasetId so the query stays on the
        # compound index over datasetId, grainId, and ts.
        self.assertIn('"datasetId": dataset_id,', dag_source)
        self.assertIn(
            "transform_grain.expand(raw_location=raw_locations)", dag_source
        )
        # The curated description rides the extract result so the dim
        # grains merge can prefer it over the raw document description.
        self.assertIn('"grain_description": target["description"],', dag_source)
        self.assertIn(
            'config["grain_description"] = raw_location.get("grain_description")',
            dag_source,
        )
        # The target freq rides the extract result the same way so the
        # fact values merge writes it as the time grain.
        self.assertIn('"time_grain": target["freq"],', dag_source)
        self.assertIn(
            'config["time_grain"] = raw_location.get("time_grain")',
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

    def test_dag_factory_discovers_categories_and_defaults_to_manual(self):
        dag_source = DAG_FILE.read_text()

        # Categories come from the gitignored config file stems, never a
        # hardcoded registry, so a fresh clone with no local config files
        # produces no DAGs instead of import errors.
        self.assertIn("for _category in list_grain_target_categories():", dag_source)
        self.assertIn("SCHEDULE_OVERRIDES.get(_category)", dag_source)
        self.assertIn("schedule=schedule", dag_source)
        self.assertIn("globals()[_dag.dag_id] = _dag", dag_source)

    def test_batch_extraction_omits_quiet_grains_without_failing(self):
        dag_source = DAG_FILE.read_text()

        # The category extracts in one batch task over a single Mongo
        # session: per task fixed costs (worker start, variable fetches,
        # Mongo connection) dominate the tiny daily volume, so they must be
        # paid once per category, not once per grain.
        self.assertIn("for target in targets:", dag_source)
        self.assertNotIn("get_grain_targets", dag_source)
        # A grain with no samples at the logical date is a legitimate quiet
        # day: it yields no raw location, so its transform chain never
        # expands while other grains proceed. The point probe for a wrong
        # dataset id to grain id mapping must keep failing loudly.
        self.assertNotIn("AirflowSkipException", dag_source)
        self.assertIn("if not lines:", dag_source)
        self.assertIn("quiet_grains.append(", dag_source)
        self.assertIn("raise ValueError(", dag_source)
        # The per grain transform chain still expands as one mapped task
        # group so each raw location rides one chain end to end.
        self.assertIn("@task_group()", dag_source)
        self.assertIn("def transform_grain(raw_location: dict):", dag_source)

    def test_dag_reads_raw_through_per_query_temp_definition(self):
        dag_source = DAG_FILE.read_text()

        # No persistent raw external tables: each BigQuery job resolves the
        # raw name through a temporary external table definition scoped to
        # the one object this run uploaded, so concurrent grain chains never
        # share raw table state and the dataset stays free of raw_* clutter.
        self.assertNotIn("_grain_raw_table_id", dag_source)
        self.assertNotIn("raw_table_id", dag_source)
        self.assertNotIn("CREATE OR REPLACE EXTERNAL TABLE", dag_source)
        self.assertIn("run_validate_raw_data", dag_source)
        self.assertIn(
            "gs://{raw_location['bucket']}/{raw_location['object']}",
            dag_source,
        )

    def test_dag_imports_with_expected_tasks_when_airflow_is_available(self):
        try:
            import airflow  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("Airflow is not installed in the local test environment")

        categories = list_grain_target_categories()
        if not categories:
            self.skipTest(
                "No grain target config files present; they are gitignored "
                "and local-only"
            )

        spec = importlib.util.spec_from_file_location(
            "mongo_data_ingestion_dag",
            DAG_FILE,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        expected_task_ids = {
            "extract_raw_data_to_gcs",
            "transform_grain.validate_raw_data",
            "transform_grain.dim_grains",
            "transform_grain.dim_metrics",
            "transform_grain.fact_values",
        }

        self.assertFalse(hasattr(module, "dag"))
        for category in categories:
            with self.subTest(category=category):
                dag = getattr(module, f"mongo-data-ingestion-{category}")

                self.assertEqual(
                    dag.dag_id,
                    f"mongo-data-ingestion-{category}",
                )
                self.assertEqual(
                    module.SCHEDULE_OVERRIDES.get(category),
                    getattr(dag, "schedule", None),
                )
                self.assertIn(category, dag.tags)
                self.assertEqual(expected_task_ids, set(dag.task_ids))
