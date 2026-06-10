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
                "get_grain_ids",
                "extract_raw_data_to_gcs",
                "raw_data_samples",
                "dim_grains",
                "dim_metrics",
                "fact_values",
            },
            task_ids,
        )
