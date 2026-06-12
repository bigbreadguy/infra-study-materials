from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = REPO_ROOT / "dags" / "mongo-grain-metadata-ingestion.py"

sys.path.append(str(REPO_ROOT / "dags"))


class MongoGrainMetadataDagTest(unittest.TestCase):
    def test_dag_routes_gcp_access_through_impersonation_hooks(self):
        dag_source = DAG_FILE.read_text()

        # This sandbox keeps the multi-SA lesson design: GCS and BigQuery
        # access impersonate task-scoped service accounts via hooks.
        self.assertIn("GCSHook", dag_source)
        self.assertIn("BigQueryHook", dag_source)
        self.assertIn("gcs_impersonation_chain", dag_source)
        self.assertIn("bigquery_impersonation_chain", dag_source)

    def test_extraction_queries_catalog_by_nested_grain_id(self):
        dag_source = DAG_FILE.read_text()

        # Catalog documents key grain identity at process.grainId; the
        # catalog datasetId can diverge from the target dataset_id, so the
        # find must not filter on it.
        self.assertIn('"process.grainId": target["grain_id"]', dag_source)
        self.assertNotIn('"process.datasetId":', dag_source)
        self.assertIn("mongo_catalog_collection_name", dag_source)
        self.assertIn("select_catalog_document(", dag_source)
        self.assertIn("collect_unique_grain_targets()", dag_source)

    def test_rows_key_on_target_dataset_id_for_dim_grains_join(self):
        dag_source = DAG_FILE.read_text()

        self.assertIn('"datasetId": target["dataset_id"]', dag_source)
        self.assertIn('"grainId": target["grain_id"]', dag_source)

    def test_missing_catalog_documents_warn_instead_of_failing(self):
        dag_source = DAG_FILE.read_text()

        self.assertIn("missing_grain_ids.append", dag_source)
        self.assertIn(
            "from airflow.sdk.exceptions import AirflowSkipException",
            dag_source,
        )
        self.assertIn("if not lines:", dag_source)
        self.assertIn("raise AirflowSkipException(", dag_source)

    def test_dag_stays_manual_with_serialized_runs(self):
        dag_source = DAG_FILE.read_text()

        self.assertIn("schedule=None", dag_source)
        self.assertIn("max_active_runs=1", dag_source)
        self.assertIn("catchup=False", dag_source)
        self.assertIn('"retries": 2', dag_source)

    def test_dag_scopes_raw_input_to_the_uploaded_object(self):
        dag_source = DAG_FILE.read_text()

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
            "mongo_grain_metadata_ingestion_dag",
            DAG_FILE,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        dag = module.dag
        self.assertEqual("mongo-grain-metadata-ingestion", dag.dag_id)
        self.assertIsNone(getattr(dag, "schedule", None))
        self.assertIn("catalog", dag.tags)
        self.assertEqual(
            {
                "get_grain_targets",
                "extract_catalog_to_gcs",
                "raw_grain_catalog",
                "dim_grain_metadata",
            },
            set(dag.task_ids),
        )


if __name__ == "__main__":
    unittest.main()
