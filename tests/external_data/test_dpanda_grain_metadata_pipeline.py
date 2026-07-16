from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = (
    REPO_ROOT / "dags" / "external_data" / "dpanda_grain_metadata_pipeline.py"
)


class DpandaGrainMetadataPipelineDagTest(unittest.TestCase):
    def test_dag_uses_adc_clients_without_airflow_gcp_connection(self):
        dag_source = DAG_FILE.read_text()

        self.assertNotIn("GCSHook", dag_source)
        self.assertNotIn("BigQueryHook", dag_source)
        self.assertNotIn("gcp_conn_id", dag_source)
        self.assertIn("storage.Client()", dag_source)
        self.assertIn("bigquery.Client(", dag_source)

    def test_dag_runs_as_workload_identity_without_impersonation(self):
        dag_source = DAG_FILE.read_text()

        # Dev policy: tasks run as the single Airflow workload identity SA;
        # hooks must not impersonate task-scoped service accounts.
        self.assertNotIn("impersonation_chain", dag_source)

    def test_extraction_queries_catalog_by_nested_grain_id(self):
        dag_source = DAG_FILE.read_text()

        # Catalog documents key grain identity at process.grainId; the
        # catalog datasetId can diverge from the target dataset_id, so the
        # find must not filter on it.
        self.assertIn('"process.grainId": target["grain_id"]', dag_source)
        self.assertNotIn('"process.datasetId":', dag_source)
        self.assertIn("dpanda_mongo_catalog_collection_name", dag_source)
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
            "from airflow.exceptions import AirflowSkipException", dag_source
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
            "dpanda_grain_metadata_pipeline_dag",
            DAG_FILE,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        dag = module.dag
        self.assertEqual("external_data__dpanda_grain_metadata", dag.dag_id)
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
