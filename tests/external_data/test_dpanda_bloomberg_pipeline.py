from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = REPO_ROOT / "dags" / "external_data" / "dpanda_bloomberg_pipeline.py"
DAGS_ROOT = REPO_ROOT / "dags"


def _load_pipeline_module():
    """Import the scheduled DAG module (needs Airflow for airflow.sdk)."""
    if str(DAGS_ROOT) not in sys.path:
        sys.path.insert(0, str(DAGS_ROOT))
    spec = importlib.util.spec_from_file_location(
        "dpanda_bloomberg_pipeline_dag",
        DAG_FILE,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class DpandaBloombergScheduledDagTest(unittest.TestCase):
    def test_dag_has_no_gcp_hooks_or_impersonation(self):
        dag_source = DAG_FILE.read_text()

        # The GKE cluster defines no Airflow GCP connection; the engine reaches
        # GCP through google.cloud clients (ADC/workload identity), so the DAG
        # file carries no hooks, conn ids, or impersonation.
        self.assertNotIn("DataformHook", dag_source)
        self.assertNotIn("GCSHook", dag_source)
        self.assertNotIn("BigQueryHook", dag_source)
        self.assertNotIn("gcp_conn_id", dag_source)
        self.assertNotIn("impersonation_chain", dag_source)

    def test_dag_delegates_to_the_shared_ingest_engine(self):
        dag_source = DAG_FILE.read_text()

        # Extract + single-job transform + observability live in the shared
        # engine; the DAG file only wires the schedule and the D-1 window.
        self.assertIn(
            "from external_data.common.dpanda_bloomberg_ingest import",
            dag_source,
        )
        self.assertIn("ingest(", dag_source)
        self.assertIn('raw_format="ndjson"', dag_source)
        self.assertIn("window=_resolve_interval(context)", dag_source)

    def test_dag_serializes_runs_and_extracts_full_utc_day(self):
        dag_source = DAG_FILE.read_text()

        self.assertIn("max_active_runs=1", dag_source)
        self.assertIn('"retries": 2', dag_source)
        # Cron trigger timetables derive the data interval from the trigger
        # wall clock, so the logical date drives the extraction window and
        # expands to the full utc day, stepping back one day (D-1).
        self.assertIn(
            'context["logical_date"] or context["data_interval_start"]',
            dag_source,
        )
        self.assertIn('in_timezone("UTC").start_of("day")', dag_source)
        self.assertIn("start_date.add(days=1)", dag_source)
        self.assertIn('start_date=datetime(1968, 1, 2, tz="UTC")', dag_source)

    def test_dag_schedules_daily(self):
        from external_data.common.dpanda_bloomberg_ingest import CATEGORIES

        dag_source = DAG_FILE.read_text()

        # The schedule comes from the engine's category registry.
        self.assertIn('schedule=settings["schedule"]', dag_source)
        self.assertTrue(
            all(settings["schedule"] == "@daily" for settings in CATEGORIES.values())
        )

    def test_dag_runs_one_pod_per_run_as_a_single_task(self):
        dag_source = DAG_FILE.read_text()

        # One task per DAG run means the KubernetesExecutor runs one pod per
        # run: no mapped task group, no expand.
        self.assertNotIn("@task_group", dag_source)
        self.assertNotIn(".expand(", dag_source)
        self.assertIn("def ingest_scheduled()", dag_source)

    def test_task_takes_the_shared_pool(self):
        dag_source = DAG_FILE.read_text()

        # The shared size-1 pool serializes writes against the backfill DAG.
        self.assertIn("@task(pool=POOL_NAME)", dag_source)

    def test_dag_has_no_conf_driven_backfill(self):
        dag_source = DAG_FILE.read_text()

        # Manual backfill moved to its own DAG; the scheduled file is pure D-1.
        self.assertNotIn("_resolve_manual_window", dag_source)
        self.assertNotIn("dag_run.conf", dag_source)
        self.assertNotIn("extract_start_date", dag_source)
        self.assertNotIn("extract_end_date", dag_source)

    def test_dag_imports_with_expected_task_when_airflow_is_available(self):
        try:
            import airflow  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("Airflow is not installed in the local test environment")

        module = _load_pipeline_module()

        expected_task_ids = {"ingest_scheduled"}

        self.assertFalse(hasattr(module, "dag"))
        for category, settings in module.CATEGORIES.items():
            with self.subTest(category=category):
                dag = getattr(
                    module,
                    f"external_data__dpanda_bloomberg__{category}",
                )

                self.assertEqual(
                    dag.dag_id,
                    f"external_data__dpanda_bloomberg__{category}",
                )
                self.assertEqual(
                    settings["schedule"],
                    getattr(dag, "schedule", None),
                )
                self.assertIn(category, dag.tags)
                self.assertEqual(expected_task_ids, set(dag.task_ids))
                # Every scheduled task shares the single dpanda pool.
                task = dag.get_task("ingest_scheduled")
                self.assertEqual(task.pool, module.POOL_NAME)


if __name__ == "__main__":
    unittest.main()
