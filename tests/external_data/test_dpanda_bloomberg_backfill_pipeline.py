from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = (
    REPO_ROOT
    / "dags"
    / "external_data"
    / "dpanda_bloomberg_backfill_pipeline.py"
)
DAGS_ROOT = REPO_ROOT / "dags"


def _load_backfill_module():
    if str(DAGS_ROOT) not in sys.path:
        sys.path.insert(0, str(DAGS_ROOT))
    spec = importlib.util.spec_from_file_location(
        "dpanda_bloomberg_backfill_dag",
        DAG_FILE,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class BackfillDagSourceTest(unittest.TestCase):
    def test_dag_is_manual_only(self):
        source = DAG_FILE.read_text()

        self.assertIn("schedule=None", source)
        self.assertIn("catchup=False", source)
        self.assertIn("max_active_runs=1", source)

    def test_dag_exposes_trigger_params(self):
        source = DAG_FILE.read_text()

        self.assertIn("Param(", source)
        self.assertIn('"category"', source)
        self.assertIn('"extract_start_date"', source)
        self.assertIn('"extract_end_date"', source)
        self.assertIn('"metric_names"', source)
        self.assertIn("enum=sorted(CATEGORIES)", source)

    def test_dag_takes_shared_pool_and_chunked_backfill(self):
        source = DAG_FILE.read_text()

        self.assertIn("@task(pool=POOL_NAME)", source)
        self.assertIn("run_backfill(", source)
        # No executor_config: per-year chunks are small, so no kubernetes
        # client dependency is pulled into DAG parsing.
        self.assertNotIn("executor_config", source)
        self.assertNotIn("pod_override", source)

    def test_dag_imports_with_expected_task_when_airflow_is_available(self):
        try:
            import airflow  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("Airflow is not installed in the local test environment")

        module = _load_backfill_module()
        # The backfill DAG is the module-level `with DAG(...) as dag` object.
        dag = module.dag

        self.assertEqual(dag.dag_id, module.BACKFILL_DAG_ID)
        self.assertIsNone(getattr(dag, "schedule", None))
        self.assertIn("backfill", dag.tags)
        self.assertEqual({"ingest_backfill"}, set(dag.task_ids))

        task = dag.get_task("ingest_backfill")
        self.assertEqual(task.pool, module.POOL_NAME)

        self.assertEqual(
            set(dag.params),
            {
                "category",
                "extract_start_date",
                "extract_end_date",
                "metric_names",
            },
        )

    def test_resolve_backfill_window_parses_and_validates(self):
        try:
            import airflow  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("Airflow is not installed in the local test environment")

        module = _load_backfill_module()
        resolve = module._resolve_backfill_window

        start, end = resolve({
            "extract_start_date": "2003-05-01",
            "extract_end_date": "2006-06-01",
        })
        self.assertEqual(start.to_date_string(), "2003-05-01")
        self.assertEqual(end.to_date_string(), "2006-06-01")
        self.assertEqual(start.offset, 0)
        self.assertEqual(end.offset, 0)

        with self.assertRaises(ValueError):
            resolve({"extract_start_date": "2003-05-01"})
        with self.assertRaises(ValueError):
            resolve({"extract_end_date": "2006-06-01"})
        with self.assertRaises(ValueError):
            resolve({
                "extract_start_date": "2006-06-01",
                "extract_end_date": "2006-06-01",
            })


if __name__ == "__main__":
    unittest.main()
