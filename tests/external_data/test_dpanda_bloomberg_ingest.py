from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pendulum

from external_data.common import dpanda_bloomberg_ingest as engine


REPO_ROOT = Path(__file__).resolve().parents[2]
ENGINE_FILE = (
    REPO_ROOT / "dags" / "external_data" / "common" / "dpanda_bloomberg_ingest.py"
)


def _bson_available() -> bool:
    try:
        import bson  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


class EngineHelpersTest(unittest.TestCase):
    def test_label_value_lowercases_sanitizes_and_truncates(self):
        run_id_label = engine._label_value("scheduled__2026-06-16T00:00:00+00:00")
        self.assertEqual(run_id_label, run_id_label.lower())
        self.assertNotIn(":", run_id_label)
        self.assertNotIn("+", run_id_label)
        self.assertRegex(run_id_label, r"^[a-z0-9_-]*$")
        self.assertEqual(engine._label_value("Copper/Total Prod"), "copper_total_prod")
        self.assertEqual(len(engine._label_value("x" * 100)), 63)

    def test_materials_recipe_namespaces_by_datasource(self):
        self.assertEqual(engine.materials_recipe("copper"), "dpanda_bloomberg.copper")

    def test_transform_labels_cover_run_identity(self):
        labels = engine._transform_labels({
            "dag_id": "external_data__dpanda_bloomberg__copper",
            "run_id": "scheduled__2026-06-16T00:00:00+00:00",
            "category": "copper",
        })
        self.assertEqual(set(labels), {"dag_id", "run_id", "category", "step"})
        self.assertEqual(labels["step"], "transform")
        for value in labels.values():
            self.assertRegex(value, r"^[a-z0-9_-]*$")
            self.assertLessEqual(len(value), 63)

    def test_transform_job_id_prefix_is_greppable_and_valid(self):
        prefix = engine._transform_job_id_prefix(
            {"dag_id": "d", "run_id": "r:1+2", "category": "copper"}
        )
        self.assertTrue(prefix.startswith("dpanda_copper_"))
        self.assertIn("transform", prefix)
        self.assertRegex(prefix, r"^[A-Za-z0-9_-]+$")

    def test_run_scoped_prefix_isolates_category_run_and_window(self):
        prefix = engine._run_scoped_prefix(
            "raw/dpanda",
            "copper",
            "scheduled__2026-06-15T00:00:00+00:00",
            pendulum.parse("2026-06-14", tz="UTC"),
        )
        self.assertEqual(
            prefix,
            "raw/dpanda/category=copper/run=scheduled__2026-06-15T00_00_00_00_00"
            "/window=20260614T000000",
        )
        # Two backfill chunks under one run id stay isolated by the window
        # segment, so one combined job never reads another chunk's objects.
        chunk_a = engine._run_scoped_prefix(
            "raw", "copper", "run", pendulum.parse("2003-01-01", tz="UTC")
        )
        chunk_b = engine._run_scoped_prefix(
            "raw", "copper", "run", pendulum.parse("2004-01-01", tz="UTC")
        )
        self.assertNotEqual(chunk_a, chunk_b)

    def test_build_gcs_object_name_keeps_grain_identity_and_date_path(self):
        name = engine._build_gcs_object_name(
            "raw/dpanda/category=copper/run=r/window=20030101T000000",
            pendulum.parse("2003-04-05T00:00:00", tz="UTC"),
            "0123456789abcdef01234567",
            "copper_cash_offer",
            "parquet",
        )
        self.assertIn("dataset_id=0123456789abcdef01234567", name)
        self.assertIn("grain_id=copper_cash_offer", name)
        self.assertIn("2003/04/05/00", name)
        self.assertTrue(name.endswith(".parquet"))

    def test_run_wildcard_uri_targets_the_run_prefix(self):
        uri = engine._run_wildcard_uri("bkt", "raw/category=copper/run=r/window=w")
        self.assertEqual(uri, "gs://bkt/raw/category=copper/run=r/window=w/*")

    def test_normalize_date_pins_yyyy_mm_dd(self):
        self.assertEqual(
            engine._normalize_date(datetime(2003, 4, 5, 23, tzinfo=timezone.utc)),
            "2003-04-05",
        )
        self.assertEqual(engine._normalize_date("2003-04-05"), "2003-04-05")
        self.assertEqual(engine._normalize_date("2003-04-05T12:00:00Z"), "2003-04-05")
        self.assertIsNone(engine._normalize_date(None))
        self.assertIsNone(engine._normalize_date(""))

    def test_backfill_window_chunks_splits_by_calendar_year(self):
        chunks = engine.backfill_window_chunks(
            pendulum.parse("2003-03-15", tz="UTC"),
            pendulum.parse("2006-01-01", tz="UTC"),
        )
        as_dates = [(s.to_date_string(), e.to_date_string()) for s, e in chunks]
        self.assertEqual(
            as_dates,
            [
                ("2003-03-15", "2004-01-01"),
                ("2004-01-01", "2005-01-01"),
                ("2005-01-01", "2006-01-01"),
            ],
        )
        within = engine.backfill_window_chunks(
            pendulum.parse("2024-06-01", tz="UTC"),
            pendulum.parse("2024-09-01", tz="UTC"),
        )
        self.assertEqual(len(within), 1)
        decade = engine.backfill_window_chunks(
            pendulum.parse("2016-01-01", tz="UTC"),
            pendulum.parse("2026-01-01", tz="UTC"),
        )
        self.assertEqual(len(decade), 10)

    def test_backfill_window_chunks_rejects_non_positive_range(self):
        with self.assertRaises(ValueError):
            engine.backfill_window_chunks(
                pendulum.parse("2026-01-01", tz="UTC"),
                pendulum.parse("2026-01-01", tz="UTC"),
            )

    def test_format_transform_summary_reports_statements(self):
        summary = engine._format_transform_summary(
            {
                "raw_locations": [{"grain_id": "g"}],
                "quiet_grains": ["q@d"],
                "total_documents": 31,
            },
            {
                "job_id": "job-1",
                "elapsed_s": 12.5,
                "statements": [
                    {"job_id": "c1", "statement_type": "ASSERT",
                     "dml_affected_rows": None},
                    {"job_id": "c2", "statement_type": "MERGE",
                     "dml_affected_rows": 217},
                ],
            },
        )
        self.assertIn("grains with data: 1", summary)
        self.assertIn("quiet grains: 1", summary)
        self.assertIn("raw docs: 31", summary)
        self.assertIn("job-1", summary)
        self.assertIn("MERGE", summary)
        self.assertIn("rows=217", summary)


class SurgicalFilterTest(unittest.TestCase):
    """Metric-wise filter narrows extraction grains to the named metrics."""

    def _config(self):
        return {
            "metrics": [
                {
                    "name": "Com_LME_Cu_Cash",
                    "match": {
                        "grain_id": "copper_cash_offer",
                        "dataset_id": "693fb256455eba93435b82ff",
                    },
                },
                {
                    "name": "Com_LME_Cu_3M",
                    "match": {
                        "grain_id": "copper_3m_offer",
                        "dataset_id": "693fb256455eba93435b82ff",
                    },
                },
                {
                    "name": "Com_Copper_Prod",
                    "match": {
                        "grain_id": "1_16_-1",
                        "dataset_id": "69328021455eba93435a27c3",
                    },
                },
                # OHLCV-expanded field metrics share one grain; a column-matched
                # KOSA-shaped metric (no grain identity) names no grain.
                {
                    "name": "Com_SHFE_Cu_open",
                    "match": {
                        "grain_id": "VC",
                        "dataset_id": "68c90e7b5ca53d5d938cdd98",
                    },
                },
                {"name": "ColumnMatched", "match": {"category": "x"}},
            ]
        }

    def _targets(self):
        return [
            {"dataset_id": "693fb256455eba93435b82ff",
             "grain_id": "copper_cash_offer", "description": "d", "freq": "D"},
            {"dataset_id": "693fb256455eba93435b82ff",
             "grain_id": "copper_3m_offer", "description": "d", "freq": "D"},
            {"dataset_id": "69328021455eba93435a27c3",
             "grain_id": "1_16_-1", "description": "d", "freq": "M"},
            {"dataset_id": "68c90e7b5ca53d5d938cdd98",
             "grain_id": "VC", "description": "d", "freq": "W"},
        ]

    def test_metric_grain_index_skips_column_matched_metrics(self):
        index = engine._metric_grain_index(self._config())
        self.assertEqual(
            index["Com_LME_Cu_Cash"],
            ("693fb256455eba93435b82ff", "copper_cash_offer"),
        )
        self.assertNotIn("ColumnMatched", index)

    def test_select_keeps_only_named_metric_grains(self):
        selected = engine._select_targets_for_metrics(
            self._targets(),
            self._config(),
            ["Com_LME_Cu_Cash", "Com_LME_Cu_3M"],
            "copper",
        )
        self.assertEqual(
            {t["grain_id"] for t in selected},
            {"copper_cash_offer", "copper_3m_offer"},
        )

    def test_select_dedupes_names_sharing_one_grain(self):
        # Two field metrics on the same grain collapse to a single extraction
        # target, mirroring the OHLCV expansion.
        selected = engine._select_targets_for_metrics(
            self._targets(), self._config(), ["Com_SHFE_Cu_open"], "copper"
        )
        self.assertEqual([t["grain_id"] for t in selected], ["VC"])

    def test_select_rejects_unknown_metric_name(self):
        with self.assertRaises(ValueError) as ctx:
            engine._select_targets_for_metrics(
                self._targets(), self._config(), ["Nope_Not_Here"], "copper"
            )
        self.assertIn("Nope_Not_Here", str(ctx.exception))

    def test_select_ignores_blank_entries(self):
        selected = engine._select_targets_for_metrics(
            self._targets(), self._config(), ["", "  ", "Com_LME_Cu_Cash"], "copper"
        )
        self.assertEqual([t["grain_id"] for t in selected], ["copper_cash_offer"])


class EngineRowShapeTest(unittest.TestCase):
    """The extractor reshapes each Mongo doc into a dl_materials ``row`` record."""

    def _docs_and_target(self):
        target = {
            "grain_id": "copper_x",
            "dataset_id": "693fb256455eba93435b82ff",
            "description": "LME copper",
            "freq": "D",
        }
        docs = [
            {
                "_id": {"$oid": "a" * 24},
                "datasetId": {"$oid": target["dataset_id"]},
                "ts": datetime(2003, 4, 5, tzinfo=timezone.utc),
                "grainId": "copper_x",
                "data": {"dt": "2003-04-05", "open": "1", "close": "2"},
            },
            {
                "_id": {"$oid": "b" * 24},
                "datasetId": {"$oid": target["dataset_id"]},
                "ts": datetime(2003, 4, 6, tzinfo=timezone.utc),
                "grainId": "copper_x",
                "data": {"value": "9"},
            },
        ]
        return docs, target

    def test_materials_row_flattens_data_and_pins_identity_and_date(self):
        docs, target = self._docs_and_target()
        row = engine._materials_row(docs[0], target)
        self.assertEqual(row["grain_id"], "copper_x")
        self.assertEqual(row["dataset_id"], target["dataset_id"])
        self.assertEqual(row["logical_date"], "2003-04-05")
        # data fields flatten to the top level so a metric measure_column names one.
        self.assertEqual(row["open"], "1")
        self.assertEqual(row["close"], "2")

    def test_materials_row_falls_back_to_ts_for_date(self):
        docs, target = self._docs_and_target()
        # Second doc has no data date field, so the document ts drives the date.
        row = engine._materials_row(docs[1], target)
        self.assertEqual(row["logical_date"], "2003-04-06")
        self.assertEqual(row["value"], "9")

    def test_materials_row_normalizes_ohlcv_aliases_to_canonical(self):
        target = {"grain_id": "g", "dataset_id": "693fb256455eba93435b82ff"}
        doc = {
            "grainId": "g",
            "ts": datetime(2026, 6, 18, tzinfo=timezone.utc),
            "data": {"Open": "1", "High": "2", "low": "0.5", "Close": "1.5",
                     "Volume": "100", "oi": "50"},
        }
        row = engine._materials_row(doc, target)
        self.assertEqual(row["open"], "1")
        self.assertEqual(row["high"], "2")
        self.assertEqual(row["low"], "0.5")
        self.assertEqual(row["close"], "1.5")
        self.assertEqual(row["volume"], "100")
        self.assertEqual(row["openinterest"], "50")

    def test_materials_row_treats_naive_ts_as_utc(self):
        # pymongo returns naive (UTC) datetimes by default; the date must not be
        # shifted by the pod's local offset.
        target = {"grain_id": "g", "dataset_id": "693fb256455eba93435b82ff"}
        doc = {"grainId": "g", "ts": datetime(2026, 6, 18, 1, 0, 0), "data": {"close": "1"}}
        self.assertEqual(engine._materials_row(doc, target)["logical_date"], "2026-06-18")

    def test_ndjson_serialization_wraps_each_row(self):
        if not _bson_available():
            self.skipTest("bson (pymongo) is not installed locally")
        docs, target = self._docs_and_target()
        payload, sample = engine._serialize_grain(docs, target, "ndjson")
        lines = [line for line in payload.splitlines() if line]
        self.assertEqual(len(lines), 2)
        first = json.loads(lines[0])
        self.assertIn("row", first)
        self.assertEqual(first["row"]["grain_id"], "copper_x")
        self.assertEqual(first["row"]["logical_date"], "2003-04-05")
        self.assertTrue(sample)

    def test_parquet_serialization_single_row_string_column(self):
        if not _bson_available():
            self.skipTest("bson (pymongo) is not installed locally")
        import io
        import pyarrow.parquet as pq

        docs, target = self._docs_and_target()
        payload, _ = engine._serialize_grain(docs, target, "parquet")
        table = pq.read_table(io.BytesIO(payload))
        self.assertEqual(table.column_names, ["row"])
        self.assertTrue(all(str(f.type) == "string" for f in table.schema))
        rows = [json.loads(text) for text in table.to_pydict()["row"]]
        self.assertEqual(rows[0]["grain_id"], "copper_x")
        self.assertEqual(rows[0]["close"], "2")


class EngineSourceTest(unittest.TestCase):
    def test_engine_reaches_gcp_through_adc_clients(self):
        source = ENGINE_FILE.read_text()

        # ADC/workload identity, no Airflow GCP connection.
        self.assertIn("storage.Client()", source)
        self.assertIn("bigquery.Client(", source)
        self.assertNotIn("gcp_conn_id", source)
        self.assertNotIn("GCSHook", source)
        self.assertNotIn("BigQueryHook", source)

    def test_transform_runs_as_one_materials_job(self):
        source = ENGINE_FILE.read_text()

        # Bloomberg now feeds the shared dl_materials transform.
        self.assertIn("run_materials_transform(", source)
        self.assertNotIn("run_combined_transform(", source)
        # Per-grain transform chain is gone; extraction keeps isolation.
        self.assertNotIn("_run_grain_transform", source)
        self.assertIn("for target in targets:", source)
        self.assertIn("::group::", source)
        self.assertIn('POOL_NAME = "dpanda_bloomberg"', source)


if __name__ == "__main__":
    unittest.main()
