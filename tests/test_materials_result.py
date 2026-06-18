from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.materials_result import (
    envelope_records,
    envelope_to_ndjson,
    records_to_ndjson,
)


def _success_envelope() -> dict:
    return {
        "status": "success",
        "recipe": "kosa.steel_scrap_import",
        "data": [
            {"시점": "2024.01", "국내수입 물량": "115"},
            {"시점": "2024.02", "국내수입 물량": "184"},
        ],
    }


class EnvelopeRecordsTests(TestCase):
    def test_success_returns_records(self):
        self.assertEqual(len(envelope_records(_success_envelope())), 2)

    def test_failed_returns_empty(self):
        env = _success_envelope()
        env["status"] = "failed"
        self.assertEqual(envelope_records(env), [])

    def test_null_data_returns_empty(self):
        env = {"status": "success", "data": None}
        self.assertEqual(envelope_records(env), [])

    def test_non_dict_record_raises(self):
        env = {"status": "success", "data": ["not-an-object"]}
        with self.assertRaises(ValueError):
            envelope_records(env)


class RecordsToNdjsonTests(TestCase):
    def test_wraps_each_record_under_row(self):
        ndjson = records_to_ndjson([{"시점": "2024.01", "국내수입 물량": "115"}])
        lines = ndjson.splitlines()
        self.assertEqual(len(lines), 1)
        parsed = json.loads(lines[0])
        self.assertEqual(list(parsed.keys()), ["row"])
        self.assertEqual(parsed["row"]["국내수입 물량"], "115")

    def test_preserves_korean_unescaped(self):
        ndjson = records_to_ndjson([{"품목명": "용해용철스크랩"}])
        self.assertIn("용해용철스크랩", ndjson)

    def test_empty_records_yield_empty_string(self):
        self.assertEqual(records_to_ndjson([]), "")

    def test_one_line_per_record(self):
        ndjson = envelope_to_ndjson(_success_envelope())
        self.assertEqual(len(ndjson.splitlines()), 2)
