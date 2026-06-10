from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.grain_targets import parse_grain_targets


VALID_TARGET = {
    "dataset_id": "64a1f0c2e4b0a1b2c3d4e5f6",
    "grain_id": "SX5E_Index",
    "description": "euro stoxx 50 index daily ohlcv",
}


class GrainTargetsTest(TestCase):
    def test_parses_json_string_and_returns_normalized_targets(self):
        raw = json.dumps([VALID_TARGET])

        targets = parse_grain_targets(raw)

        self.assertEqual(targets, [VALID_TARGET])

    def test_accepts_already_deserialized_list(self):
        targets = parse_grain_targets([dict(VALID_TARGET, enabled=True)])

        self.assertEqual(targets, [VALID_TARGET])

    def test_skips_disabled_targets(self):
        second = dict(
            VALID_TARGET,
            grain_id="KOSIS007",
            dataset_id="64a1f0c2e4b0a1b2c3d4e5f7",
            enabled=False,
        )

        targets = parse_grain_targets([VALID_TARGET, second])

        self.assertEqual([t["grain_id"] for t in targets], ["SX5E_Index"])

    def test_rejects_invalid_json(self):
        with self.assertRaisesRegex(ValueError, "must be valid json"):
            parse_grain_targets("not json")

    def test_rejects_non_array_and_empty_array(self):
        with self.assertRaisesRegex(ValueError, "non-empty json array"):
            parse_grain_targets(json.dumps({"grain_id": "X"}))
        with self.assertRaisesRegex(ValueError, "non-empty json array"):
            parse_grain_targets("[]")

    def test_rejects_missing_grain_id(self):
        entry = dict(VALID_TARGET)
        del entry["grain_id"]
        with self.assertRaisesRegex(ValueError, "must set grain_id"):
            parse_grain_targets([entry])

    def test_rejects_duplicate_grain_id(self):
        with self.assertRaisesRegex(ValueError, "duplicate grain_id"):
            parse_grain_targets([VALID_TARGET, dict(VALID_TARGET)])

    def test_rejects_malformed_dataset_id(self):
        for bad in ("", "xyz", "64A1F0C2E4B0A1B2C3D4E5F6", "64a1f0c2"):
            entry = dict(VALID_TARGET, dataset_id=bad)
            with self.assertRaisesRegex(ValueError, "dataset_id"):
                parse_grain_targets([entry])

    def test_rejects_missing_description(self):
        entry = dict(VALID_TARGET, description="")
        with self.assertRaisesRegex(ValueError, "description"):
            parse_grain_targets([entry])

    def test_rejects_description_with_sql_literal_breakers(self):
        # The description is embedded as a SQL string literal in the dim
        # grains merge, so the characters the literal builder refuses must
        # fail at parse time, not mid pipeline.
        for bad in ("euro stoxx 50 'index'", "euro stoxx 50 \\ index"):
            entry = dict(VALID_TARGET, description=bad)
            with self.assertRaisesRegex(ValueError, "quotes or backslashes"):
                parse_grain_targets([entry])

    def test_rejects_non_boolean_enabled(self):
        entry = dict(VALID_TARGET, enabled="yes")
        with self.assertRaisesRegex(ValueError, "enabled"):
            parse_grain_targets([entry])

    def test_rejects_all_targets_disabled(self):
        entry = dict(VALID_TARGET, enabled=False)
        with self.assertRaisesRegex(ValueError, "at least one grain"):
            parse_grain_targets([entry])
