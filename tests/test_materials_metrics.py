from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.materials_metrics import parse_materials_config


def _valid_config() -> dict:
    return {
        "datasource": {"name": "kosa_steeldata", "description": "한국철강협회 STEEL DATA"},
        "metrics": [
            {
                "match": {"국가": "일본", "품목명": "용해용철스크랩"},
                "measure_column": "국내수입 물량",
                "previous_year_column": "전년 물량",
                "name": "steel_scrap_import_from_japan_volume",
                "description": "용해용철스크랩 일본 국내수입 물량",
                "unit": "천톤",
            }
        ],
    }


class ParseMaterialsConfigTests(TestCase):
    def test_valid_config_applies_defaults(self):
        config = parse_materials_config(_valid_config(), "test")
        self.assertEqual(config["period_column"], "시점")
        self.assertEqual(config["period_format"], "%Y.%m")
        self.assertEqual(config["time_grain"], "M")
        self.assertEqual(config["datasource"]["name"], "kosa_steeldata")
        self.assertEqual(len(config["metrics"]), 1)
        self.assertEqual(config["metrics"][0]["previous_year_column"], "전년 물량")

    def test_optional_previous_year_defaults_none(self):
        raw = _valid_config()
        del raw["metrics"][0]["previous_year_column"]
        config = parse_materials_config(raw, "test")
        self.assertIsNone(config["metrics"][0]["previous_year_column"])

    def test_optional_unit_defaults_none(self):
        raw = _valid_config()
        del raw["metrics"][0]["unit"]
        config = parse_materials_config(raw, "test")
        self.assertIsNone(config["metrics"][0]["unit"])

    def test_rejects_missing_datasource(self):
        raw = _valid_config()
        del raw["datasource"]
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_empty_metrics(self):
        raw = _valid_config()
        raw["metrics"] = []
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_metric_name_with_spaces(self):
        raw = _valid_config()
        raw["metrics"][0]["name"] = "bad name"
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_duplicate_metric_names(self):
        raw = _valid_config()
        raw["metrics"].append(dict(raw["metrics"][0]))
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_sql_unsafe_values(self):
        raw = _valid_config()
        raw["metrics"][0]["description"] = "has ' quote"
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_unsafe_match_key(self):
        raw = _valid_config()
        raw["metrics"][0]["match"] = {'b"ad': "x"}
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_bad_period_format(self):
        raw = _valid_config()
        raw["period_format"] = "%Y;DROP"
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_non_json_object(self):
        with self.assertRaises(ValueError):
            parse_materials_config("[]", "test")
