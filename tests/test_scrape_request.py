from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.scrape_request import (
    SCHEMA_VERSION,
    build_request_payload,
    gcs_uri,
    merge_params,
    request_object_name,
    resolve_year_month,
    result_object_name,
)


class ResolveYearMonthTests(TestCase):
    def test_no_lookback_returns_same(self):
        self.assertEqual(resolve_year_month(2001, 1, 0), (2001, 1))

    def test_lookback_rolls_over_year(self):
        # January minus 2 months -> previous November.
        self.assertEqual(resolve_year_month(2001, 1, 2), (2000, 11))

    def test_lookback_within_year(self):
        self.assertEqual(resolve_year_month(2026, 6, 3), (2026, 3))

    def test_rejects_bad_month(self):
        with self.assertRaises(ValueError):
            resolve_year_month(2026, 13, 0)

    def test_rejects_negative_lookback(self):
        with self.assertRaises(ValueError):
            resolve_year_month(2026, 6, -1)


class MergeParamsTests(TestCase):
    def test_conf_overrides_defaults(self):
        defaults = {"country": "일본", "country_code": "104"}
        merged = merge_params(defaults, {"country_code": "999"})
        self.assertEqual(merged["country_code"], "999")
        self.assertEqual(merged["country"], "일본")

    def test_none_conf_returns_copy(self):
        defaults = {"a": 1}
        merged = merge_params(defaults, None)
        self.assertEqual(merged, {"a": 1})
        self.assertIsNot(merged, defaults)


class BuildRequestPayloadTests(TestCase):
    def test_payload_shape(self):
        payload = build_request_payload("kosa.steel_scrap", {"year": 2001, "month": 1})
        self.assertEqual(payload["schema_version"], SCHEMA_VERSION)
        self.assertEqual(payload["recipe"], "kosa.steel_scrap")
        self.assertEqual(payload["params"]["year"], 2001)
        # output_uri is never embedded in the request.
        self.assertNotIn("output_uri", payload)

    def test_rejects_empty_recipe(self):
        with self.assertRaises(ValueError):
            build_request_payload("", {})


class ObjectPathTests(TestCase):
    def test_paths_keyed_by_run_id(self):
        self.assertEqual(
            request_object_name("run-1"), "scrape/requests/run-1.json"
        )
        self.assertEqual(
            result_object_name("run-1"), "scrape/results/run-1.json"
        )

    def test_gcs_uri(self):
        self.assertEqual(
            gcs_uri("dfml-dev-raw", "scrape/requests/run-1.json"),
            "gs://dfml-dev-raw/scrape/requests/run-1.json",
        )
