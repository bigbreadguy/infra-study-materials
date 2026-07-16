from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import TestCase, mock

from external_data.common import materials_metrics
from external_data.common.materials_metrics import (
    load_recipe_date_floor,
    parse_floor_date,
    parse_materials_config,
    parse_scrape_block,
)


_CATEGORY = {
    "name": "metal_heavy_melting_steel_scrap",
    "category_0": "metal",
    "category_1": "steel",
    "category_2": "heavy_melting_scrap",
    "description": "금속 - 용해용철스크랩",
}


def _valid_config() -> dict:
    return {
        "datasource": {"name": "kosa_steeldata", "description": "한국철강협회 STEEL DATA"},
        "category": dict(_CATEGORY),
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

    def test_recipe_category_applies_to_every_metric(self):
        config = parse_materials_config(_valid_config(), "test")
        self.assertEqual(len(config["categories"]), 1)
        self.assertEqual(config["categories"][0]["name"], _CATEGORY["name"])
        self.assertEqual(config["categories"][0]["category_1"], "steel")
        self.assertEqual(config["metrics"][0]["category"], _CATEGORY["name"])

    def test_metric_time_grain_defaults_to_recipe(self):
        config = parse_materials_config(_valid_config(), "test")
        self.assertEqual(config["metrics"][0]["time_grain"], "M")

    def test_floor_absent_is_none(self):
        config = parse_materials_config(_valid_config(), "test")
        self.assertIsNone(config["floor"])

    def test_floor_parsed_to_year_month_tuple(self):
        raw = _valid_config()
        raw["floor_period"] = "1979-01"
        config = parse_materials_config(raw, "test")
        self.assertEqual(config["floor"], (1979, 1))

    def test_floor_rejects_malformed_period(self):
        for bad in ("1979.01", "1979-1", "1979-13", "1979-00", "abc", "197901"):
            with self.subTest(bad=bad):
                raw = _valid_config()
                raw["floor_period"] = bad
                with self.assertRaises(ValueError):
                    parse_materials_config(raw, "test")

    def test_metric_overrides_time_grain_and_category(self):
        raw = _valid_config()
        raw["time_grain"] = "M"
        raw["metrics"][0]["time_grain"] = "D"
        raw["metrics"][0]["category"] = {
            "name": "metal_copper",
            "category_0": "metal",
            "category_1": "copper",
        }
        config = parse_materials_config(raw, "test")
        self.assertEqual(config["metrics"][0]["time_grain"], "D")
        self.assertEqual(config["metrics"][0]["category"], "metal_copper")
        self.assertEqual({c["name"] for c in config["categories"]}, {"metal_copper"})

    def test_measures_expand_one_grain_into_field_metrics(self):
        raw = _valid_config()
        raw["metrics"] = [{
            "match": {"grain_id": "copper_cash_offer"},
            "name": "copper_lme_cash_offer",
            "description": "LME copper cash",
            "unit": "USD",
            "time_grain": "D",
            "measures": [
                "open", "close",
                {"column": "volume", "unit": "lots"},
            ],
        }]
        config = parse_materials_config(raw, "test")
        by_name = {m["name"]: m for m in config["metrics"]}
        self.assertEqual(
            set(by_name),
            {"copper_lme_cash_offer_open", "copper_lme_cash_offer_close",
             "copper_lme_cash_offer_volume"},
        )
        self.assertEqual(by_name["copper_lme_cash_offer_open"]["measure_column"], "open")
        self.assertEqual(by_name["copper_lme_cash_offer_open"]["unit"], "USD")
        # per-measure unit overrides the base unit
        self.assertEqual(by_name["copper_lme_cash_offer_volume"]["unit"], "lots")
        self.assertEqual(by_name["copper_lme_cash_offer_open"]["time_grain"], "D")

    def test_recipe_level_measures_default_applies_and_is_overridable(self):
        raw = _valid_config()
        raw["measures"] = ["open", "close"]
        raw["metrics"] = [
            {
                "match": {"grain_id": "g1"},
                "name": "price_grain",
                "description": "price",
            },
            {
                "match": {"grain_id": "g2"},
                "name": "prod_grain",
                "description": "production",
                "measure_column": "value",
            },
        ]
        config = parse_materials_config(raw, "test")
        names = {m["name"] for m in config["metrics"]}
        # price_grain expands via the default; prod_grain opts out with measure_column
        self.assertEqual(
            names, {"price_grain_open", "price_grain_close", "prod_grain"}
        )

    def test_rejects_both_measure_column_and_measures(self):
        raw = _valid_config()
        raw["metrics"][0]["measures"] = ["open"]
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rank_expansion_fans_measures_into_trailing_rank_metrics(self):
        # The SHFE daily-futures shape: one metric x measures x rank_expansion ->
        # <name>_<measure>_<rank>, rank TRAILING (0 = the spot/delta-0 month).
        raw = _valid_config()
        raw["measures"] = ["open", "settle"]
        raw["rank_expansion"] = {"column": "contract_rank", "count": 3}
        raw["metrics"] = [{
            "match": {"product_id": "al"},
            "name": "shfe_al",
            "description": "SHFE aluminium",
            "unit": "CNY/t",
            "time_grain": "D",
        }]
        config = parse_materials_config(raw, "test")
        by_name = {m["name"]: m for m in config["metrics"]}
        # 2 measures x 3 ranks = 6 leaf metrics, rank as the trailing suffix.
        self.assertEqual(
            set(by_name),
            {
                "shfe_al_open_0", "shfe_al_open_1", "shfe_al_open_2",
                "shfe_al_settle_0", "shfe_al_settle_1", "shfe_al_settle_2",
            },
        )
        front = by_name["shfe_al_settle_0"]
        self.assertEqual(front["measure_column"], "settle")
        # Each leaf pins its rank on the rank column, keeping the base match too.
        self.assertEqual(front["match"], {"product_id": "al", "contract_rank": "0"})
        self.assertEqual(
            by_name["shfe_al_settle_2"]["match"],
            {"product_id": "al", "contract_rank": "2"},
        )
        self.assertEqual(front["time_grain"], "D")

    def test_rank_expansion_on_single_measure_column(self):
        raw = _valid_config()
        raw["rank_expansion"] = {"column": "contract_rank", "count": 2}
        raw["metrics"] = [{
            "match": {"product_id": "al"},
            "name": "shfe_al_settle",
            "description": "SHFE aluminium settle",
            "measure_column": "settle",
        }]
        config = parse_materials_config(raw, "test")
        by_name = {m["name"]: m for m in config["metrics"]}
        self.assertEqual(set(by_name), {"shfe_al_settle_0", "shfe_al_settle_1"})
        self.assertEqual(by_name["shfe_al_settle_1"]["measure_column"], "settle")
        self.assertEqual(
            by_name["shfe_al_settle_1"]["match"],
            {"product_id": "al", "contract_rank": "1"},
        )

    def test_metric_level_rank_expansion_overrides_default(self):
        raw = _valid_config()
        raw["rank_expansion"] = {"column": "contract_rank", "count": 5}
        raw["metrics"] = [{
            "match": {"product_id": "al"},
            "name": "shfe_al_settle",
            "description": "SHFE aluminium settle",
            "measure_column": "settle",
            "rank_expansion": {"column": "contract_rank", "count": 2},
        }]
        config = parse_materials_config(raw, "test")
        self.assertEqual(len(config["metrics"]), 2)  # metric override wins over default 5

    def test_rank_expansion_rejects_previous_year_column(self):
        raw = _valid_config()
        raw["metrics"][0]["rank_expansion"] = {"column": "contract_rank", "count": 2}
        # _valid_config's metric carries previous_year_column -> nonsensical combo.
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rank_expansion_rejects_column_colliding_with_match(self):
        raw = _valid_config()
        del raw["metrics"][0]["previous_year_column"]
        raw["metrics"][0]["rank_expansion"] = {"column": "국가", "count": 2}
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rank_expansion_rejects_bad_count(self):
        for bad in (0, -1, 61, "3", 2.5, True):
            with self.subTest(bad=bad):
                raw = _valid_config()
                del raw["metrics"][0]["previous_year_column"]
                raw["metrics"][0]["rank_expansion"] = {
                    "column": "contract_rank", "count": bad
                }
                with self.assertRaises(ValueError):
                    parse_materials_config(raw, "test")

    def test_rejects_metric_without_any_category(self):
        raw = _valid_config()
        del raw["category"]
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_conflicting_category_hierarchy(self):
        raw = _valid_config()
        del raw["category"]
        raw["metrics"] = [
            {
                **raw["metrics"][0],
                "name": "a",
                "category": {"name": "x", "category_0": "metal"},
            },
            {
                **raw["metrics"][0],
                "name": "b",
                "category": {"name": "x", "category_0": "macro"},
            },
        ]
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

    def test_rejects_category_name_with_spaces(self):
        raw = _valid_config()
        raw["category"]["name"] = "bad name"
        with self.assertRaises(ValueError):
            parse_materials_config(raw, "test")

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


class ParseFloorDateTests(TestCase):
    def test_parses_and_strips(self):
        self.assertEqual(parse_floor_date("  2015-01-01  ", "test"), "2015-01-01")

    def test_rejects_malformed(self):
        for bad in ("2015-1-1", "2015/01/01", "20150101", "2015-13-01",
                    "2015-00-01", "2015-01-32", "2015-01", "abc", 20150101):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    parse_floor_date(bad, "test")


class LoadRecipeDateFloorTests(TestCase):
    # Exercises the production accessor against committed config files, mirroring
    # load_recipe_floor's coverage in test_materials_metric_files.py.
    def test_reads_floor_date_for_date_grain_recipe(self):
        self.assertEqual(
            load_recipe_date_floor("yfinance.fx_tryusd"), "2015-01-01"
        )

    def test_none_for_month_grain_recipe_without_floor_date(self):
        # A kosa recipe declares floor_period (month), not floor_date.
        self.assertIsNone(load_recipe_date_floor("kosa.steel_scrap_import"))

    def test_none_for_unknown_recipe(self):
        self.assertIsNone(load_recipe_date_floor("yfinance.does_not_exist"))


class ParseScrapeBlockTests(TestCase):
    # The scrape block turns a config file into a self-contained recipe registration
    # riding one of the scraper's parameterized recipes (fred.series,
    # yfinance.history) -- the dpanda_bloomberg "config file is the registration"
    # model applied to the scrape flow.

    def test_absent_is_none(self):
        self.assertIsNone(parse_scrape_block({}, "test"))
        self.assertIsNone(parse_scrape_block({"scrape": None}, "test"))
        self.assertIsNone(parse_scrape_block({"scrape": {}}, "test"))

    def test_parses_recipe_and_params(self):
        block = parse_scrape_block(
            {"scrape": {"recipe": "fred.series", "params": {"series_id": "DCOILWTICO"}}},
            "test",
        )
        self.assertEqual(
            block, {"recipe": "fred.series", "params": {"series_id": "DCOILWTICO"}}
        )

    def test_params_default_to_empty(self):
        block = parse_scrape_block({"scrape": {"recipe": "fred.series"}}, "test")
        self.assertEqual(block, {"recipe": "fred.series", "params": {}})

    def test_params_accept_json_scalars(self):
        block = parse_scrape_block(
            {
                "scrape": {
                    "recipe": "yfinance.history",
                    "params": {
                        "symbol": "GC=F",
                        "value_field": "value",
                        "interval": "1d",
                        "flag": True,
                        "count": 3,
                    },
                }
            },
            "test",
        )
        self.assertEqual(block["params"]["symbol"], "GC=F")
        self.assertIs(block["params"]["flag"], True)

    def test_rejects_non_object_scrape(self):
        with self.assertRaises(ValueError):
            parse_scrape_block({"scrape": "fred.series"}, "test")

    def test_rejects_unknown_keys(self):
        with self.assertRaises(ValueError):
            parse_scrape_block(
                {"scrape": {"recipe": "fred.series", "extra": 1}}, "test"
            )

    def test_rejects_malformed_recipe_key(self):
        for bad in (None, "", "fredseries", "fred.", ".series", "Fred.Series",
                    "fred.series.extra", "fred series"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    parse_scrape_block({"scrape": {"recipe": bad}}, "test")

    def test_rejects_non_object_params(self):
        with self.assertRaises(ValueError):
            parse_scrape_block(
                {"scrape": {"recipe": "fred.series", "params": ["x"]}}, "test"
            )

    def test_rejects_non_slug_param_key(self):
        with self.assertRaises(ValueError):
            parse_scrape_block(
                {"scrape": {"recipe": "fred.series", "params": {"a b": "x"}}}, "test"
            )

    def test_rejects_nested_param_value(self):
        with self.assertRaises(ValueError):
            parse_scrape_block(
                {"scrape": {"recipe": "fred.series", "params": {"q": {"n": 1}}}},
                "test",
            )


class DiscoverScrapeRecipesTests(TestCase):
    # Discovery scans a source's committed config files for scrape blocks; the file
    # stem is the instance identity the whole pipeline keys on. Exercised against a
    # temp CONFIG_DIR so the tests are independent of which dynamic configs are
    # committed at any point in time.

    def _write(self, root, relpath, payload):
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _discover(self, root, source):
        with mock.patch.object(materials_metrics, "CONFIG_DIR", root):
            return materials_metrics.discover_scrape_recipes(source)

    def test_empty_when_source_has_no_configs(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(self._discover(Path(tmp), "fred"), {})

    def test_skips_files_without_scrape_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write(root, "fred/fred.us_building_permits.json", {"metrics": []})
            self.assertEqual(self._discover(root, "fred"), {})

    def test_discovers_grouped_and_flat_instances(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write(
                root,
                "fred/fred.wti_spot.json",
                {"scrape": {"recipe": "fred.series", "params": {"series_id": "DCOILWTICO"}}},
            )
            self._write(
                root,
                "fred.brent_spot.json",
                {"scrape": {"recipe": "fred.series", "params": {"series_id": "DCOILBRENTEU"}}},
            )
            self.assertEqual(
                self._discover(root, "fred"),
                {
                    "fred.wti_spot": {
                        "recipe": "fred.series",
                        "params": {"series_id": "DCOILWTICO"},
                    },
                    "fred.brent_spot": {
                        "recipe": "fred.series",
                        "params": {"series_id": "DCOILBRENTEU"},
                    },
                },
            )

    def test_grouped_file_shadows_flat_twin(self):
        # config_path prefers the grouped layout; discovery must agree so the
        # instance's scrape params and its transform-load mapping come from the
        # same file.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write(
                root,
                "fred/fred.wti_spot.json",
                {"scrape": {"recipe": "fred.series", "params": {"series_id": "GROUPED"}}},
            )
            self._write(
                root,
                "fred.wti_spot.json",
                {"scrape": {"recipe": "fred.series", "params": {"series_id": "FLAT"}}},
            )
            discovered = self._discover(root, "fred")
            self.assertEqual(
                discovered["fred.wti_spot"]["params"]["series_id"], "GROUPED"
            )

    def test_scopes_to_the_requested_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write(
                root,
                "yfinance/yfinance.gold_futures.json",
                {"scrape": {"recipe": "yfinance.history", "params": {"symbol": "GC=F"}}},
            )
            self.assertEqual(self._discover(root, "fred"), {})
            self.assertIn(
                "yfinance.gold_futures", self._discover(root, "yfinance")
            )

    def test_rejects_cross_source_recipe(self):
        # The scrape recipe must belong to the instance's source, or the DAG's
        # per-source credential env and the scraper target would disagree.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write(
                root,
                "fred/fred.gold_futures.json",
                {"scrape": {"recipe": "yfinance.history", "params": {"symbol": "GC=F"}}},
            )
            with self.assertRaises(ValueError):
                self._discover(root, "fred")

    def test_invalid_json_fails_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "fred" / "fred.broken.json"
            path.parent.mkdir(parents=True)
            path.write_text("{not json", encoding="utf-8")
            with self.assertRaises(ValueError):
                self._discover(root, "fred")


class ScrapeParamListTests(TestCase):
    # gacc's aliases is the one list-valued dynamic param: one level of scalars is
    # allowed, nesting is not.

    def test_accepts_flat_list_of_scalars(self):
        block = parse_scrape_block(
            {
                "scrape": {
                    "recipe": "gacc.commodity",
                    "params": {
                        "commodity": "natural_gas_liquefied",
                        "keyword": "natural gases in liquefied state",
                        "aliases": ["liquefied natural gas"],
                    },
                }
            },
            "test",
        )
        self.assertEqual(block["params"]["aliases"], ["liquefied natural gas"])

    def test_rejects_nested_list(self):
        with self.assertRaises(ValueError):
            parse_scrape_block(
                {
                    "scrape": {
                        "recipe": "gacc.commodity",
                        "params": {"aliases": [["nested"]]},
                    }
                },
                "test",
            )

    def test_rejects_object_inside_list(self):
        with self.assertRaises(ValueError):
            parse_scrape_block(
                {
                    "scrape": {
                        "recipe": "gacc.commodity",
                        "params": {"aliases": [{"label": "x"}]},
                    }
                },
                "test",
            )
