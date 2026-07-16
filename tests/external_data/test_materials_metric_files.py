from __future__ import annotations

import unittest

from external_data.common.materials_metrics import (
    CONFIG_DIR,
    load_materials_config,
    load_recipe_date_floor,
    load_recipe_floor,
)
from external_data.common.dpanda_bloomberg_ingest import (
    CATEGORIES as BLOOMBERG_CATEGORIES,
    materials_recipe,
)
from external_data.scrape_external_data_pipeline import SOURCES
from external_data.estat_file_pipeline import ESTAT_KAKUHO_CONFIGS
from external_data.cochilco_grades_pipeline import COCHILCO_GRADES_CONFIGS
from external_data.annual_reports_pipeline import SOURCE_CONFIGS as ANNUAL_REPORTS_CONFIGS


def _registered_recipes() -> set[str]:
    # KOSA recipes are registered in SOURCES; the dpanda Bloomberg pipelines load
    # one materials recipe per category, so those are registered too. The e-Stat Final
    # Report and Cochilco grades file metrics are not in SOURCES (they ride their own
    # file-mode DAGs, not the scrape flow), so register their configs from those DAGs.
    # The annual-reports sources (IEA WEI) ride the manual annual_reports_pipeline DAG,
    # so register their configs from that DAG's SOURCE_CONFIGS too.
    recipes = {recipe for recipes in SOURCES.values() for recipe in recipes}
    recipes.update(materials_recipe(category) for category in BLOOMBERG_CATEGORIES)
    recipes.update(ESTAT_KAKUHO_CONFIGS)
    recipes.update(COCHILCO_GRADES_CONFIGS)
    recipes.update(
        stem for stems in ANNUAL_REPORTS_CONFIGS.values() for stem in stems
    )
    return recipes


class MaterialsMetricFilesTest(unittest.TestCase):
    def _config_files(self):
        # Config files are grouped into per-datasource subdirectories (kosa/, eia/,
        # dpanda_bloomberg/) with a few datasources left flat, so recurse.
        return sorted(CONFIG_DIR.rglob("*.json"))

    def test_each_config_file_passes_validation(self):
        files = self._config_files()
        self.assertTrue(files, f"expected at least one config file in {CONFIG_DIR}")
        for config_file in files:
            with self.subTest(config_file=config_file.name):
                # Validate through the production entry point: every metric carries
                # its own description and time_grain inline (time_grain falling back
                # to the recipe default).
                config = load_materials_config(config_file.stem)
                self.assertIsNotNone(config)
                self.assertGreater(len(config["metrics"]), 0)

    def test_every_kosa_config_declares_a_scrape_floor(self):
        # KOSA hard-fails a below-floor scrape start, so every kosa.* recipe must
        # carry a static floor for the request builder to clamp to. load_recipe_floor
        # is the production accessor the DAG uses at request-build time.
        kosa_files = [f for f in self._config_files() if f.stem.startswith("kosa.")]
        self.assertTrue(kosa_files, "expected kosa.* config files")
        for config_file in kosa_files:
            with self.subTest(config_file=config_file.name):
                floor = load_recipe_floor(config_file.stem)
                self.assertIsNotNone(
                    floor, f"{config_file.name} must declare a floor_period"
                )
                year, month = floor
                self.assertGreaterEqual(year, 1900)
                self.assertIn(month, range(1, 13))

    def test_load_recipe_floor_is_none_for_unknown_recipe(self):
        self.assertIsNone(load_recipe_floor("kosa.does_not_exist"))

    def test_cftc_configs_expand_nine_cot_measures_per_commodity(self):
        # Each CFTC recipe is single-commodity but exposes nine COT position
        # aggregates from one row, so its one config metric must expand (via the
        # recipe-level ``measures``) into nine field-metrics -- one per measure column.
        # The measure columns include the schema quirk that total-reportable long is
        # ``tot_rept_positions_long_all`` (has _all) but short is
        # ``tot_rept_positions_short`` (no _all).
        expected_columns = {
            "noncomm_positions_long_all": "noncomm_long",
            "noncomm_positions_short_all": "noncomm_short",
            "comm_positions_long_all": "comm_long",
            "comm_positions_short_all": "comm_short",
            "tot_rept_positions_long_all": "tot_rept_long",
            "tot_rept_positions_short": "tot_rept_short",
            "nonrept_positions_long_all": "nonrept_long",
            "nonrept_positions_short_all": "nonrept_short",
            "open_interest_all": "open_interest",
        }
        cftc_files = [f for f in self._config_files() if f.stem.startswith("cftc.")]
        self.assertEqual(len(cftc_files), 5, "expected five cftc.* config files")
        for config_file in cftc_files:
            with self.subTest(config_file=config_file.name):
                config = load_materials_config(config_file.stem)
                # Weekly grain, report-date column parsed as a bare date (the pipeline
                # strips the ISO timestamp before load).
                self.assertEqual(config["time_grain"], "W")
                self.assertEqual(
                    config["period_column"], "report_date_as_yyyy_mm_dd"
                )
                self.assertEqual(config["period_format"], "%Y-%m-%d")

                metrics = config["metrics"]
                self.assertEqual(len(metrics), len(expected_columns))
                by_column = {m["measure_column"]: m for m in metrics}
                self.assertEqual(set(by_column), set(expected_columns))
                # Every expanded metric keeps the recipe's single commodity match and
                # is suffixed by its measure so names stay unique per datasource.
                for column, suffix in expected_columns.items():
                    metric = by_column[column]
                    self.assertTrue(metric["name"].endswith(f"_{suffix}"))
                    self.assertEqual(list(metric["match"]), ["commodity"])
                    self.assertEqual(metric["time_grain"], "W")

                # Each cftc recipe declares a per-commodity history floor (used by the
                # request builder to clamp a below-floor backfill start).
                self.assertIsNotNone(
                    load_recipe_date_floor(config_file.stem),
                    f"{config_file.name} must declare a floor_date",
                )

    def test_shfe_futures_daily_expands_to_thirteen_ranks_per_measure(self):
        # The SHFE daily-futures reshape: seven measures (open/high/low/close/settle/
        # volume/open_interest) x thirteen month-delta contract ranks = 91 metrics named
        # ``shfe_al_<measure>_<rank>`` (rank TRAILING, 0 = the spot/delta-0 month), each
        # pinned to its delta on the staging-stamped ``contract_rank`` field plus the
        # product match.
        config = load_materials_config("shfe.futures_daily")
        self.assertEqual(config["time_grain"], "D")
        self.assertEqual(config["period_column"], "date")
        self.assertEqual(config["period_format"], "%Y-%m-%d")
        self.assertIsNotNone(
            load_recipe_date_floor("shfe.futures_daily"),
            "shfe.futures_daily must declare a floor_date",
        )

        measures = ("open", "high", "low", "close", "settle", "volume", "open_interest")
        expected_names = {
            f"shfe_al_{measure}_{rank}"
            for measure in measures
            for rank in range(13)
        }
        by_name = {m["name"]: m for m in config["metrics"]}
        self.assertEqual(set(by_name), expected_names)
        self.assertEqual(len(config["metrics"]), 91)

        for measure in measures:
            for rank in range(13):
                metric = by_name[f"shfe_al_{measure}_{rank}"]
                self.assertEqual(metric["measure_column"], measure)
                self.assertEqual(
                    metric["match"],
                    {"product_id": "al", "contract_rank": str(rank)},
                )
                self.assertEqual(metric["time_grain"], "D")

    def test_shfe_weekly_stock_selects_the_grand_total_row(self):
        # The SHFE weekly warehouse-stock report: the scraper emits every (region,
        # warehouse) row plus the exchange's subtotal/total aggregates tagged by
        # ``row_type``, and picking the aggregate is the config's job -- the metric
        # must pin the grand-total row (``row_type=total`` AND ``warehouse=Total``,
        # the labels both the .dat and html eras normalize to; ``row_type`` alone
        # would also catch the bonded/tax-included totals). Two measures off that one
        # row: ``stock`` (현물/Delivery-able) and ``futures`` (창고증권/On Warrant).
        config = load_materials_config("shfe.weekly_stock")
        self.assertEqual(config["time_grain"], "W")
        self.assertEqual(config["period_column"], "date")
        self.assertEqual(config["period_format"], "%Y-%m-%d")
        self.assertEqual(
            load_recipe_date_floor("shfe.weekly_stock"),
            "2014-05-23",
            "shfe.weekly_stock must declare the report's 2014-05-23 floor_date",
        )

        expected_columns = {
            "stock": "shfe_al_stock",
            "futures": "shfe_al_warrant_stock",
        }
        by_name = {m["name"]: m for m in config["metrics"]}
        self.assertEqual(set(by_name), set(expected_columns.values()))
        for column, name in expected_columns.items():
            metric = by_name[name]
            self.assertEqual(metric["measure_column"], column)
            self.assertEqual(
                metric["match"],
                {"product_id": "al", "row_type": "total", "warehouse": "Total"},
            )
            self.assertEqual(metric["time_grain"], "W")

    def test_every_config_file_maps_a_registered_recipe(self):
        # A recipe may exist in SOURCES without a config (load is opt-in), but a
        # committed config whose stem is not a registered recipe is dead weight or
        # a typo: it would never be loaded.
        registered = _registered_recipes()
        for config_file in self._config_files():
            with self.subTest(config_file=config_file.name):
                self.assertIn(
                    config_file.stem,
                    registered,
                    f"{config_file.name} does not match any recipe in SOURCES",
                )


class DynamicScrapeRecipeRegistrationTest(unittest.TestCase):
    # Config-declared dynamic recipes: a config file with a top-level "scrape" block
    # registers itself into SOURCES at DAG parse time (the dpanda_bloomberg model).
    # These checks pin the invariants the merge relies on, against whatever dynamic
    # configs are committed at any point in time.

    def test_committed_scrape_blocks_validate(self):
        # Every committed scrape block must parse, and its recipe must belong to the
        # file's own source prefix (the DAG's per-source credential env and the
        # scraper target line up on it).
        import json

        from external_data.common.materials_metrics import parse_scrape_block

        for config_file in sorted(CONFIG_DIR.rglob("*.json")):
            with self.subTest(config_file=config_file.name):
                parsed = json.loads(config_file.read_text(encoding="utf-8"))
                scrape = parse_scrape_block(parsed, config_file.name)
                if scrape is None:
                    continue
                source = config_file.stem.split(".", 1)[0]
                self.assertEqual(
                    scrape["recipe"].split(".", 1)[0],
                    source,
                    f"{config_file.name} scrape recipe must belong to {source!r}",
                )

    def test_dynamic_instances_are_registered_in_sources(self):
        # Whatever discovery found must have landed in SOURCES under its source,
        # carrying the scrape params as the default query -- i.e. a dynamic instance
        # is indistinguishable from a static recipe everywhere downstream.
        from external_data.scrape_external_data_pipeline import DYNAMIC_RECIPE_KEYS

        for instance, scraper_recipe in DYNAMIC_RECIPE_KEYS.items():
            with self.subTest(instance=instance):
                source = instance.split(".", 1)[0]
                self.assertIn(source, SOURCES)
                self.assertIn(instance, SOURCES[source])
                self.assertEqual(scraper_recipe.split(".", 1)[0], source)

    # Sources whose scraper stamps the scrape param NAME onto every row (fred's
    # series_id, yfinance's symbol, gacc's commodity), so each metric's match must
    # share that key with scrape.params. The other dynamic sources (eia/cftc/
    # kosis/ember) match on NATIVE row columns whose names differ from the param
    # (seriesId/msn, commodity vs code, C1/ITM_ID, series absent for the ember
    # roll-up), so only the value-agreement-on-intersection check applies there.
    _PARAM_STAMPED_SOURCES = ("fred", "yfinance", "gacc")

    def test_scrape_params_agree_with_metric_match(self):
        # The identity-drift guard the migration was for: the series the scraper is
        # ASKED for (scrape.params) and the rows the load SELECTS (metrics[].match)
        # live in the same file and must agree on every shared key -- a mismatch
        # means every scraped row is silently dropped at the match predicate.
        import json

        from external_data.common.materials_metrics import (
            load_materials_config,
            parse_scrape_block,
        )

        for config_file in sorted(CONFIG_DIR.rglob("*.json")):
            parsed = json.loads(config_file.read_text(encoding="utf-8"))
            scrape = parse_scrape_block(parsed, config_file.name)
            if scrape is None:
                continue
            source = config_file.stem.split(".", 1)[0]
            config = load_materials_config(config_file.stem)
            for metric in config["metrics"]:
                shared = set(metric["match"]) & set(scrape["params"])
                if source in self._PARAM_STAMPED_SOURCES:
                    self.assertTrue(
                        shared,
                        f"{config_file.name} metric {metric['name']} matches on "
                        f"none of the scrape params -- its rows could never "
                        f"self-identify",
                    )
                for key in shared:
                    with self.subTest(
                        config_file=config_file.name, metric=metric["name"], key=key
                    ):
                        self.assertEqual(
                            str(scrape["params"][key]),
                            metric["match"][key],
                            f"{config_file.name}: scrape.params.{key} and "
                            f"metrics[].match.{key} disagree",
                        )

    def test_yfinance_scrape_value_field_matches_measure_column(self):
        # yfinance.history emits the Close under scrape.params.value_field (default
        # "value"); the metric's measure_column must read that same column or the
        # load finds no value.
        import json

        from external_data.common.materials_metrics import (
            load_materials_config,
            parse_scrape_block,
        )

        yfinance_files = [
            f for f in sorted(CONFIG_DIR.rglob("*.json"))
            if f.stem.startswith("yfinance.")
        ]
        self.assertTrue(yfinance_files, "expected yfinance.* config files")
        for config_file in yfinance_files:
            parsed = json.loads(config_file.read_text(encoding="utf-8"))
            scrape = parse_scrape_block(parsed, config_file.name)
            if scrape is None:
                continue
            value_field = scrape["params"].get("value_field", "value")
            config = load_materials_config(config_file.stem)
            for metric in config["metrics"]:
                with self.subTest(
                    config_file=config_file.name, metric=metric["name"]
                ):
                    self.assertEqual(
                        metric["measure_column"],
                        value_field,
                        f"{config_file.name}: measure_column must read the "
                        f"scrape value_field {value_field!r}",
                    )

    # Sources that migrated off static SOURCES entries entirely -- their whole
    # recipe surface is config-declared.
    _FULLY_DYNAMIC_SOURCES = (
        "fred", "yfinance", "eia", "cftc", "ember", "kosis", "gacc", "shfe",
    )

    def test_dynamic_sources_are_fully_config_declared(self):
        # These sources have no static SOURCES entries: every committed config of
        # theirs must self-register via a scrape block (a config without one would
        # silently stop being scraped, since there is no static entry to fall back
        # to), and their SOURCES dicts must stay empty (a static re-add would be a
        # duplicate-registration parse failure waiting to happen).
        from external_data.scrape_external_data_pipeline import (
            DYNAMIC_RECIPE_KEYS,
            SOURCES as MERGED_SOURCES,
        )

        for source in self._FULLY_DYNAMIC_SOURCES:
            static_keys = [
                key for key in MERGED_SOURCES[source]
                if key not in DYNAMIC_RECIPE_KEYS
            ]
            self.assertEqual(
                static_keys, [],
                f"{source} must stay fully config-declared (no static entries)",
            )

        stems = [
            f.stem for f in sorted(CONFIG_DIR.rglob("*.json"))
            if f.stem.split(".", 1)[0] in self._FULLY_DYNAMIC_SOURCES
        ]
        self.assertTrue(stems, "expected dynamic-source config files")
        for stem in stems:
            with self.subTest(stem=stem):
                self.assertIn(
                    stem,
                    DYNAMIC_RECIPE_KEYS,
                    f"{stem}.json must declare a scrape block (its source has "
                    "no static SOURCES entries)",
                )
