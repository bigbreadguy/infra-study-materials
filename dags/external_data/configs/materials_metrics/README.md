# materials_metrics/

These JSON files are the source of truth for the `dl_materials` transform-load and
are synced to Airflow through the DAG bundle. One file per recipe, named
`<recipe>.json`.

Files are grouped into a per-datasource subdirectory named by the recipe's
datasource prefix (the part before the first dot): `dpanda_bloomberg/`, `eia/`,
`kosa/`, and `yfinance/`. Datasources without a subdirectory stay flat in this
directory (e.g. `kosis.manufacturing_operation.json`, `cssc.customs_trade.json`).
`config_path` in
`common/materials_metrics.py` resolves either layout — grouped first, flat fallback —
so a recipe is always looked up by its full `<recipe>.json` name regardless of where
it lives.

Three writers consume them, all through `common/materials_bigquery.py`:

- **External-data scrape** (`scrape_external_data_pipeline.py`): for each recipe in
  `SOURCES` (the `kosa.*`, `kosis.*`, `yfinance.*`, and `eia.*` sources), the
  transform-load step reads the matching `<recipe>.json` mapping and upserts the
  recipe's records into the `dl_materials` star schema. A recipe with no mapping file
  loads nothing (its transform-load step skips). The sources differ in period grain —
  `kosa.*` and `kosis.*` are monthly (`%Y.%m` / `%Y%m`), `yfinance.*` is daily (`date`
  / `%Y-%m-%d`, `time_grain "D"`), and `eia.*` is per-series weekly or monthly
  (`period` / `%Y-%m-%d` `W` or `%Y-%m` `M`) — and each metric's `match` selects its
  series by a categorical result field (`품목명`/`국가` for KOSA, `C1`/`ITM_ID` for
  KOSIS, `symbol` for yfinance, and `series`/`seriesId`/`msn` for EIA's
  PET/STEO/Total-Energy rows respectively), so a single-series result still names a
  non-empty `match`. EIA fetches the full series each run (no range), so there is no
  slice; the `fact_values` MERGE dedups the re-fetched overlap. EIA's Venezuela crude
  series is quarterly (`time_grain "Q"`): its `YYYY-Q#` period has no `PARSE_DATE`
  format, so the normalize step rewrites it to the quarter-start date
  (`Q1→YYYY-01-01 … Q4→YYYY-10-01`, `normalize_quarter_periods`) and the config parses
  it as `%Y-%m-%d`.
- **dpanda Bloomberg** (`dpanda_bloomberg_*_pipeline.py`): each category loads
  `dpanda_bloomberg.<category>.json` (one datasource, `dpanda_bloomberg`; each
  category is a `dim_categories` row; each grain×field is a metric). The extractor
  flattens every Mongo document into a `{"row": {...}}` record with `grain_id`,
  `logical_date`, and the document's `data` fields, so a metric's `match` selects a
  grain by `{"grain_id", "dataset_id"}` (the full grain identity — `grain_id`
  recurs across datasets) and its `measure_column` names a `data` field. This file
  is the **single source of truth for each grain's `description` and `time_grain`
  (its `freq`)**: `grain_targets_from_config` (in `common/materials_metrics.py`)
  collapses the metrics to one extraction target per `(dataset_id, grain_id)` pair
  to drive the Mongo pull, so the two must stay in one place — here.
- **File pipelines** (`estat_file_pipeline.py`, `annual_reports_pipeline.py`): sources
  published as downloadable files rather than a queryable API. The `dfml-scraper` Cloud
  Run Job parses the workbook off-pod into `{"row": <json>}` parquet, then the DAG runs
  `run_combined_materials_transform` over the source's configs (`estat_kakuho.*`;
  `iea_wei.*`). These yearly sources use `period_column "year"` /
  `period_format "%Y"` / `time_grain "Y"`, and each metric's `match` selects one series
  by the long-format dimension fields the parser emits (IEA WEI `region`/`metric`). IEA WEI
  blocks bots, so the workbook is an operator-uploaded GCS object the DAG passes to the job
  as `INPUT_URI`.

The DAGs own the BigQuery tables (Terraform owns only the `dl_materials` dataset):
the transform script opens with idempotent `CREATE TABLE IF NOT EXISTS`, so a fresh
dataset self-bootstraps and steady-state runs no-op.

## Config-declared dynamic recipes (`scrape` block)

For sources whose scraper exposes a **parameterized** recipe, a config file can
register itself as a new recipe, the same way the `dpanda_bloomberg` pipelines are
driven purely by these files. Seven sources are fully config-declared today:

| source | scraper recipe | identity params |
|---|---|---|
| `fred` | `fred.series` | `series_id` |
| `yfinance` | `yfinance.history` | `symbol`, optional `value_field` / `interval` |
| `eia` | `eia.series` | `series_id` |
| `cftc` | `cftc.contract` | `code` (contract market code) |
| `ember` | `ember.installed_capacity` | `is_aggregate_series`, optional `series` |
| `kosis` | `kosis.series` | `org_id` / `tbl_id` / `itm_id` / `obj_l1` |
| `gacc` | `gacc.commodity` | `commodity` / `keyword`, optional `hs6` / `aliases[]` |
| `shfe` | `shfe.futures_daily` / `shfe.weekly_stock` | `product_id` (the ISO `start`/`end` day window is added by the `shfe` period kind) |

Give the file a top-level `scrape` block and **no** `SOURCES` entry:

```json
{
  "scrape": {
    "recipe": "fred.series",
    "params": { "series_id": "DCOILWTICO" }
  },
  "datasource": { "name": "fred", "description": "Federal Reserve Economic Data (FRED)" },
  "period_column": "date",
  "period_format": "%Y-%m-%d",
  "time_grain": "D",
  "category": { "name": "energy_crude", "category_0": "energy", "category_1": "crude", "description": "에너지 - 원유" },
  "metrics": [
    {
      "match": { "series_id": "DCOILWTICO" },
      "measure_column": "value",
      "name": "wti_spot_price",
      "description": "WTI 현물 가격 (달러/배럴, 일별)",
      "unit": "USD/bbl"
    }
  ]
}
```

`scrape_external_data_pipeline.py` discovers such files at DAG parse time
(`discover_scrape_recipes`) and merges them into `SOURCES`: the **file stem** is the
recipe identity everywhere in the pipeline (trigger form, request/result/staging
objects, `floor_period`/`floor_date` clamp, normalize, transform-load), while the
request payload names the parameterized scraper recipe with `scrape.params` as its
query (plus the source's usual period window — `fred` is "full", `yfinance` is the
ISO `start_date`/`end_date` date kind). So **adding a FRED series or a Yahoo ticker
is one committed config file** — no scraper deploy, no DAG edit. Rules, enforced by
`parse_scrape_block` / CI:

- `scrape.recipe` must be `<target>.<flow>` and share the file's source prefix
  (`fred.…` files may only ride `fred.*` recipes).
- `scrape.params` is a flat object of JSON scalars; keys must bind to the scraper
  recipe's signature (`series_id`; `symbol` / `value_field` / `interval`), or the
  scrape fails loudly with `BAD_PARAMS`.
- The `metrics[].match` must select the rows the scraper stamps: FRED rows carry the
  request's `series_id`; yfinance rows carry `symbol`, with the close under
  `value_field` (default `value`).
- A stem may be registered **either** statically in `SOURCES` **or** via a `scrape`
  block, never both (parse fails loudly).
- For a yfinance instance, declare its `floor_date` (the symbol's earliest history)
  so backfills clamp correctly.

Each file maps result columns to curated star-schema metrics:

- `datasource` — the stable datasource `name`/`description` (`dim_datasources`).
- `period_column` / `period_format` — the source period field and its strptime
  format (KOSA default `시점` / `%Y.%m`; Bloomberg uses `logical_date` / `%Y-%m-%d`);
  parsed into `fact_values.logical_date`.
- `time_grain` — short token stamped on fact rows (default `M`); a metric may
  override it (e.g. KOSA daily `D`, quarterly `Q`). For grain-based (dpanda
  Bloomberg) metrics this is also the grain's extraction `freq`.
- `floor_period` (optional, `"YYYY-MM"`) — the recipe's earliest-available scrape
  month. KOSA hard-fails (by design) a request whose start precedes a metric's
  earliest published period, so `scrape_external_data_pipeline.py` reads this at
  request-build time and clamps the scrape `start` UP to it (logging at WARN when a
  clamp actually moves the start; skipping the recipe when the whole requested range
  is below the floor). It is per recipe and **not uniform** — `steel_scrap_domestic`
  reaches back to `1979-01`, ~20 years before the others (`2000-01`/`2001-01`).
  Floors are static; the upper bound (latest published month) is owned separately by
  the YTD over-fetch/slice (`common/materials_result.py`), so do **not** add a
  ceiling here. Recipes with no static floor (the dpanda Bloomberg recipes) omit it.
- `floor_date` (optional, `"YYYY-MM-DD"`) — the date-grain analog of `floor_period`,
  for daily recipes whose period is a `date` rather than a month tuple (e.g.
  `yfinance.fx_tryusd`, whose `TRYUSD=X` history starts `2015-01-01`).
  `scrape_external_data_pipeline.py` reads it at request-build time and clamps the
  ISO `start_date` UP to it (skipping the recipe when the whole requested range is
  below the floor). A recipe declares `floor_period` *or* `floor_date` per its period
  grain, never both.
- `category` — the `dim_categories` hierarchy (`name` slug + optional
  `category_0`/`category_1`/`category_2` + optional `description`). Set once at the
  recipe level and/or overridden per metric; every metric must resolve to exactly
  one category because `dim_metrics.category_id` is REQUIRED.
- `metrics[]` — for each curated metric: `match` (the row filter), the value
  source, the curated `name`/`unit`, an optional per-metric `category`, and an
  inline `description`/`time_grain` (`time_grain` falling back to the recipe
  default). The value source is one of:
  - `measure_column` — a single field (the KOSA shape), optionally with a
    `previous_year_column` split off as a prior-year row.
  - `measures` — a list that **expands one grain into several field-metrics**
    named `<name>_<suffix>` (the Bloomberg OHLCV shape: one entry yields
    `..._open`, `..._high`, `..._low`, `..._close`, `..._volume`,
    `..._openinterest`). Each measure is a column string, or an object
    `{ "column", "suffix"?, "unit"?, "description_suffix"? }` so volume/open-
    interest can carry their own unit. Set `measures` once at the **recipe level**
    as a default for every metric, and/or per metric; a metric with its own
    `measure_column` opts out (e.g. a monthly production grain measuring `value`).
- `measures` (recipe level, optional) — the default measure set applied to every
  metric that sets neither `measure_column` nor its own `measures`.
- `rank_expansion` (recipe level or per metric, optional) — fans every resulting
  leaf metric across a **per-period ordinal rank**, appending `_<rank>` as the
  **trailing** name suffix (so a `measures` grain becomes
  `<name>_<measure>_<rank>`). An object `{ "column", "count", "description_suffix"? }`:
  `count` copies are produced for ranks `0..count-1`, each adding `column = "<rank>"`
  to the metric's `match` so it selects exactly that rank's row per period, and
  `column` (default `contract_rank`) is a **staging-stamped** field, not a raw scraper
  column. This is the SHFE daily-futures shape — the pipeline's `stamp_contract_ranks`
  normalize step ranks each trading day's delivery-month contracts nearby→back
  (`contract_rank` `"0"`..), and `rank_expansion: {"column": "contract_rank", "count":
  13}` over the six OHLC/settle/volume/OI measures yields `shfe_al_settle_0 ..
  shfe_al_settle_12` (0 = nearby month). Ranks past `count` fall through the merge
  unmatched; not supported with `previous_year_column`, and `column` must not already
  be a `match` key.

The dpanda Bloomberg extractor normalizes source `data` keys to canonical
lowercase fields (`open/high/low/close/volume/openinterest/value`, collapsing
`Open`/`oi`/`openInterest` etc.), so `measure_column`/`measures` always reference
the canonical name regardless of source casing.

Every string is embedded into the transform-load SQL as a literal or a JSON path,
so values must not contain quotes or backslashes and metric/category `name`s must
be a SQL-safe identifier slug. The files are validated by
`tests/external_data/test_materials_metric_files.py`, and the schema validator
lives in `common/materials_metrics.py`.

> **`dpanda_bloomberg.*.json` still need human curation.** Each grain's entry is
> built from a live Mongo field inventory: each `match` carries the full
> `(grain_id, dataset_id)`, the value source reflects the grain's actual fields
> (OHLCV `measures` vs a single `value`), and each carries its own curated
> `description`/`time_grain`. What remains
> before enabling the Bloomberg → `dl_materials` load: rename every
> `PLACEHOLDER_<category>_<grain>` metric `name` to a curated slug, fill the real
> `unit`s (`PLACEHOLDER_unit`; price vs volume vs open-interest differ), and review
> the preserved curated price grains (e.g. `*_cash_offer`, `*_3m_offer`) that fall
> through to the recipe-level OHLCV `measures` default — those grains are `value`-only
> in Mongo, so confirm or give them an explicit `measure_column`.
