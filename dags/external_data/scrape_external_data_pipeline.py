"""Trigger the generic scraper Cloud Run Job for every external-data scrape recipe.

One DAG run fans out across all recipes registered in SOURCES below, grouped by
source (the scraper's ``Target``: one source == one origin + credential set). Fourteen
sources are wired today: **kosa** (Playwright browser recipes), **kosis** (KOSIS
OpenAPI HTTP/JSON), **yfinance** (the yfinance SDK, daily FX), **eia** (the EIA v2
API, full-series HTTP/JSON; api key from an Airflow Variable), **fred** (the FRED
API, daily rate/index series HTTP/JSON; api key from an Airflow Variable),
**estat** (the e-Stat API, Japan petroleum monthly DB series HTTP/JSON; appId from an
Airflow Variable), **cssc** (the China Stainless Steel Council customs-trade
articles, crawled then parsed by Gemini via Vertex AI; a daily publish-date check
that parses the day's article when one is published), **us_census** (the US
Census Bureau International Trade API, full-series monthly HTTP/JSON; api key from an
Airflow Variable), **ember** (the Ember Energy API, full-series monthly
installed-capacity HTTP/JSON; api key from an Airflow Variable), **worldstainless**
(the worldstainless.org meltshop-production data page, a static HTML table scraped and
parsed without a browser; no credential), and **gacc** (the China Customs / 海关总署
English monthly bulletin's Major Import Commodities table, static HTML parsed without a
browser; no credential), and **cftc** (the CFTC Commitments-of-Traders Legacy
Futures-Only report via the public Socrata reporting API, weekly trader-positioning
rows; no credential), and **petronet** (Korea National Oil Corporation's PETRONET daily
international crude oil price table -- Dubai / Brent(ICE) / WTI(NYMEX) / Oman -- served as
a static HTML table by its Excel-export endpoint, scraped without a browser; no
credential), and **gscpi** (the FRBNY Global Supply Chain Pressure Index monthly
composite, a downloadable NY Fed ``.xls`` workbook fetched over plain HTTP and parsed
without a browser -- a full-history series like FRED; no credential), and **eurostat**
(the European Commission's Eurostat SDMX 3.0 dissemination API -- monthly industrial
production, both working-day-adjusted (CA) and seasonally+working-day-adjusted (SCA)
series; public CSV, no credential; NOT the Japan ``estat`` source), and **abs** (the
Australian Bureau of Statistics SDMX 2.1 Data API, monthly merchandise-export values by
SITC commodity; public CSV, no credential), and **statcan** (Statistics Canada's CIMT
service, monthly merchandise trade at 8-digit HS granularity -- the only StatCan API that
reaches an HS-8 commodity, since WDS/CODR stops at NAPCS/HS-sections; public JSON, no
credential), and **lme** (the London Metal Exchange's trading-data API and weekly MiFID
Commitments-of-Traders workbooks -- daily official Bid/Offer prices, daily opening
stocks/warrants, weekly COT positioning; no credential, but the whole zone sits behind a
Cloudflare managed challenge only a headed Chromium passes, which the scraper drives
itself). Each source is
four steps -- build_requests -> execute -> normalize ->
load_bq -- and three of them (build, normalize, load) are ONE pod per source: pure I/O
orchestration collapsed per AGENTS.md so an empty/config-less/unselected recipe is an
in-process no-op rather than a pod that only rehydrates to self-skip. build renders
every selected recipe's request in a single pod and returns the LIST of built recipes;
that list is exactly the work ``execute`` runs over. Only ``execute`` -- the genuinely
heavy, externally-retryable scrape -- stays fanned out, and even it materializes a pod
only per recipe build actually built: an UNPACED source dynamic-maps the scrape over
the built list (``.expand_kwargs`` -> one parallel pod per built recipe, so a failure
or retry of one scrape does not block the others), while a PACED source (kosa, yfinance)
collapses its scrapes into one serial pod. A new source is added by appending a key to
SOURCES (it becomes its own TaskGroup automatically).

Sources differ in two axes the DAG branches on, keyed off the source name:

- **Period convention** (``SOURCE_PERIOD_KIND``): kosa and kosis take a four-int
  ``[start..end]`` MONTH range and return one record per month; yfinance is a DAILY
  series and takes an ISO ``start_date``/``end_date`` range instead (the addendum
  deliberately breaks the month convention, since its business key is per trading
  day). ``_build_params`` dispatches to the matching builder.
- **Scrape pacing** (``SOURCE_SCRAPE_DELAY_SECONDS``): a paced source collapses its
  scrapes into a single ``run_scraper_serial`` pod that loops the built recipes with an
  inter-scrape gap. Two sources are paced: kosa (its origin webpage blocks request
  bursts, so it can't parallelize) and yfinance (its ~30 tiny daily-FX SDK scrapes are
  pure I/O the pod only orchestrates -- fanning them out spent ~30 pods on the
  pod-start-up-dominates anti-pattern, and 30 concurrent pods risk Yahoo's burst limit --
  so they collapse to one serial pod with a short gap). Unpaced sources (kosis HTTP API,
  eia, ...) dynamic-map ``run_scraper`` over the built list, so their scrapes run as
  independent parallel pods, one per built recipe.

Per-recipe flow: render a {schema_version, recipe, params} request -> write it to
GCS (keyed by run_id AND recipe so recipes of one run don't collide) -> run the
scraper Cloud Run Job with REQUEST_URI / OUTPUT_URI as per-execution env overrides
-> gate on the job's exit (no result-existence sensor) -> normalize the result to
wrapped NDJSON staged under a run+recipe key (all of a source's recipes normalized in
one pod). A yearly-chunked source (shfe; SOURCE_YEARLY_CHUNKED) splits a multi-year
backfill window into per-calendar-year requests at build time -- one Cloud Run
execution per year, so a decades-deep backfill stays under the job's 1h task timeout
-- and normalize concatenates the chunk results back into the recipe's single staging
object. The transform-load then collapses across recipes: ONE combined BigQuery job
per source merges every staged recipe of that source at once, reading the run's
per-source staging wildcard
through a job-scoped external table. This is the fix for BigQuery's per-table
update rate limit -- a per-recipe load fired one job per recipe (EIA alone is ~50),
each MERGEing the 4 shared dl_materials tables, so a daily run hammered those
tables with scores of concurrent MERGEs; one job per source touches them once per
source per run instead.

Each step logs greppable ``[scrape-metrics]`` counts inside a collapsible log
group so a run's data collection is inspectable from the task log alone: normalize
reports ``scraped -> staged`` (with the kosa over-fetch ``dropped``) and the
per-source load reports ``staged -> merged`` (the fact_values MERGE's combined
inserts+updates summed over the source's recipes, the "XXXX rows" to cross-check
against fact_values in the BQ console) plus a per-statement DML breakdown. The same
counts ride out on each task's XCom.

Execution model (per AGENTS.md): this repo runs on the KubernetesExecutor, so one
task instance == one pod and GCP access goes through google.cloud clients
resolving Application Default Credentials (the Airflow workload identity service
account), never gcp_conn_id hooks. The scrape itself is genuinely heavy, external,
and independently retryable per recipe, so it is the documented exception to "one pod
per DAG" and stays fanned out -- but it fans out via dynamic mapping over the BUILT
recipes (not a static per-recipe task), so an unselected/floor-skipped recipe never
becomes a pod that only rehydrates to self-skip and clog the KubernetesExecutor pool.
build, normalize and load, by contrast, are pure I/O orchestration, so each IS
collapsed to one task per source (per AGENTS.md's "rebuild observability inside the
single task": per-source log group, per-recipe failure isolation, summed counts,
per-statement DML report). The two benign normalize no-ops (no config, no rows in
range) and the unselected-recipe case thus cost zero pods.

Config comes from Airflow Variables (namespaced scraper_ / materials_; provision
from the deepfl-infra scraper service outputs). Query params come from the per-recipe
defaults below, overridable per run via the trigger form / conf. The request period
follows the source's convention (above): month-grain recipes take a start..end month
range and date-grain recipes take an ISO start_date..end_date range.

The DAG runs daily as a monitor and reads its runtime inputs from a trigger form
(Airflow ``Param``s, unified with ``dag_run.conf`` into ``context["params"]``). A
*scheduled* run uses the form defaults -- the monitor window: month-grain recipes
pull a width-``monitor_window_months`` window ending at the current month (default 2
-> ``[prev .. current]``), date-grain recipes pull the last ``monitor_window_days``
days through yesterday. Sources that have nothing new in the requested window skip
rather than fail (KOSA over-fetch -> slice -> empty; KOSIS no-data -> exit 0). A
*manual* run uses the form to name an explicit ``start_date``/``end_date`` range
(targeted re-pull / backfill) and/or multi-select ``recipes`` (unselected ones skip).
Narrow by source via raw conf ``{"sources": [...]}`` too.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, TaskGroup, Variable, get_current_context, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery, storage
from pendulum import datetime

try:
    from airflow.sdk import Param
except ImportError:  # pragma: no cover - import location varies across 3.x
    from airflow.models.param import Param

from external_data.common.scrape_request import (
    build_request_payload,
    clamp_date_range_to_ceiling,
    clamp_date_range_to_floor,
    clamp_period_to_floor,
    gcs_uri,
    merge_params,
    month_window_range,
    recent_days_range_iso,
    request_object_name,
    result_object_name,
    split_date_range_yearly,
    staging_object_name,
    staging_source_wildcard,
    year_month_from_iso,
)
from external_data.common.gcs_object import upload_replacing_object
from external_data.common.materials_metrics import (
    discover_scrape_recipes,
    load_materials_config,
    load_recipe_date_floor,
    load_recipe_floor,
)
from external_data.common.materials_result import (
    derive_lme_cot_summary,
    envelope_records,
    normalize_estat_periods,
    normalize_iso_timestamp_periods,
    normalize_quarter_periods,
    records_to_ndjson,
    rename_estat_keys,
    slice_records_to_requested_range,
    stamp_contract_ranks,
)
from external_data.common.materials_bigquery import run_combined_materials_transform
from external_data.common.scraper_cloud_run import (
    SCRAPE_EXECUTION_TIMEOUT,
    SERIAL_SCRAPE_EXECUTION_TIMEOUT,
    execute_scraper_job,
)


logger = logging.getLogger(__name__)


# Registry of what to scrape, grouped by source. A "source" is the scraper's
# Target: one origin webpage + credential set, and the prefix of every recipe key
# it owns ("<source>.<flow>"). Each recipe maps to its default *query* params --
# the filter selections specific to that recipe -- overridable per run via
# dag_run.conf. The start..end month range is shared across all recipes and
# resolved separately (see _resolve_period), so it is never listed here.
#
# Add a source -> add a key here and it becomes its own TaskGroup. Add a recipe ->
# add an entry under its source (and a configs/materials_metrics/<recipe>.json to
# load it). Recipes that take only the month range carry {}.
#
# For sources whose scraper exposes a PARAMETERIZED recipe (fred.series,
# yfinance.history), skip the entry here entirely: give the config file a top-level
# "scrape" block instead and it self-registers at parse time (see the discovery
# merge below SOURCES).
SOURCES: dict[str, dict[str, dict[str, Any]]] = {
    "kosa": {
        "kosa.steel_scrap_import": {
            "country": "일본",
            "country_code": "104",
            "item": "용해용철스크랩",
            "item_code": "691",
        },
        "kosa.steel_scrap_domestic": {},
        "kosa.long_products_production": {},
        "kosa.eaf_steel_production": {},
    },
    # KOSIS OpenAPI (HTTP/JSON). Fully CONFIG-DECLARED: every instance is a
    # configs/materials_metrics/kosis.*.json file whose "scrape" block rides the
    # scraper's parameterized ``kosis.series`` recipe (org_id/tbl_id/itm_id/obj_l1 as
    # params; the shared four-int month range is added by this source's "month"
    # period kind). Add a series by committing one config file.
    "kosis": {},
    # yfinance SDK (daily FX and any Yahoo ticker). Fully CONFIG-DECLARED: every
    # instance is a configs/materials_metrics/yfinance/*.json file whose "scrape"
    # block rides the scraper's parameterized ``yfinance.history`` recipe (symbol +
    # value_field as params; the date range is added by this source's "date" period
    # kind). The series identity rides on every row as ``symbol`` (the config
    # matches on it), so a single-series result still names a non-empty ``match``.
    # XXXUSD majors/EMs plus the bare USD-base symbols (KRW=X, JPY=X, ...) and two
    # non-USD crosses (EURKRW, KRWCNY); Yahoo doesn't carry CNYKRW=X so KRWCNY=X is
    # substituted (invert for CNY/KRW). A dead symbol returns an empty frame -> the
    # scraper's clean exit 1. Add a pair/ticker by committing one config file.
    "yfinance": {},
    # EIA v2 API (HTTP/JSON). Fully CONFIG-DECLARED: every instance is a
    # configs/materials_metrics/eia/*.json file whose "scrape" block rides the
    # scraper's parameterized ``eia.series`` recipe (series_id as its param). The
    # seriesid endpoint returns the whole series in one call (all under EIA's
    # 5000-row page), so EIA uses the "full" period kind -> no range params; the
    # request fetches the full series each run and the fact_values MERGE dedups (the
    # over-fetch model). The api key is injected as an env override from an Airflow
    # Variable (see SOURCE_SECRET_ENV), not Secret Manager. Mixed native frequency
    # (W/M/Q) lives in each recipe's materials config, not here. ~56 instances: US
    # weekly petroleum supply/demand/inventory (+ their native .M companions), days
    # of supply, STEO international production/consumption/inventory, natural gas /
    # weather / price series. Add a series by committing one config file.
    "eia": {},
    # FRED API (HTTP/JSON, st. louis fed). Fully CONFIG-DECLARED: every instance is
    # a configs/materials_metrics/fred/*.json file whose "scrape" block rides the
    # scraper's parameterized ``fred.series`` recipe (series_id as its param). The
    # observations endpoint returns the whole series in one call (one page well under
    # FRED's 100000-row limit), so FRED uses the "full" period kind like EIA -- no
    # range params; the request fetches the full series each run and the fact_values
    # MERGE dedups the re-fetched overlap (the over-fetch model). The api key is
    # injected as an env override from an Airflow Variable (see SOURCE_SECRET_ENV),
    # not Secret Manager. FRED observation rows carry no series column, so the
    # scraper stamps each row's series_id and the materials config matches on it.
    # Add a series by committing one config file.
    "fred": {},
    # e-Stat API (Japan official statistics, HTTP/JSON). Each recipe targets one DB table
    # of the Petroleum Products Supply-Demand Dynamics Statistics survey, filtered to a
    # single product/area via baked category codes. getStatsData returns the full monthly
    # series in one call, so estat uses the "full" period kind like EIA/FRED -- no range
    # params ({}); the fact_values MERGE dedups the re-fetched overlap. The appId is
    # injected as an env override from an Airflow Variable (see SOURCE_SECRET_ENV). Rows
    # carry a YYYY00MMMM @time code rewritten to YYYY-MM at staging (normalize_estat_periods).
    #
    # AUTHORITATIVE SOURCE: the file-based Petroleum Statistics Final Report carries the
    # accurate, finalized figures and is the source we trust/use for these metrics -- it is
    # loaded separately under the "estat_kakuho" datasource by the estat_file_pipeline DAG
    # (XLS -> parquet -> BigQuery). This DB (supply-demand dynamics) source is the more
    # timely preliminary view kept for freshness/comparison; both land in fact_values
    # under distinct datasources (dim_metrics keys by datasource+name), so consumers should
    # prefer the estat_kakuho metrics.
    "estat": {
        "estat.crude_oil_imports": {},
        "estat.gasoline_consumption": {},
        "estat.kerosene_imports": {},
        "estat.lpg_consumption": {},
    },
    # US Census International Trade API (HTTP/JSON, array-of-arrays). The recipe bakes
    # one HS commodity + the total-for-all-countries roll-up; the timeseries endpoint
    # returns the full monthly series in one call (the recipe defaults the mandatory
    # ``time`` predicate to the series floor when no range is sent), so us_census uses
    # the same "full" period kind as EIA/FRED/estat -- no range params ({}); the
    # fact_values MERGE dedups the re-fetched overlap across runs. The api key is
    # injected as an env override from an Airflow Variable (see SOURCE_SECRET_ENV).
    "us_census": {
        "us_census.copper_scrap_exports": {},
    },
    # Ember Energy API (HTTP/JSON, top-level data list). Fully CONFIG-DECLARED:
    # every instance is a configs/materials_metrics/ember/*.json file whose "scrape"
    # block rides the scraper's parameterized ``ember.installed_capacity`` recipe
    # (series + is_aggregate_series as params; the all-renewables roll-up omits
    # series). The monthly endpoint returns the full series in one call (no date
    # window sent, so Ember serves from its 2020-12 floor) -> the "full" period
    # kind; the fact_values MERGE dedups the re-fetched overlap across runs. The api
    # key is injected as an env override from an Airflow Variable (see
    # SOURCE_SECRET_ENV). Add a series by committing one config file.
    "ember": {},
    # worldstainless.org meltshop production (static HTML table, no browser/credential).
    # The data page renders only the latest quarter (two same-quarter years side by side,
    # by region), with no request window -- the recipe takes no params, so worldstainless
    # uses the "full" period kind ({}); each run re-fetches the current snapshot and the
    # fact_values MERGE dedups the rolling overlap across runs. No SOURCE_SECRET_ENV entry:
    # the page is public.
    "worldstainless": {
        "worldstainless.meltshop_production": {},
    },
    # CSSC customs-trade (browser-crawl + Gemini/Vertex LLM parse). The scraper crawls the
    # 进出口量 list page for articles whose PUBLISH DATE falls in the requested window, parses
    # each with Gemini (Vertex AI via the job SA's ADC in prod), validates against the source,
    # and returns long rows plus a validation report. Daily publish-date grain, so it uses the
    # "cssc" period kind: the scheduled monitor windows to the single day before logical_date
    # (the "check for data published yesterday, parse it if present" cadence). A day with no
    # new article returns status=empty (exit 0) and the load self-skips. The recipe's
    # ``adjudicate`` flag stays False (deterministic validation report only); set it true via
    # conf to also attach the LLM adjudicator's verdict per flagged article. No site
    # credential; VERTEX_LOCATION is injected from an Airflow Variable (see SOURCE_CONFIG_ENV).
    "cssc": {
        "cssc.customs_trade": {"adjudicate": False},
    },
    # China Customs / GACC (海关总署) English monthly bulletin, "(14) Major Import
    # Commodities" table (static HTML, no browser/credential -- like worldstainless).
    # Fully CONFIG-DECLARED: every instance is a configs/materials_metrics/gacc/*.json
    # file whose "scrape" block rides the scraper's parameterized ``gacc.commodity``
    # recipe (commodity slug / keyword / optional hs6 + historical aliases as params;
    # each reads one commodity line of the table). gacc uses the "gacc" period kind:
    # the recipe REQUIRES ISO ``start``/``end`` bounds (windowed by month, day
    # ignored) and returns one record per published month; a window with no published
    # month returns status=empty (exit 0) and the load self-skips. Public site, so no
    # SOURCE_SECRET_ENV / SOURCE_CONFIG_ENV entry. Add a commodity by committing one
    # config file.
    "gacc": {},
    # CFTC Commitments of Traders (Legacy Futures-Only, Socrata Public Reporting API,
    # no auth -- like worldstainless/yfinance). Fully CONFIG-DECLARED: every instance
    # is a configs/materials_metrics/cftc/*.json file whose "scrape" block rides the
    # scraper's parameterized ``cftc.contract`` recipe (the contract market code --
    # the single-commodity filter -- as its param). WEEKLY grain: each row is one
    # Tuesday report date, and every row of a recipe shares the same commodity (the
    # config matches on it). cftc uses the "cftc" period kind: a rolling ISO
    # ``start``/``end`` date window (default the last monitor_window_days through
    # yesterday, per-commodity floor_date clamp). A window with no published report
    # returns status=empty (exit 0) and the load self-skips. Public API, so no
    # SOURCE_SECRET_ENV / SOURCE_CONFIG_ENV entry. Add a commodity by committing one
    # config file.
    "cftc": {},
    # PETRONET (한국석유공사) daily international crude oil price table -- Dubai,
    # Brent(ICE), WTI(NYMEX), Oman (static HTML, no browser/credential -- like
    # worldstainless/gacc). One recipe returns all four benchmarks (long rows, one
    # per date x product), so the four-series table is the atom -> {}. petronet uses
    # the "petronet" period kind: a rolling ISO ``start``/``end`` DATE window (like
    # cftc -- default the last monitor_window_days through yesterday, per-recipe
    # floor_date clamp), since the series is DAILY (business days). A window with no
    # published trading day returns status=empty (exit 0) and the load self-skips.
    # Public endpoint, so no SOURCE_SECRET_ENV / SOURCE_CONFIG_ENV entry.
    "petronet": {
        "petronet.crude_oil_price": {},
    },
    # FRBNY GSCPI (Global Supply Chain Pressure Index) -- NY Fed's monthly composite of
    # global supply-chain pressure (in std deviations), served only as a downloadable
    # workbook (no API, no FRED mirror). Conceptually a twin of a FRED series: the
    # scraper GETs the full monthly history in one call (a plain HTTP fetch, no browser
    # -- the worldstainless/petronet profile) and parses the .xls, so gscpi uses the same
    # "full" period kind as FRED/EIA -- no range params ({}); each run re-fetches the full
    # history back to 1998-01 and the fact_values MERGE dedups the overlap. One recipe
    # covers the single series (identity stamped as ``series="GSCPI"`` on every row, which
    # the config matches on). Public workbook, so no SOURCE_SECRET_ENV / SOURCE_CONFIG_ENV.
    "gscpi": {
        "gscpi.pressure_index": {},
    },
    # Eurostat SDMX 3.0 dissemination API (public CSV, no browser/credential). Distinct
    # from the "estat" source above, which is JAPAN's e-Stat -- unrelated agency and API.
    # One recipe covers the euro-area industrial production index (sts_inpr_m, NACE B-D,
    # 2021=100) and returns BOTH adjustments Eurostat publishes for that cut in a single
    # request: CA (working-day adjusted) and SCA (seasonally + working-day adjusted). They
    # are different time series, not two views of one, so the scraper stamps s_adj into the
    # ``series`` identity and the config maps each to its own metric. Fetching both costs
    # one round-trip (c[s_adj]=CA,SCA is an OR-list), so the pair is the atom -> {}.
    # eurostat uses the "full" period kind: sending no window returns the full history back
    # to 1991-01 and the fact_values MERGE dedups the re-fetched overlap. This also dodges
    # the endpoint's sharpest edge -- a window past the TIME_PERIOD codelist answers HTTP
    # 400 (not an empty 200), and Eurostat lags the reference month by ~2 months, so a
    # month-window monitor would fail routinely. Public endpoint, so no SOURCE_SECRET_ENV /
    # SOURCE_CONFIG_ENV entry.
    "eurostat": {
        "eurostat.industrial_production": {},
    },
    # Australian Bureau of Statistics SDMX 2.1 Data API (public CSV, no browser/credential).
    # One recipe covers monthly export values of SITC 285 ("Aluminium ores and concentrates,
    # incl. alumina") to all countries from all states -- dataflow MERCH_EXP, key
    # 285.TOT.TOT.M. The dimension key is baked into the scraper recipe (a mistyped code and
    # an empty window both answer an identical 404 NoRecordsFound), so the single series is
    # the atom -> {}. abs uses the "full" period kind: sending no window returns the full
    # history back to 1995-07 (the dataflow's own floor, 371 contiguous months) and the MERGE
    # dedups the overlap. NOTE the scraper normalizes the published figures, which ABS reports
    # in THOUSANDS of AUD (UNIT_MULT=3), to whole AUD -- the config's unit is AUD accordingly.
    # Public endpoint, so no SOURCE_SECRET_ENV / SOURCE_CONFIG_ENV entry.
    "abs": {
        "abs.aluminium_ore_exports": {},
    },
    # Statistics Canada CIMT (Canadian International Merchandise Trade) REST service, which
    # backs StatCan's published 71-607-x web application (public JSON, no browser/credential
    # -- but the endpoint REQUIRES a Referer header, which the scraper sends; without it the
    # service answers a 404 HTML page). This is deliberately NOT the documented WDS API: no
    # CODR cube carries 8-digit HS (the finest trade cuts are NAPCS and HS sections), and the
    # nearest CODR series is a superset that also folds in alloyed aluminum. One recipe covers
    # HS 76011000 ("Aluminium unwrought, not alloyed") exports, Canada -> World; the HS code is
    # baked into the scraper recipe (a bogus code and an empty window both answer an identical
    # empty chart), so the commodity is the atom -> {}. Each month yields up to four NATIVE
    # measures (domestic/re-export x value CAD / quantity KGM); the scraper never synthesises a
    # total, so the config selects the measure it wants by ``series``. statcan uses the "full"
    # period kind: sending no window collects the full history back to 1988-01 (the chart
    # endpoint returns only a 61-month rolling window, so the scraper pages backwards in ~8
    # calls) and the MERGE dedups the overlap. No SOURCE_SECRET_ENV / SOURCE_CONFIG_ENV entry.
    "statcan": {
        "statcan.aluminum_unwrought_exports": {},
    },
    # Shanghai Futures Exchange (SHFE) reports (no credential; the scraper solves the
    # weekly report's post-2025-11 SafeLine WAF challenge itself). Fully
    # CONFIG-DECLARED: every instance is a configs/materials_metrics/shfe/*.json file
    # whose "scrape" block rides one of the scraper's parameterized recipes
    # (product_id as its param; the ISO start/end date window is added by this
    # source's "shfe" period kind):
    #
    # - ``shfe.futures_daily`` -- the scraper enumerates the window's trading days
    #   client-side and emits every listed delivery-month contract per day; the daily
    #   futures config stamps each row's ``contract_rank`` as the calendar month
    #   delta from the trading day to the contract's delivery month at staging
    #   (stamp_contract_ranks) and its ``rank_expansion`` fans each measure into
    #   ``<name>_<measure>_0..12`` metrics (fixed month offsets, so a day past the
    #   spot contract's delisting simply has no ``_0`` value).
    # - ``shfe.weekly_stock`` -- the weekly warehouse-stock report (one publication
    #   Friday per week); the scraper emits every matching (region, warehouse) row
    #   plus the exchange's subtotal/total rows tagged by ``row_type``, and the config
    #   picks the aggregate it wants (the grand-total ``row_type=total`` /
    #   ``warehouse=Total`` row) with Stock / On-Warrant weights as measures.
    #
    # Add a product/report by committing one config file.
    "shfe": {},
    # London Metal Exchange (LME) trading data + weekly MiFID Commitments-of-Traders
    # workbooks (no credential; the whole www.lme.com zone sits behind a Cloudflare
    # managed challenge only a HEADED Chromium passes, so the scraper drives every
    # request through an in-page fetch itself -- see the scraper repo's
    # docs/lme-recipe-addendum.md). The recipes are per-metal and UNPARAMETERIZED
    # (the datasource GUIDs / COT listing path are baked scraper-side), so they
    # register statically here like statcan; each recipe still needs its
    # configs/materials_metrics/lme/<recipe>.json to load. All three read an ISO
    # ``start``/``end`` window (the "lme" period kind):
    #
    # - ``lme.aluminium_official_prices`` -- per-trading-day official Bid/Offer per
    #   contract (Cash / 3-month / Dec-N) from the chart-data endpoint. History is a
    #   SLIDING 5-year wall anchored to today (pre-wall data is permanently gone and
    #   an entirely-pre-wall window is a scraper-side HTTP 500 failure), so
    #   _build_lme_params clamps the start to the wall dynamically -- there is no
    #   static floor_date to declare.
    # - ``lme.aluminium_stocks`` -- per-business-day Opening Stock / Live warrants /
    #   Cancelled warrants (tonnes) from the day-delayed table. Data is T-1 with a
    #   ~5-business-day lookback and NO history exists anywhere, so the series is
    #   accumulated forward by the daily monitor; the config's floor_date marks the
    #   earliest day ever captured (deeper requests answer empty).
    # - ``lme.aluminium_commitments_of_traders`` -- the weekly MiFID COT "Number of
    #   Positions" block, one row per (as-of Friday, position-holder category), lots.
    #   Published the following Tuesday ~05:00Z; the scraper windows on the as-of
    #   date, so the daily monitor picks each report up from its publication day.
    #   The curated net/open-interest metrics are derived at staging
    #   (derive_lme_cot_summary) and matched via the synthetic market_summary row.
    "lme": {
        # DISABLED (2026-07-15): the daily chart-data recipes are being served a
        # Cloudflare managed challenge the scraper's headed Chromium no longer
        # passes, so every scrape is blocked. Unregistered here AND their
        # configs/materials_metrics/lme/*.json removed (a committed config that
        # maps to no registered recipe fails the metric-file test). To re-enable
        # once the gate clears: uncomment the two lines below and restore the two
        # config files from git history. Only the weekly MiFID COT workbook still
        # gets through, so it stays registered.
        # "lme.aluminium_official_prices": {},
        # "lme.aluminium_stocks": {},
        "lme.aluminium_commitments_of_traders": {},
    },
}

# Config-declared dynamic recipes (the dpanda_bloomberg model). A source's materials
# config file may carry a top-level "scrape" block naming one of the scraper's
# PARAMETERIZED recipes (fred.series, yfinance.history) plus the identity params it
# bakes (series_id / symbol / value_field ...). Discovery merges each such file into
# SOURCES at parse time: the file stem becomes the pipeline identity -- the
# request/result/staging objects, floor clamp, trigger-form entry, normalize, and
# transform-load all key on it, exactly like a statically-registered recipe -- its
# scrape.params become the recipe's default query (conf-overridable per run like any
# other recipe query, so a manual run can retarget the ticker/series), and
# build_source_requests names the parameterized scraper recipe in the request payload
# instead of the stem (DYNAMIC_RECIPE_KEYS). Adding a fred/yfinance metric is
# therefore ONE committed config file: no scraper deploy, no edit here. A malformed
# scrape block fails DAG parse loudly (it is CI-validated with the other config
# files), and declaring one for a stem that is also statically registered above is a
# conflict -- there must be exactly one registration per recipe.
DYNAMIC_RECIPE_KEYS: dict[str, str] = {}
for _source in SOURCES:
    for _instance, _scrape in discover_scrape_recipes(_source).items():
        if _instance in SOURCES[_source]:
            raise ValueError(
                f"{_instance}: config declares a scrape block but the recipe is "
                "already statically registered in SOURCES; keep exactly one "
                "registration"
            )
        SOURCES[_source][_instance] = dict(_scrape["params"])
        DYNAMIC_RECIPE_KEYS[_instance] = _scrape["recipe"]

# Daily-monitor default windows (overridable per run via the trigger form / conf).
# Month-grain sources pull a width-N window ending at the run's current month, so the
# default 2 is ``[prev .. current]`` -- it picks up the previous month the day it
# publishes while harmlessly re-scraping it until the current month lands (idempotent
# through the MERGE). Date-grain sources pull the last N days through *yesterday*; 7
# guarantees the range spans a trading day across weekends/holidays.
DEFAULT_MONITOR_WINDOW_MONTHS = 2
DEFAULT_MONITOR_WINDOW_DAYS = 7

# Every recipe key across all sources, for the ``recipes`` multi-select Param's enum
# and default (all selected). Built at parse time so the trigger form's options track
# SOURCES automatically -- adding a recipe to SOURCES adds it to the form.
ALL_RECIPES = [recipe for recipes in SOURCES.values() for recipe in recipes]

# Each source's request period convention. Month-grain sources (kosa, kosis) take a
# four-int ``[start..end]`` month range; date-grain sources (yfinance) take an ISO
# ``start_date``/``end_date`` range (daily series). A source absent here defaults to
# the month convention, so adding another month-grain source needs no entry.
SOURCE_PERIOD_KIND: dict[str, str] = {
    "kosa": "month",
    "kosis": "month",
    "yfinance": "date",
    # EIA recipes take no range -- the seriesid endpoint returns the full series in one
    # call -- so they carry only their (empty) query and the MERGE dedups re-runs.
    "eia": "full",
    # FRED's observations endpoint returns the full daily series in one call, so FRED
    # uses the same "full" kind as EIA -- no range params, the MERGE dedups re-fetched
    # rows across runs. (The scraper recipe still accepts optional start/end for manual
    # backfills, but the daily DAG sends none.)
    "fred": "full",
    # e-Stat's getStatsData returns the full monthly series for a baked table+product in
    # one call, so estat uses the same "full" kind -- no range params, the MERGE dedups
    # re-fetched rows across runs. (The scraper recipe accepts optional start/end for
    # manual backfills via cdTimeFrom/cdTimeTo, but the daily DAG sends none.)
    "estat": "full",
    # US Census's timeseries endpoint returns the full monthly series for a baked
    # commodity in one call (the recipe defaults the mandatory ``time`` predicate to the
    # series floor when no range is sent), so us_census uses the same "full" kind -- no
    # range params, the MERGE dedups re-fetched rows across runs. (The scraper recipe
    # accepts optional start/end for manual backfills, but the daily DAG sends none.)
    "us_census": "full",
    # Ember's installed-capacity/monthly endpoint returns the full monthly series for a
    # baked World query in one call (the recipe omits the date window by default, serving
    # from Ember's 2020-12 floor), so ember uses the same "full" kind -- no range params,
    # the MERGE dedups re-fetched rows across runs. (The scraper recipe accepts optional
    # start/end for manual backfills, but the daily DAG sends none.)
    "ember": "full",
    # worldstainless's data page renders only the current-quarter snapshot with no window
    # control, and the recipe takes no params, so worldstainless uses the "full" kind -- no
    # range params ({}); each run re-fetches the snapshot and the MERGE dedups the rolling
    # overlap. (Period rows are quarterly; the config keys on the quarter-start period_start.)
    "worldstainless": "full",
    # CSSC is daily publish-date grain but its own kind: the recipe takes ``start``/``end``
    # (not the date-grain ``start_date``/``end_date``) and the scheduled monitor windows to
    # the single day before logical_date rather than an N-day look-back. See _build_cssc_params.
    "cssc": "cssc",
    # gacc resolves a MONTH window (like kosa/kosis: monitor default [prev..current],
    # explicit start_date/end_date backfill, per-recipe floor clamp) but the recipe
    # REQUIRES ISO ``start``/``end`` (first-of-month), so it can use neither "month"
    # (which emits four ints) nor "full" (which emits no params). Its own kind formats
    # the resolved window as first-of-month ISO. See _build_gacc_params.
    "gacc": "gacc",
    # cftc is a higher-than-monthly (WEEKLY) series like yfinance, so it resolves an ISO
    # DATE window (monitor default: the last monitor_window_days through yesterday;
    # explicit start_date/end_date backfill; per-commodity floor_date clamp) -- but the
    # scraper recipe reads ``start``/``end`` (not the date-grain ``start_date``/
    # ``end_date``), so it can't reuse the "date" kind. Its own kind resolves/clamps the
    # window exactly like "date" then emits it under start/end. See _build_cftc_params.
    "cftc": "cftc",
    # petronet is a DAILY (business-day) crude-price series with the same window
    # controls and scraper interface as cftc: it resolves an ISO DATE window (monitor
    # default the last monitor_window_days through yesterday; explicit start_date/
    # end_date backfill; per-recipe floor_date clamp) and the scraper recipe reads
    # ``start``/``end`` (not the date-grain ``start_date``/``end_date``). Its own kind
    # keeps the source self-documenting even though the builder mirrors cftc's. See
    # _build_petronet_params.
    "petronet": "petronet",
    # gscpi's workbook endpoint always returns the full monthly history (there is no
    # server-side window; the recipe's optional start/end filter client-side), so gscpi
    # uses the same "full" kind as FRED/EIA -- no range params, the recipe is invoked with
    # only its (empty) query and the fact_values MERGE dedups the re-fetched overlap across
    # runs. (The scraper recipe accepts optional start/end for manual backfills, but the
    # daily DAG sends none.)
    "gscpi": "full",
    # eurostat's SDMX 3.0 endpoint returns the full monthly history when no window is sent
    # (the recipe's optional start/end are for manual backfills), so eurostat uses the same
    # "full" kind as FRED/EIA -- no range params, the MERGE dedups the re-fetched overlap.
    # "full" is not merely convenient here, it is the correct kind: a month window past the
    # TIME_PERIOD codelist is an HTTP 400 rather than an empty 200, and Eurostat publishes
    # ~2 months in arrears, so a "month"-kind monitor asking for the current month would
    # fail on nearly every run.
    "eurostat": "full",
    # abs's SDMX 2.1 endpoint returns the full monthly history when no startPeriod/endPeriod
    # is sent, so abs uses the same "full" kind -- no range params, the MERGE dedups the
    # re-fetched overlap. (The scraper recipe accepts optional start/end for manual
    # backfills, but the daily DAG sends none.)
    "abs": "full",
    # statcan's CIMT chart endpoint serves only a 61-month rolling window per call, so the
    # scraper pages backwards to cover whatever window it is given; with no window it reads
    # the service's own getPeriods floor (1988-01) and collects the full history in ~8 calls.
    # That makes it a "full" kind -- no range params, the MERGE dedups the re-fetched overlap.
    # (The scraper recipe accepts optional start/end for manual backfills.)
    "statcan": "full",
    # shfe reports are date-windowed series -- daily futures per trading day, weekly
    # stock per publication Friday -- with the same date-window controls and scraper
    # interface as cftc/petronet: each resolves an ISO DATE window (monitor default
    # the last monitor_window_days through yesterday; explicit start_date/end_date
    # backfill; per-recipe floor_date clamp) and the scraper recipes read
    # ``start``/``end`` (not the date-grain ``start_date``/``end_date``). Its own
    # kind keeps the source self-documenting though the builder mirrors cftc's. See
    # _build_shfe_params.
    "shfe": "shfe",
    # lme series are date-windowed like cftc/petronet/shfe -- daily prices/stocks,
    # weekly COT (windowed on the report's as-of Friday) -- and the scraper recipes
    # read ``start``/``end``. Its own kind because the official-prices recipe needs a
    # DYNAMIC floor: the source serves a sliding 5-year window anchored to today, so
    # a static floor_date cannot express it. See _build_lme_params.
    "lme": "lme",
}

# Non-secret per-source config injected into the scraper job as env overrides (env var ->
# Airflow Variable). Unlike SOURCE_SECRET_ENV this is plain configuration, not a credential:
# it does NOT set SCRAPE_LOCAL_CREDENTIALS and the value is non-sensitive, so it is read with
# a fallback (the scraper has its own code default) rather than required. cssc's Vertex region
# is the only entry today; in prod the job authenticates to Vertex via ADC, no key.
SOURCE_CONFIG_ENV: dict[str, dict[str, str]] = {
    "cssc": {"VERTEX_LOCATION": "scraper_vertex_location"},
}

# Per-source credentials the DAG injects into the scraper job as env overrides (the
# scraper's SCRAPE_LOCAL_CREDENTIALS path), mapping the scraper env var name -> the
# Airflow Variable holding the value. EIA's api key is managed as the
# ``scraper_eia_api_key`` Variable and read by the scraper as ``EIA_API_KEY``; the DAG
# also sets ``SCRAPE_LOCAL_CREDENTIALS=1`` for these sources so the scraper reads the
# env instead of Secret Manager. Sources absent here resolve their own credentials in
# the job (Secret Manager) or need none (yfinance).
SOURCE_SECRET_ENV: dict[str, dict[str, str]] = {
    "eia": {"EIA_API_KEY": "scraper_eia_api_key"},
    "fred": {"FRED_API_KEY": "scraper_fred_api_key"},
    "estat": {"ESTAT_APP_ID": "scraper_estat_app_id"},
    "us_census": {"US_CENSUS_API_KEY": "scraper_us_census_api_key"},
    "ember": {"EMBER_API_KEY": "scraper_ember_api_key"},
}


def _source_extra_env(source: str) -> dict[str, str]:
    """Resolve a source's injected job env from Airflow Variables, or ``{}``.

    Two kinds, both read at scrape time (kept off XCom):

    - **Config** (SOURCE_CONFIG_ENV) -- non-secret settings like cssc's VERTEX_LOCATION.
      Read with a fallback (the scraper has a code default) and injected as-is; no
      ``SCRAPE_LOCAL_CREDENTIALS`` (it is configuration, not a credential).
    - **Credentials** (SOURCE_SECRET_ENV) -- e.g. EIA's api key. Required, and accompanied
      by ``SCRAPE_LOCAL_CREDENTIALS=1`` so the scraper reads the env instead of Secret
      Manager.
    """
    env: dict[str, str] = {}

    for env_name, variable_name in SOURCE_CONFIG_ENV.get(source, {}).items():
        value = Variable.get(variable_name, default=None)
        if value:
            env[env_name] = value

    secret_mapping = SOURCE_SECRET_ENV.get(source)
    if secret_mapping:
        for env_name, variable_name in secret_mapping.items():
            env[env_name] = _required_variable(variable_name)
        env["SCRAPE_LOCAL_CREDENTIALS"] = "1"

    return env

# Per-source anti-burst pacing. A paced source (any nonzero entry here) collapses its
# scrapes into a single serial pod (run_scraper_serial; see the build-out loop), each
# successive scrape waiting this many seconds before hitting the source -- a deliberate
# inter-request gap. Two reasons a source is paced:
#
# - kosa's origin webpage blocks request bursts, so it MUST serialize with a long gap.
# - yfinance has ~30 tiny daily-FX recipes, each a trivial yfinance SDK call. Fanning
#   them out was one pod per recipe (~30 pods) for work the pod merely orchestrates --
#   exactly the pod-start-up-dominates anti-pattern AGENTS.md warns against -- and 30
#   simultaneous pods also risk Yahoo's burst rate limit. Collapsing to one serial pod
#   with a short gap pays the pod fixed cost once and spaces the Yahoo calls; the scrapes
#   are trivial, so the serial wall-clock is far below 30x pod start-up.
#
# - lme sits behind a Cloudflare managed challenge (the scraper opens a headed Chromium
#   per execution); three simultaneous browser sessions from the same egress invite an
#   IP-level escalation of exactly the gate the scraper must pass, so its three recipes
#   run serially with a short gap. Each scrape is a page-open plus a handful of in-page
#   fetches, so the serial wall-clock stays minutes.
#
# Sources absent here (kosis HTTP API, eia, ...) tolerate concurrency and are genuinely
# heavy/independently-retryable per recipe, so their scrapes dynamic-map into independent
# parallel pods, ungapped (default 0). Overridable per run via conf ``scrape_delay_seconds``.
SOURCE_SCRAPE_DELAY_SECONDS: dict[str, int] = {"kosa": 60, "yfinance": 2, "lme": 10}

# Sources whose resolved ISO date window is split into per-CALENDAR-YEAR chunk
# requests at build time (split_date_range_yearly). The shfe scraper enumerates
# every trading day of its window serially inside ONE Cloud Run execution, so a
# deep backfill (shfe.futures_daily's floor is 2002-01-07 -- a 24+ year window)
# in a single request exceeds the job's 1h task_timeout and fails. Chunking bounds
# each execution to at most one year of trading days: build_requests emits one
# request/result object pair per year (chunk-suffixed), execute dynamic-maps one
# pod / Cloud Run execution per chunk, and normalize concatenates a recipe's chunk
# results back into its single staging object -- the BigQuery load is unchanged.
# A window inside a single year (the daily monitor) builds exactly as before, one
# unchunked request. Only meaningful for sources whose params carry an ISO
# ``start``/``end`` window.
SOURCE_YEARLY_CHUNKED: frozenset[str] = frozenset({"shfe"})

# Cap on a yearly-chunked source's concurrently-running execute pods (mapped task
# instances per dag run). A 24-year backfill fans out ~24 chunk executions; running
# them all at once would burst dozens of parallel scrapes at the origin (SHFE
# WAF-guards its endpoints), so only this many chunks scrape at a time -- each
# still safely under the Cloud Run task timeout.
CHUNKED_EXECUTE_MAX_PARALLELISM = 3


def _source_scrape_delay(source: str) -> int:
    return SOURCE_SCRAPE_DELAY_SECONDS.get(source, 0)


def _required_variable(name: str) -> str:
    value = Variable.get(name, default=None)
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _config() -> dict[str, str]:
    # No gcp_conn_id / impersonation_chain: GCP access is via ADC (the Airflow
    # workload identity SA), and per dev policy tasks use no impersonation.
    return {
        "project_id": _required_variable("scraper_gcp_project_id"),
        "region": _required_variable("scraper_cloud_run_region"),
        "job_name": _required_variable("scraper_cloud_run_job_name"),
        "bucket": _required_variable("scraper_scrape_bucket_name"),
    }


def _bq_config() -> dict[str, str]:
    # The transform-load writes BigQuery, so it carries its own materials_bigquery_*
    # Variables. The dataset defaults to dl_materials (Terraform owns the
    # dataset/tables).
    return {
        "project_id": _required_variable("materials_bigquery_project_id"),
        "dataset_id": Variable.get(
            "materials_bigquery_dataset_id", default="dl_materials"
        ),
        "region": _required_variable("materials_bigquery_region"),
    }


# BigQuery labels accept only [a-z0-9_-] (<=63 chars); job-id prefixes allow mixed
# case and are longer. Slugify free-form identity (run ids carry ':' and '+') so the
# combined load job still submits and stays greppable in the console / JOBS views.
_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")
_JOB_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")

# Truncate a logged sample record so one recipe's first staged row cannot flood the
# task log; mirrors the dpanda Bloomberg ingest's per-grain sample preview. A sample
# makes the actual staged field names/values inspectable from the log, so a silent
# drop (the row never matched a metric or its period failed PARSE_DATE) is diagnosable
# without trawling GCS or BigQuery.
SAMPLE_PREVIEW_CHARS = 280


def _label_value(value: Any) -> str:
    return _LABEL_DISALLOWED.sub("_", str(value).lower())[:63]


def _load_labels(context: dict[str, Any], source: str) -> dict[str, str]:
    dag_run = context.get("dag_run")
    return {
        "dag_id": _label_value(getattr(dag_run, "dag_id", "") or ""),
        "run_id": _label_value(context.get("run_id", "")),
        "source": _label_value(source),
        "step": "load",
    }


def _load_job_id_prefix(context: dict[str, Any], source: str) -> str:
    raw = f"scrape_{source}_{context.get('run_id', '')}_load_"
    return _JOB_ID_DISALLOWED.sub("_", raw)[:512]


def _dag_params() -> dict[str, Any]:
    # Runtime inputs come from ``context["params"]``, which unifies three entry modes
    # into one dict: the Param *defaults* (a scheduled daily monitor run), the trigger
    # *form* (a manual targeted re-pull / backfill), and any raw ``dag_run.conf`` keys
    # (API triggers and advanced per-recipe query overrides, which aren't first-class
    # form fields). The resolve/select helpers below all read this single dict.
    context = get_current_context()
    return dict(context.get("params") or {})


def _run_point_kst():
    # A manually-triggered run can have logical_date=None and no data interval
    # (scheduled daily runs do carry one). Fall back through the run's timestamp,
    # then to now, instead of failing.
    import pendulum

    context = get_current_context()
    dag_run = context.get("dag_run")
    run_point = (
        context.get("logical_date")
        or context.get("data_interval_start")
        or getattr(dag_run, "run_after", None)
        or getattr(dag_run, "logical_date", None)
        or pendulum.now("UTC")
    )
    # context datetimes are pendulum-aware; normalize to KST before deriving month.
    return run_point.in_timezone("Asia/Seoul")


def _resolve_period(conf: dict[str, Any]) -> tuple[int, int, int, int]:
    """Return (start_year, start_month, end_year, end_month) for the request.

    The kosa/kosis recipes take a start..end month range and return one record per
    month. Precedence:

    1. **Explicit month range** ``{"start_year","start_month",[ "end_year","end_month"
       ]}`` in conf -> used as-is (end defaults to start). The raw-conf backfill path.
    2. **Explicit ISO date range** ``{"start_date",[ "end_date" ]}`` (the unified
       trigger-form field) -> the enclosing ``(year, month)`` bounds of those dates;
       end defaults to start. This is the form-driven targeted re-pull / backfill.
    3. **Single month** ``{"year","month"}`` in conf -> ``start == end``.
    4. **Derived monitor window** -> a width-``monitor_window_months`` window ending
       at the run's current month (KST); default 2 -> ``[prev .. current]``. This is
       the scheduled daily-monitor default.
    """

    if "start_year" in conf and "start_month" in conf:
        start_year = int(conf["start_year"])
        start_month = int(conf["start_month"])
        end_year = int(conf.get("end_year", start_year))
        end_month = int(conf.get("end_month", start_month))
        return start_year, start_month, end_year, end_month

    start_date = conf.get("start_date")
    if start_date:
        end_date = conf.get("end_date") or start_date
        start_year, start_month = year_month_from_iso(str(start_date))
        end_year, end_month = year_month_from_iso(str(end_date))
        return start_year, start_month, end_year, end_month

    if conf.get("year") and conf.get("month"):
        year, month = int(conf["year"]), int(conf["month"])
        return year, month, year, month

    window = int(conf.get("monitor_window_months") or DEFAULT_MONITOR_WINDOW_MONTHS)
    run_point = _run_point_kst()
    return month_window_range(run_point.year, run_point.month, window)


def _resolve_date_range(conf: dict[str, Any]) -> tuple[str, str]:
    """Return ISO ``(start_date, end_date)`` for a date-grain (daily) recipe.

    The date-grain analog of :func:`_resolve_period`; both ends inclusive. Precedence:

    1. **Explicit range** ``{"start_date",[ "end_date" ]}`` (the unified trigger-form
       field) -> the targeted re-pull / backfill; end defaults to start. The end is
       clamped down to *yesterday* (KST) -- a backfill obeys the same never-today
       rule as the monitor (shfe's daily kx file is served intraday with no
       settlement prices until after the Beijing close, so requesting "today"
       scrapes a not-yet-published day); a range entirely after yesterday skips.
    2. **Derived monitor window** -> the last ``monitor_window_days`` days through
       *yesterday* (KST), so a scheduled daily run always covers at least one trading
       day and never requests a not-yet-traded "today" (dedup-by-(recipe, date)
       absorbs the day-to-day overlap through the MERGE).
    """

    start_date = conf.get("start_date")
    if start_date:
        end_date = str(conf.get("end_date") or start_date)
        start_date = str(start_date)
        yesterday = _run_point_kst().subtract(days=1).to_date_string()
        clamped = clamp_date_range_to_ceiling(start_date, end_date, yesterday)
        if clamped is None:
            raise AirflowSkipException(
                f"requested range {start_date}..{end_date} is entirely after "
                f"yesterday {yesterday}; not-yet-published days are never scraped"
            )
        if clamped[1] != end_date:
            logger.warning(
                "clamped end %s down to yesterday %s "
                "(a not-yet-published day is never requested)",
                end_date,
                clamped[1],
            )
        return clamped

    days = int(conf.get("monitor_window_days") or DEFAULT_MONITOR_WINDOW_DAYS)
    run_point = _run_point_kst()
    end = run_point.subtract(days=1)
    return recent_days_range_iso(end.to_date_string(), days)


def _conf_query_overlay(
    default_query: dict[str, Any], conf: dict[str, Any]
) -> dict[str, Any]:
    # Only the recipe's own query keys are overridable from conf; the shared period
    # range is resolved separately so it applies uniformly to every recipe in the run.
    query_keys = tuple(default_query)
    return merge_params(default_query, {k: conf[k] for k in query_keys if k in conf})


def _build_month_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    params = _conf_query_overlay(default_query, conf)

    start_year, start_month, end_year, end_month = _resolve_period(conf)

    # The shared range is resolved once, but each month-grain recipe has its own
    # static earliest-available period (its floor_period config) and KOSA hard-fails
    # a below-floor start by design (KOSIS's empty-array below-floor case is skipped
    # the same way). Clamp the start UP to this recipe's floor; recipes with no floor
    # (e.g. dpanda Bloomberg) keep the range as-is.
    floor = load_recipe_floor(recipe)
    if floor is not None:
        clamped = clamp_period_to_floor(
            start_year, start_month, end_year, end_month, floor[0], floor[1]
        )
        if clamped is None:
            # Whole range below the floor (even end < floor): an explicit empty
            # request. Skip rather than emit a nonsensical/inverted range.
            raise AirflowSkipException(
                f"{recipe}: requested range "
                f"{start_year}-{start_month:02d}..{end_year}-{end_month:02d} is "
                f"entirely below the {floor[0]}-{floor[1]:02d} floor; nothing to scrape"
            )
        if (clamped[0], clamped[1]) != (start_year, start_month):
            # WARN so a masked bug (wrong year from an unset default or typo) stays
            # observable rather than silently absorbed by the clamp.
            logger.warning(
                "%s: clamped start %d-%02d up to floor %d-%02d "
                "(requested start was below the recipe's earliest available period)",
                recipe, start_year, start_month, clamped[0], clamped[1],
            )
        start_year, start_month, end_year, end_month = clamped

    params["start_year"] = start_year
    params["start_month"] = start_month
    params["end_year"] = end_year
    params["end_month"] = end_month
    return params


def _build_date_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    params = _conf_query_overlay(default_query, conf)

    start_date, end_date = _resolve_date_range(conf)

    # The date-grain analog of the month clamp: a daily series has an earliest-
    # available day (its floor_date config; yfinance's TRYUSD=X starts 2015-01-01)
    # and the scraper returns an empty result -> clean exit 1 for a fully-below
    # request. Clamp the start UP to the recipe's floor; skip when the whole range
    # is below it.
    floor = load_recipe_date_floor(recipe)
    if floor is not None:
        clamped = clamp_date_range_to_floor(start_date, end_date, floor)
        if clamped is None:
            raise AirflowSkipException(
                f"{recipe}: requested range {start_date}..{end_date} is entirely "
                f"below the {floor} floor; nothing to scrape"
            )
        if clamped[0] != start_date:
            logger.warning(
                "%s: clamped start %s up to floor %s "
                "(requested start was below the recipe's earliest available date)",
                recipe, start_date, clamped[0],
            )
        start_date, end_date = clamped

    params["start_date"] = start_date
    params["end_date"] = end_date
    return params


def _build_cftc_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    """Build cftc params: ISO ``{start, end}`` for a weekly COT date window.

    cftc has the same date-window controls as yfinance -- monitor default (the last
    ``monitor_window_days`` through yesterday), explicit ``start_date``/``end_date``
    backfill, per-recipe ``floor_date`` clamp -- but the scraper recipe reads
    ``start``/``end`` rather than the date-grain ``start_date``/``end_date``. So
    resolve and floor-clamp the window in ISO-date units (reusing _resolve_date_range /
    clamp_date_range_to_floor, exactly like _build_date_params), then emit it under the
    recipe's ``start``/``end`` keys.
    """
    params = _conf_query_overlay(default_query, conf)

    start_date, end_date = _resolve_date_range(conf)

    floor = load_recipe_date_floor(recipe)
    if floor is not None:
        clamped = clamp_date_range_to_floor(start_date, end_date, floor)
        if clamped is None:
            raise AirflowSkipException(
                f"{recipe}: requested range {start_date}..{end_date} is entirely "
                f"below the {floor} floor; nothing to scrape"
            )
        if clamped[0] != start_date:
            logger.warning(
                "%s: clamped start %s up to floor %s "
                "(requested start was below the recipe's earliest available date)",
                recipe, start_date, clamped[0],
            )
        start_date, end_date = clamped

    params["start"] = start_date
    params["end"] = end_date
    return params


def _build_petronet_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    """Build petronet params: ISO ``{start, end}`` for a daily crude-price date window.

    petronet is a DAILY (business-day) series with the same date-window controls as
    yfinance/cftc -- monitor default (the last ``monitor_window_days`` through
    yesterday), explicit ``start_date``/``end_date`` backfill, per-recipe ``floor_date``
    clamp -- but the scraper recipe reads ``start``/``end`` (not the date-grain
    ``start_date``/``end_date``). So resolve and floor-clamp the window in ISO-date units
    (reusing _resolve_date_range / clamp_date_range_to_floor, exactly like
    _build_cftc_params), then emit it under the recipe's ``start``/``end`` keys. A window
    with no published trading day returns status=empty (exit 0) and the load self-skips.
    """
    params = _conf_query_overlay(default_query, conf)

    start_date, end_date = _resolve_date_range(conf)

    floor = load_recipe_date_floor(recipe)
    if floor is not None:
        clamped = clamp_date_range_to_floor(start_date, end_date, floor)
        if clamped is None:
            raise AirflowSkipException(
                f"{recipe}: requested range {start_date}..{end_date} is entirely "
                f"below the {floor} floor; nothing to scrape"
            )
        if clamped[0] != start_date:
            logger.warning(
                "%s: clamped start %s up to floor %s "
                "(requested start was below the recipe's earliest available date)",
                recipe, start_date, clamped[0],
            )
        start_date, end_date = clamped

    params["start"] = start_date
    params["end"] = end_date
    return params


def _build_shfe_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    """Build shfe params: ISO ``{start, end}`` for a trading-day / publication window.

    shfe reports are date-windowed series -- daily futures per trading day, weekly stock
    per publication Friday -- with the same date-window controls and scraper interface
    as cftc/petronet: monitor default (the last ``monitor_window_days`` through
    yesterday), explicit ``start_date``/``end_date`` backfill, per-recipe ``floor_date``
    clamp -- and the scraper recipes read ``start``/``end`` (not the date-grain
    ``start_date``/``end_date``). So resolve and floor-clamp the window in ISO-date
    units (reusing _resolve_date_range / clamp_date_range_to_floor, exactly like
    _build_cftc_params), then emit it under the recipe's ``start``/``end`` keys. The
    scraper enumerates the window's trading days (daily) or publication Fridays
    (weekly) itself; a window with none returns status=empty (exit 0) and the load
    self-skips. One execution fetches every date in its window serially, so a
    multi-year backfill window is NOT sent as one request: shfe is in
    SOURCE_YEARLY_CHUNKED and ``build_requests`` splits the resolved window into
    per-calendar-year chunk requests, keeping each Cloud Run execution under the
    job's task timeout.
    """
    params = _conf_query_overlay(default_query, conf)

    start_date, end_date = _resolve_date_range(conf)

    floor = load_recipe_date_floor(recipe)
    if floor is not None:
        clamped = clamp_date_range_to_floor(start_date, end_date, floor)
        if clamped is None:
            raise AirflowSkipException(
                f"{recipe}: requested range {start_date}..{end_date} is entirely "
                f"below the {floor} floor; nothing to scrape"
            )
        if clamped[0] != start_date:
            logger.warning(
                "%s: clamped start %s up to floor %s "
                "(requested start was below the recipe's earliest available date)",
                recipe, start_date, clamped[0],
            )
        start_date, end_date = clamped

    params["start"] = start_date
    params["end"] = end_date
    return params


# The official-prices recipe's history floor is a SLIDING wall: the LME chart-data
# endpoint serves only the last 5 years anchored to TODAY (an entirely-pre-wall window
# is an HTTP 500 scraper failure, and a straddling one answers zero-padded pre-wall
# points the scraper drops). A static config floor_date cannot express that, so
# _build_lme_params clamps these recipes' start to today - 5y plus a safety margin
# (the wall moves daily; the margin keeps a request built today valid when it actually
# runs). Pre-wall history is permanently unrecoverable, so the clamp loses nothing.
LME_SLIDING_WALL_RECIPES = frozenset({"lme.aluminium_official_prices"})
LME_SLIDING_WALL_SAFETY_DAYS = 7


def _build_lme_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    """Build lme params: ISO ``{start, end}`` for a trading-day / as-of-Friday window.

    lme series have the same date-window controls and scraper interface as
    cftc/petronet/shfe: monitor default (the last ``monitor_window_days`` through
    yesterday), explicit ``start_date``/``end_date`` backfill, per-recipe ``floor_date``
    clamp -- and the scraper recipes read ``start``/``end``. Two lme-specific floors
    compose (the higher wins): the config's static ``floor_date`` (the COT listing's
    2020-01-03; the stocks capture epoch) and the official-prices recipes' DYNAMIC
    sliding 5-year wall (LME_SLIDING_WALL_RECIPES). The COT recipe windows on the
    report's as-of Friday; the scraper resolves publication-lag candidates itself, so
    no window widening happens here.
    """
    params = _conf_query_overlay(default_query, conf)

    start_date, end_date = _resolve_date_range(conf)

    floor = load_recipe_date_floor(recipe)
    if recipe in LME_SLIDING_WALL_RECIPES:
        wall = (
            _run_point_kst()
            .subtract(years=5)
            .add(days=LME_SLIDING_WALL_SAFETY_DAYS)
            .to_date_string()
        )
        floor = wall if floor is None else max(floor, wall)
    if floor is not None:
        clamped = clamp_date_range_to_floor(start_date, end_date, floor)
        if clamped is None:
            raise AirflowSkipException(
                f"{recipe}: requested range {start_date}..{end_date} is entirely "
                f"below the {floor} floor; nothing to scrape"
            )
        if clamped[0] != start_date:
            logger.warning(
                "%s: clamped start %s up to floor %s "
                "(requested start was below the recipe's earliest available date)",
                recipe, start_date, clamped[0],
            )
        start_date, end_date = clamped

    params["start"] = start_date
    params["end"] = end_date
    return params


def _build_cssc_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    """Build cssc params: ``{start, end[, adjudicate]}`` for a daily publish check.

    The cssc recipe takes ISO ``start``/``end`` bounds matched against each article's
    PUBLISH DATE (both inclusive). Precedence:

    1. **Explicit range** ``{"start_date",[ "end_date" ]}`` (the unified trigger-form
       field) -> a targeted re-pull / backfill; end defaults to start.
    2. **Scheduled monitor** -> the SINGLE day before the run's ``logical_date`` (KST). The
       scraper crawls the CSSC list for articles published that day and parses one if it
       exists; an empty day returns ``status=empty`` (exit 0) and the load self-skips. This
       is the "check for data published yesterday, run the process if published" cadence.

    ``adjudicate`` rides through the query overlay (default False; conf can flip it to attach
    the LLM verdict to flagged articles). No floor clamp -- the daily window is always recent.
    """
    params = _conf_query_overlay(default_query, conf)

    start_date = conf.get("start_date")
    if start_date:
        end_date = conf.get("end_date") or start_date
    else:
        day = _run_point_kst().subtract(days=1).to_date_string()
        start_date = end_date = day

    params["start"] = str(start_date)
    params["end"] = str(end_date)
    return params


def _build_gacc_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, recipe: str
) -> dict[str, Any]:
    """Build gacc params: ``{start, end}`` as first-of-month ISO strings.

    gacc has the same month-window controls as kosa/kosis -- monitor default
    ``[prev .. current]``, explicit ``start_date``/``end_date`` backfill, per-recipe
    ``floor_period`` clamp -- but the recipe requires ISO ``start``/``end`` (windowed
    by month, day ignored) rather than the four-int month range. So resolve and
    floor-clamp the window in month units (reusing _resolve_period /
    clamp_period_to_floor), then format each bound as the first of its month.
    """
    params = _conf_query_overlay(default_query, conf)

    start_year, start_month, end_year, end_month = _resolve_period(conf)

    floor = load_recipe_floor(recipe)
    if floor is not None:
        clamped = clamp_period_to_floor(
            start_year, start_month, end_year, end_month, floor[0], floor[1]
        )
        if clamped is None:
            raise AirflowSkipException(
                f"{recipe}: requested range "
                f"{start_year}-{start_month:02d}..{end_year}-{end_month:02d} is "
                f"entirely below the {floor[0]}-{floor[1]:02d} floor; nothing to scrape"
            )
        if (clamped[0], clamped[1]) != (start_year, start_month):
            logger.warning(
                "%s: clamped start %d-%02d up to floor %d-%02d "
                "(requested start was below the recipe's earliest available period)",
                recipe, start_year, start_month, clamped[0], clamped[1],
            )
        start_year, start_month, end_year, end_month = clamped

    params["start"] = f"{start_year:04d}-{start_month:02d}-01"
    params["end"] = f"{end_year:04d}-{end_month:02d}-01"
    return params


def _build_full_params(
    default_query: dict[str, Any], conf: dict[str, Any]
) -> dict[str, Any]:
    """Build params for a full-series source (EIA): the query overlay, no range.

    EIA's seriesid endpoint returns the whole series in one call, so there is no range
    to resolve or floor-clamp -- the recipe is invoked with only its (empty) query and
    the fact_values MERGE dedups the re-fetched overlap across runs (the over-fetch
    model). Conf can still override a recipe's own query keys, but none of the EIA
    recipes carry any today.
    """
    return _conf_query_overlay(default_query, conf)


def _build_params(
    default_query: dict[str, Any], conf: dict[str, Any], *, source: str, recipe: str
) -> dict[str, Any]:
    """Build a recipe's request params, dispatched on the source's period kind.

    Month-grain sources (kosa, kosis) get a four-int ``[start..end]`` month range;
    date-grain sources (yfinance) get an ISO ``start_date``/``end_date`` range;
    cftc gets an ISO ``start``/``end`` date window (the weekly-COT analog of the
    date kind); petronet gets the same ISO ``start``/``end`` date window (a daily
    crude-price series); full-series sources (eia) get only their query (no range); cssc
    gets ISO ``start``/``end`` publish-date bounds (default: the single day before
    logical_date); gacc gets first-of-month ISO ``start``/``end`` resolved from a month
    window. Each branch resolves and floor-clamps the period in its own units; a source
    not listed in SOURCE_PERIOD_KIND defaults to the month convention.
    """
    kind = SOURCE_PERIOD_KIND.get(source, "month")
    if kind == "date":
        return _build_date_params(default_query, conf, recipe=recipe)
    if kind == "cftc":
        return _build_cftc_params(default_query, conf, recipe=recipe)
    if kind == "petronet":
        return _build_petronet_params(default_query, conf, recipe=recipe)
    if kind == "shfe":
        return _build_shfe_params(default_query, conf, recipe=recipe)
    if kind == "lme":
        return _build_lme_params(default_query, conf, recipe=recipe)
    if kind == "cssc":
        return _build_cssc_params(default_query, conf, recipe=recipe)
    if kind == "gacc":
        return _build_gacc_params(default_query, conf, recipe=recipe)
    if kind == "full":
        return _build_full_params(default_query, conf)
    return _build_month_params(default_query, conf, recipe=recipe)


def _is_selected(source: str, recipe: str, conf: dict[str, Any]) -> bool:
    """Honor optional conf allow-lists: {"sources": [...]} / {"recipes": [...]}.

    Absent (or empty) list means "all". Both narrow independently and are ANDed, so
    {"sources": ["kosa"], "recipes": ["kosa.eaf_steel_production"]} runs just that one.
    """

    sources = conf.get("sources")
    recipes = conf.get("recipes")
    if sources and source not in sources:
        return False
    if recipes and recipe not in recipes:
        return False
    return True


def _normalize_recipe(
    gcs_client: storage.Client,
    config: dict[str, str],
    run_id: str,
    recipe: str,
    result_uris: list[str],
) -> dict[str, Any] | None:
    """Read one recipe's result envelope(s) and stage them as wrapped NDJSON.

    The per-recipe body of the collapsed ``normalize`` task. ``result_uris`` is the
    recipe's built result objects: one for a normal windowed run, one PER BACKFILL
    YEAR for a yearly-chunked source (SOURCE_YEARLY_CHUNKED) -- the chunks'
    records concatenate, in window order, into the recipe's single staging object,
    so the combined per-source load is oblivious to chunking. Returns the staging
    descriptor, or ``None`` for the two benign no-op cases -- no materials mapping
    config (loading is opt-in) and no rows in the requested range -- so the caller
    records an in-process skip rather than spending a pod on a self-skipping task.
    Real errors propagate to the caller, which isolates them per recipe.

    BigQuery external tables need NDJSON and the records' Korean keys carry spaces, so
    each record is wrapped under an ASCII ``row`` column (see common/materials_result).
    """
    materials_config = load_materials_config(recipe)
    if materials_config is None:
        print(f"[scrape-metrics] {recipe} skipped: no materials metrics config")
        return None

    uri_prefix = f"gs://{config['bucket']}/"
    records: list[Any] = []
    scraped_count = 0
    for result_uri in result_uris:
        if not result_uri.startswith(uri_prefix):
            raise ValueError(
                f"{recipe}: result uri {result_uri!r} is outside the "
                f"configured bucket {config['bucket']!r}"
            )
        raw = (
            gcs_client.bucket(config["bucket"])
            .blob(result_uri[len(uri_prefix):])
            .download_as_bytes()
        )
        envelope = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        chunk_records = envelope_records(envelope)
        scraped_count += len(chunk_records)

        # KOSA browser recipes over-fetch the current calendar year (the scraper
        # returns the full published year-to-date as a superset of the requested
        # range and does NOT slice -- the raw YTD envelope above is the audit
        # trail). Slice back to the requested closed [start..end] month range
        # before loading; this is idempotent (a no-op for past-year requests, which
        # return the exact range) and dedups across re-runs together with the
        # fact_values MERGE. Other sources honor exact ranges and use non-monthly
        # periods, so the month-tuple slice is scoped to kosa. Per envelope, since
        # the requested range rides each envelope's own params.
        if recipe.startswith("kosa."):
            chunk_records = slice_records_to_requested_range(
                chunk_records, envelope.get("params")
            )

        records.extend(chunk_records)

    # Quarterly recipes (EIA's Venezuela crude) carry a "YYYY-Q#" period that
    # BigQuery's PARSE_DATE cannot read, so rewrite it to the quarter-start date
    # here and let the config parse it as %Y-%m-%d. Idempotent and a no-op for
    # non-quarter periods, so it is scoped by the config's time_grain.
    if materials_config["time_grain"] == "Q":
        records = normalize_quarter_periods(
            records, materials_config["period_column"]
        )

    # e-Stat rows key fields with @-prefixes and the value under "$", and carry a
    # YYYY00MMMM monthly time code BigQuery cannot PARSE_DATE. Rename the keys to plain
    # names (so the config reads "time"/"value"/"cat01" rather than fragile $."$"
    # paths), then rewrite the time code to YYYY-MM (parsed as %Y-%m). Both idempotent
    # and scoped to estat.
    if recipe.startswith("estat."):
        records = rename_estat_keys(records)
        records = normalize_estat_periods(
            records, materials_config["period_column"]
        )

    # CFTC COT rows carry the report date as an ISO timestamp
    # (``YYYY-MM-DDT00:00:00.000``); BigQuery's SAFE.PARSE_DATE would return NULL
    # on the trailing time and silently drop the row before the MERGE. Strip it to
    # the leading date so the config's %Y-%m-%d period_format parses it. Idempotent
    # and scoped to cftc.
    if recipe.startswith("cftc."):
        records = normalize_iso_timestamp_periods(
            records, materials_config["period_column"]
        )

    # SHFE daily futures emits every listed delivery-month contract per trading day;
    # the "13 metrics per measure, month-delta" reshape needs each row's contract rank
    # for the config's rank_expansion match to select each rank's row. Stamp it here
    # as the calendar month delta from the trading day (period column) to the row's
    # delivery_month, so ``_k`` metrics are fixed month offsets. No row is dropped --
    # deltas past the config's count fall through the merge unmatched. Scoped to the
    # futures recipe; the weekly-stock report is warehouse rows, not a contract curve.
    if recipe == "shfe.futures_daily":
        records = stamp_contract_ranks(
            records, period_column=materials_config["period_column"]
        )

    # LME COT emits the Number-of-Positions block as-published (one row per as-of date
    # and MiFID category); the curated commercial/investment-funds nets and the summed
    # open interest are cross-row derivations -- the DAG's job -- so append the
    # synthetic per-date market_summary row the config's match selects. Original rows
    # pass through and fall through the merge unmatched. Scoped to the COT recipe; the
    # prices/stocks reports are already row-per-period as-published.
    if recipe == "lme.aluminium_commitments_of_traders":
        records = derive_lme_cot_summary(
            records, period_column=materials_config["period_column"]
        )

    staged_count = len(records)
    # A truncated preview of the first staged record so the actual field
    # names/values reaching the load are inspectable from the log. The
    # fact_values MERGE drops rows whose ``period_column`` fails PARSE_DATE or
    # whose ``match`` keys are absent (both happen silently before the merge),
    # so seeing the real row shape here is how a "scraped but never merged"
    # recipe is diagnosed against its materials config.
    sample = (
        json.dumps(records[0], ensure_ascii=False)[:SAMPLE_PREVIEW_CHARS]
        if records
        else ""
    )
    # Greppable per-step counts (scraped -> staged). ``dropped`` is the kosa
    # over-fetch sliced off (0 for exact-range sources). Logged before the
    # empty-skip so a 0-staged run still reports what it scraped.
    print(
        f"[scrape-metrics] recipe={recipe} step=normalize "
        f"chunks={len(result_uris)} scraped={scraped_count} staged={staged_count} "
        f"dropped={scraped_count - staged_count}"
    )
    if sample:
        print(f"  sample staged row: {sample}")

    if not records:
        # No rows in range: either an empty scrape, or the requested month is
        # not published yet (the YTD chunk arrived but its in-range subset is
        # empty). Both are benign -- skip the load rather than fail it.
        print(f"[scrape-metrics] {recipe} skipped: no rows in the requested range")
        return None

    staging_object = staging_object_name(run_id, recipe)
    upload_replacing_object(
        gcs_client,
        bucket_name=config["bucket"],
        object_name=staging_object,
        data=records_to_ndjson(records),
        mime_type="application/x-ndjson",
    )

    return {
        "recipe": recipe,
        "staging_uri": gcs_uri(config["bucket"], staging_object),
        "row_count": staged_count,
        "scraped_count": scraped_count,
    }


with DAG(
    dag_id="external_data__scrape_external_data",
    description=(
        "KOSA STEEL DATA / KOSIS / yfinance 스크래핑 Cloud Run Job 실행 후 결과를 "
        "dl_materials 스타 스키마로 transform-load."
    ),
    # start_date is only the scheduler anchor for the daily cadence, NOT a data floor:
    # each KOSA metric's earliest-available period is per recipe (as far back as
    # 1979-01 for steel_scrap_domestic) and lives in its
    # configs/materials_metrics/<recipe>.json (floor_period), clamped onto the request
    # at build time (see _build_params). catchup=False keeps the scheduler from
    # auto-firing historical days on unpause -- a historical backfill is a deliberate,
    # separate run (name an explicit range in the trigger form).
    start_date=datetime(2001, 1, 1, tz="Asia/Seoul"),
    # Daily monitor: pick up each source's latest publication the day it lands. 09:00
    # UTC == 18:00 KST, after the Korean sources refresh. Empty results are now exit 0
    # (KOSIS no-data -> skip), so most days simply re-scrape the published previous
    # month as an idempotent no-op rather than failing.
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    # Trigger form (Param widgets). Scheduled runs use the defaults (the monitor
    # window); a manual run uses the form (targeted re-pull / backfill). Values are
    # read from context["params"] (see _dag_params), which also absorbs raw conf.
    params={
        "recipes": Param(
            default=ALL_RECIPES,
            type="array",
            items={"type": "string", "enum": ALL_RECIPES},
            title="Metrics to update",
            description="Multi-select. Default = all. Unselected recipes skip.",
        ),
        "start_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            title="Explicit start (optional; targeted re-pull / backfill)",
            description=(
                "ISO date. Drives both period kinds: date-grain uses it directly, "
                "month-grain derives the (year, month) bounds. Omit for the monitor "
                "window."
            ),
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            title="Explicit end (optional; defaults to start)",
        ),
        "monitor_window_months": Param(
            default=DEFAULT_MONITOR_WINDOW_MONTHS,
            type="integer",
            minimum=1,
            title="Scheduled month-grain window (months back through current)",
        ),
        "monitor_window_days": Param(
            default=DEFAULT_MONITOR_WINDOW_DAYS,
            type="integer",
            minimum=1,
            title="Scheduled date-grain window (days back through yesterday)",
        ),
        "scrape_delay_seconds": Param(
            default=None,
            type=["null", "integer"],
            minimum=0,
            title="Override per-source inter-scrape gap (kosa pacing)",
        ),
    },
    tags=[
        "external_data",
        "scraper",
        "kosa",
        "kosis",
        "yfinance",
        "eia",
        "fred",
        "estat",
        "cssc",
        "us_census",
        "ember",
        "worldstainless",
        "gacc",
        "cftc",
        "petronet",
        "gscpi",
        "eurostat",
        "abs",
        "statcan",
        "shfe",
        "cloud-run",
        "bigquery",
    ],
) as dag:

    @task()
    def build_source_requests(
        source: str, recipes: dict[str, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Build+write every recipe of one source's request in a SINGLE task.

        Building a request is pure I/O orchestration (render a params dict, PUT a
        small JSON to GCS), and injects no credential -- secrets/config env are
        resolved later in ``run_scraper`` -- so per AGENTS.md the per-recipe build
        fan-out is collapsed to one pod per source (mirroring the per-source
        ``load_bq``). The heavy, independently-retryable scrape fan-out (execute) is
        kept. ``_config()``/``get_current_context()``/the GCS client are opened once,
        then the recipes are built in-process.

        Returns a LIST of ``{recipe, request_uris}`` entries -- one per recipe that
        actually built a request, or one PER BACKFILL YEAR for a yearly-chunked
        source's multi-year window (SOURCE_YEARLY_CHUNKED; several entries then
        share a recipe) -- which is exactly the work list the unpaced
        ``execute`` dynamic-maps over (``.expand_kwargs``) and the paced serial execute
        loops. A recipe that skips (unselected, or a floor-below empty request) is
        OMITTED from the list, so it never materializes a self-skipping execute pod; a
        benign per-item skip must not fail the whole task. A genuine per-recipe error is
        recorded and the task fails ONCE at the end (retry replays the idempotent
        build), so one broken recipe stays legible without hiding the others.
        Observability is rebuilt in-process: each recipe is wrapped in a collapsible log
        group and an end-of-run summary logs the built/skipped/failed counts.
        """
        conf = _dag_params()
        context = get_current_context()
        run_id = context["run_id"]
        config = _config()
        gcs_client = storage.Client(project=config["project_id"])

        built: list[dict[str, Any]] = []
        skipped: dict[str, str] = {}
        failures: dict[str, str] = {}

        for recipe, default_query in recipes.items():
            print(f"::group::[scrape-build] {recipe}")
            try:
                # Optional allow-list: omit recipes/sources not selected for this run.
                if not _is_selected(source, recipe, conf):
                    reason = "not selected by dag_run.conf"
                    print(f"[scrape-build] {recipe} skipped: {reason}")
                    skipped[recipe] = reason
                    continue

                # _build_params raises AirflowSkipException for a floor-below empty
                # request; catch it here so the benign per-recipe skip omits this
                # recipe rather than skipping the whole (multi-recipe) task.
                try:
                    params = _build_params(
                        default_query, conf, source=source, recipe=recipe
                    )
                except AirflowSkipException as skip:
                    print(f"[scrape-build] {recipe} skipped: {skip}")
                    skipped[recipe] = str(skip)
                    continue

                # A config-declared dynamic instance keeps its file stem as the
                # pipeline identity (objects, floors, normalize, load) but the
                # payload names the scraper's parameterized recipe it rides on
                # (fred.series, yfinance.history); a static recipe names itself.
                payload_recipe = DYNAMIC_RECIPE_KEYS.get(recipe, recipe)

                # A yearly-chunked source (SOURCE_YEARLY_CHUNKED) splits a
                # multi-year date window into one request per calendar year, each
                # a separate built entry -> its own execute pod / Cloud Run
                # execution bounded to at most one year of trading days (a whole
                # backfill window in one execution exceeds the job's 1h
                # task_timeout). A single-year window -- every scheduled monitor
                # run -- keeps today's single unchunked request and object names.
                chunk_windows = (
                    split_date_range_yearly(params["start"], params["end"])
                    if source in SOURCE_YEARLY_CHUNKED
                    else None
                )
                if chunk_windows and len(chunk_windows) > 1:
                    chunked_params = [
                        (start[:4], {**params, "start": start, "end": end})
                        for start, end in chunk_windows
                    ]
                else:
                    chunked_params = [(None, params)]

                for chunk, request_params in chunked_params:
                    request = build_request_payload(payload_recipe, request_params)
                    request_object = request_object_name(run_id, recipe, chunk=chunk)
                    result_object = result_object_name(run_id, recipe, chunk=chunk)

                    # Idempotent overwrite keyed by run_id + recipe (+ chunk): a
                    # task retry re-writes the same object, and sibling recipes /
                    # chunks write to distinct objects.
                    upload_replacing_object(
                        gcs_client,
                        bucket_name=config["bucket"],
                        object_name=request_object,
                        data=json.dumps(request, ensure_ascii=False),
                        mime_type="application/json",
                    )

                    built.append(
                        {
                            "recipe": recipe,
                            "request_uris": {
                                "request_uri": gcs_uri(
                                    config["bucket"], request_object
                                ),
                                "output_uri": gcs_uri(
                                    config["bucket"], result_object
                                ),
                            },
                        }
                    )
                    via = (
                        f" via {payload_recipe}" if payload_recipe != recipe else ""
                    )
                    window_note = (
                        f" [{request_params['start']}..{request_params['end']}]"
                        if chunk
                        else ""
                    )
                    print(
                        f"[scrape-build] {recipe} built{via}{window_note} "
                        f"-> {request_object}"
                    )
            except Exception as exc:  # noqa: BLE001 -- isolate one bad recipe
                # A genuine build error: record it, keep building the rest, fail the
                # task once at the end so retries replay the whole idempotent build.
                failures[recipe] = repr(exc)
                logger.exception("[scrape-build] %s: build failed", recipe)
            finally:
                print("::endgroup::")

        print(f"::group::[scrape-build] {source} summary")
        print(
            f"[scrape-build] source={source} recipes={len(recipes)} "
            f"built={len(built)} skipped={len(skipped)} "
            f"failed={len(failures)}"
        )
        print("::endgroup::")

        if failures:
            raise RuntimeError(
                f"{source}: request build failed for {sorted(failures)}: {failures}"
            )

        return built

    @task(execution_timeout=SCRAPE_EXECUTION_TIMEOUT)
    def run_scraper(
        request_uris: dict[str, str],
        recipe: str,
        apply_delay: bool = True,
        default_delay: int = 0,
        source: str | None = None,
    ) -> dict[str, Any]:
        """Run the scraper Cloud Run Job for ONE recipe and gate on its exit.

        The genuinely heavy, externally-retryable scrape is the documented exception to
        "one pod per DAG": an UNPACED source dynamic-maps this task over exactly the
        recipes ``build_requests`` produced (``.expand_kwargs``), so one pod runs per
        built recipe, in parallel, and an unselected/floor-skipped recipe never
        materializes a self-skipping pod at all. A PACED source (kosa, yfinance) uses
        ``run_scraper_serial`` instead (one pod, serial with gaps), so ``apply_delay``
        stays False here. ADC, no conn_id.
        """
        # Dynamic mapping only materializes an instance per built recipe, so
        # ``request_uris`` is always present; keep a defensive guard rather than the
        # old per-recipe self-skip.
        if not request_uris:
            raise AirflowSkipException(
                f"{recipe}: no request built; nothing to scrape"
            )
        if apply_delay:
            # The form's scrape_delay_seconds defaults to None ("use the source's
            # default"); only an explicit override wins.
            override = _dag_params().get("scrape_delay_seconds")
            delay = int(override) if override is not None else default_delay
            if delay > 0:
                print(f"::group::inter-scrape gap: sleeping {delay}s before scrape")
                time.sleep(delay)
                print("::endgroup::")
        config = _config()
        # A source may need a credential injected from an Airflow Variable as a job env
        # override (the scraper's SCRAPE_LOCAL_CREDENTIALS path) -- EIA's api key. Read
        # at scrape time so the secret never rides on XCom; {} for sources that resolve
        # their own credentials in the job (Secret Manager) or need none.
        extra_env = _source_extra_env(source) if source else {}
        return execute_scraper_job(
            project_id=config["project_id"],
            region=config["region"],
            job_name=config["job_name"],
            request_uri=request_uris["request_uri"],
            output_uri=request_uris["output_uri"],
            extra_env=extra_env,
        )

    @task(execution_timeout=SERIAL_SCRAPE_EXECUTION_TIMEOUT)
    def run_scraper_serial(
        built_requests: list[dict[str, Any]],
        source: str,
        default_delay: int = 0,
    ) -> list[dict[str, Any]]:
        """Run a PACED source's scrapes serially in a SINGLE pod.

        kosa's origin webpage blocks request bursts, so its recipes must scrape one at
        a time with an inter-request gap. Rather than a static per-recipe execute chain
        (which materialized one self-skipping pod per unselected recipe and clogged the
        pool), collapse the whole paced source into one pod that loops its built recipes
        in order, sleeping the gap before each scrape after the first. The scrape is
        idempotent (kosa over-fetches YTD; GCS overwrite), so a coarse retry replaying
        the whole loop is safe (per AGENTS.md). Per-recipe log groups + failure
        isolation rebuild the observability the collapse gives up: one bad recipe is
        recorded and the task fails ONCE at the end without hiding the others.
        """
        if not built_requests:
            raise AirflowSkipException(
                f"{source}: no recipe built a request; nothing to scrape"
            )

        # scrape_delay_seconds defaults to None ("use the source's default"); only an
        # explicit override wins.
        override = _dag_params().get("scrape_delay_seconds")
        delay = int(override) if override is not None else default_delay

        config = _config()
        extra_env = _source_extra_env(source)

        results: list[dict[str, Any]] = []
        failures: dict[str, str] = {}
        for index, item in enumerate(built_requests):
            recipe = item["recipe"]
            request_uris = item["request_uris"]
            print(f"::group::[scrape-run] {recipe}")
            try:
                # First scrape has nothing to space from; each subsequent one waits the
                # inter-request gap so the origin webpage doesn't block the burst.
                if index > 0 and delay > 0:
                    print(f"inter-scrape gap: sleeping {delay}s before scrape")
                    time.sleep(delay)
                result = execute_scraper_job(
                    project_id=config["project_id"],
                    region=config["region"],
                    job_name=config["job_name"],
                    request_uri=request_uris["request_uri"],
                    output_uri=request_uris["output_uri"],
                    extra_env=extra_env,
                )
                results.append({"recipe": recipe, **result})
                print(f"[scrape-run] {recipe} done")
            except Exception as exc:  # noqa: BLE001 -- isolate one bad recipe
                failures[recipe] = repr(exc)
                logger.exception("[scrape-run] %s: scrape failed", recipe)
            finally:
                print("::endgroup::")

        print(f"::group::[scrape-run] {source} summary")
        print(
            f"[scrape-run] source={source} recipes={len(built_requests)} "
            f"scraped={len(results)} failed={len(failures)}"
        )
        print("::endgroup::")

        if failures:
            raise RuntimeError(
                f"{source}: scrape failed for {sorted(failures)}: {failures}"
            )
        return results

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def normalize_source_results(
        source: str, built_requests: list[dict[str, Any]]
    ) -> list[dict[str, Any] | None]:
        """Normalize+stage every built recipe of one source in a SINGLE task.

        Normalizing a result is pure I/O orchestration (download the result envelope,
        transform records in-process, upload NDJSON), so per AGENTS.md the per-recipe
        normalize fan-out is collapsed to one pod per source -- mirroring
        ``build_requests`` / ``load_bq``. This removes the per-recipe normalize pods
        (and, crucially, the ones that only rehydrated to self-skip an empty/config-less
        recipe): the two benign no-op cases (no materials config, no rows in range)
        become in-process ``None`` entries costing no pod.

        ``NONE_FAILED_MIN_ONE_SUCCESS`` mirrors ``load_bq``: a genuine scrape failure
        (a failed execute) blocks this task and surfaces rather than loading a partial
        picture, while unselected/skipped executes don't. The GCS client / config are
        opened once, then each built recipe is normalized in a collapsible log group
        with per-recipe failure isolation; the returned list (``None`` for a skipped
        recipe) is exactly what ``load_bq`` already filters and merges.

        A yearly-chunked source's backfill builds SEVERAL entries per recipe (one
        per year; SOURCE_YEARLY_CHUNKED), so the built list is first grouped back
        by recipe: all of a recipe's chunk results concatenate into its ONE staging
        object. Staging is keyed by run+recipe -- per-chunk staging writes would
        overwrite each other -- and one staging object per recipe keeps the
        combined per-source load oblivious to chunking.
        """
        if not built_requests:
            raise AirflowSkipException(
                f"{source}: no recipe built a request; nothing to normalize"
            )

        context = get_current_context()
        run_id = context["run_id"]
        config = _config()
        gcs_client = storage.Client(project=config["project_id"])

        result_uris_by_recipe: dict[str, list[str]] = {}
        for item in built_requests:
            result_uris_by_recipe.setdefault(item["recipe"], []).append(
                item["request_uris"]["output_uri"]
            )

        stagings: list[dict[str, Any] | None] = []
        staged = skipped = 0
        failures: dict[str, str] = {}
        for recipe, result_uris in result_uris_by_recipe.items():
            print(f"::group::[scrape-metrics] {recipe} normalize")
            try:
                staging = _normalize_recipe(
                    gcs_client, config, run_id, recipe, result_uris
                )
                stagings.append(staging)
                if staging is None:
                    skipped += 1
                else:
                    staged += 1
            except Exception as exc:  # noqa: BLE001 -- isolate one bad recipe
                # A genuine normalize error: record it, keep normalizing the rest, fail
                # the task once at the end so retries replay the whole idempotent
                # normalize. One broken recipe must not hide the others.
                failures[recipe] = repr(exc)
                stagings.append(None)
                logger.exception("[scrape-metrics] %s: normalize failed", recipe)
            finally:
                print("::endgroup::")

        print(f"::group::[scrape-metrics] {source} normalize summary")
        print(
            f"[scrape-metrics] source={source} step=normalize "
            f"recipes={len(result_uris_by_recipe)} chunks={len(built_requests)} "
            f"staged={staged} skipped={skipped} failed={len(failures)}"
        )
        print("::endgroup::")

        if failures:
            raise RuntimeError(
                f"{source}: normalize failed for {sorted(failures)}: {failures}"
            )

        return stagings

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_source_to_bigquery(
        source: str, stagings: list[dict[str, Any] | None]
    ) -> dict[str, Any]:
        """Transform every staged recipe of one source in a SINGLE BigQuery job.

        Collapses the source's per-recipe loads into one combined transform over the
        run's per-source staging wildcard, so the shared dl_materials dims/fact are
        MERGEd once per source per run instead of once per recipe. EIA alone fans out
        ~50 recipes; a per-recipe load fired ~50 concurrent jobs each MERGEing the 4
        shared tables, blowing past BigQuery's per-table update rate limit. One job
        per source keeps the run well under it.

        ``stagings`` is this source's collapsed normalize output; a recipe with no
        materials config or no rows in range contributes ``None`` and is filtered out
        (unselected recipes were already omitted upstream by ``build_requests``), so the
        combined job covers exactly the recipes that staged data -- which is exactly what
        the wildcard external table reads. A genuine normalize/scrape failure blocks this
        task (NONE_FAILED_MIN_ONE_SUCCESS), surfacing rather than silently loading a
        partial picture; an all-skipped source self-skips.
        """
        present = [staging for staging in stagings if staging]
        if not present:
            raise AirflowSkipException(f"{source}: no recipe staged rows to load")

        context = get_current_context()
        run_id = context["run_id"]
        config = _config()
        bq = _bq_config()

        # Resolve each staged recipe's mapping config; all share one datasource per
        # source (the combined SQL re-checks and fails loudly otherwise). Normalize
        # only stages recipes that have a config, so none of these are None.
        recipes = [staging["recipe"] for staging in present]
        configs = [load_materials_config(recipe) for recipe in recipes]
        expected_row_count = sum(int(staging["row_count"]) for staging in present)
        raw_gcs_uri = gcs_uri(
            config["bucket"], staging_source_wildcard(run_id, source)
        )

        client = bigquery.Client(project=bq["project_id"], location=bq["region"])
        started = time.monotonic()
        result = run_combined_materials_transform(
            client,
            project_id=bq["project_id"],
            dataset_id=bq["dataset_id"],
            region=bq["region"],
            raw_gcs_uri=raw_gcs_uri,
            configs=configs,
            expected_row_count=expected_row_count,
            labels=_load_labels(context, source),
            job_id_prefix=_load_job_id_prefix(context, source),
        )
        elapsed_s = round(time.monotonic() - started, 3)

        # Greppable load summary: the total staged rows vs the fact_values MERGE's
        # combined affected rows (inserts + updates -- BigQuery reports MERGE DML as
        # one number), plus a per-recipe staged breakdown and a per-statement DML
        # breakdown of the script's child jobs so each step is inspectable from the
        # task log. The merged count is the "XXXX rows" to cross-check against
        # fact_values in the BQ console.
        merged = result.get("dml_affected_rows")
        print(f"::group::[scrape-metrics] {source} load ({len(recipes)} recipes)")
        print(
            f"[scrape-metrics] source={source} step=load "
            f"recipes={len(recipes)} staged={expected_row_count} merged={merged} "
            f"job_id={result.get('job_id')} elapsed_s={elapsed_s}"
        )
        # Per-recipe staged INPUT rows. The combined job MERGEs every recipe at once
        # and reports one affected-row total (above), not a per-recipe split, so this
        # is the closest per-metric accounting the single job exposes: how many rows
        # each recipe fed in. A recipe staging 0 here never reached the merge.
        width = max((len(name) for name in recipes), default=0)
        print("  per-recipe staged:")
        for staging in present:
            print(f"    {staging['recipe']:<{width}}  staged={staging['row_count']}")
        print("  statements (in order):")
        for index, statement in enumerate(result.get("statements") or []):
            rows = statement.get("dml_affected_rows")
            print(
                f"    {index:>2} {statement.get('statement_type') or '?':<14} "
                f"job={statement.get('job_id')} "
                f"rows={'n/a' if rows is None else rows}"
            )
        print("::endgroup::")

        # Staged rows but nothing merged: the rows reached BigQuery but were dropped
        # before the fact_values MERGE -- they matched no metric (the config's
        # ``match`` keys were absent/wrong) or their ``period_column`` failed
        # PARSE_DATE (filtered by ``WHERE logical_date IS NOT NULL``). The task still
        # "succeeds", so warn loudly to make this silent-drop case visible; the fix
        # is in the recipe's materials config, cross-checked against the "sample
        # staged row" line in that recipe's normalize log.
        if expected_row_count and not merged:
            logger.warning(
                "[scrape-metrics] %s: staged %d rows but the fact_values MERGE "
                "affected %s -- rows likely failed their metric match or period "
                "PARSE_DATE and were dropped before the merge. Check each recipe's "
                "'sample staged row' (normalize log) against its materials config "
                "match keys and period_format.",
                source, expected_row_count, merged,
            )

        return {
            "source": source,
            "recipes": recipes,
            **result,
            "row_count": expected_row_count,
        }

    # Fan out by source. Each source is a TaskGroup (one origin + credential set) with
    # four steps: build_requests -> execute -> normalize -> load_bq. build, normalize
    # and load are one pod per source (pure I/O orchestration, collapsed per AGENTS.md);
    # only ``execute`` -- the genuinely heavy, externally-retryable scrape -- stays
    # fanned out, and even that materializes a pod ONLY per recipe ``build_requests``
    # actually built:
    #
    # - UNPACED sources (kosis HTTP API, eia, ...) dynamic-map ``run_scraper`` over the
    #   built list (``.expand_kwargs``), so the scrapes run in parallel and an
    #   unselected/floor-skipped recipe never becomes a self-skipping pod that clogs the
    #   pool.
    # - PACED sources (kosa, yfinance; see SOURCE_SCRAPE_DELAY_SECONDS) collapse into ONE
    #   ``run_scraper_serial`` pod that loops the built recipes in order with an
    #   inter-request gap (mapped instances can't be serialized or gapped). kosa's origin
    #   blocks request bursts; yfinance's ~30 tiny SDK scrapes are pure I/O not worth a
    #   pod each (and risk Yahoo's burst limit).
    #
    # A new source/recipe is added purely by editing SOURCES above (and adding its
    # materials config).
    for source_name, recipes in SOURCES.items():
        source_delay = _source_scrape_delay(source_name)
        paced = source_delay > 0
        with TaskGroup(group_id=source_name):
            # One build task per source (mirrors normalize / load_bq): it renders and
            # writes every selected recipe's request in a single pod, returning the LIST
            # of built {recipe, request_uris} entries -- exactly the work list execute
            # runs over, with unselected/floor-skipped recipes already omitted.
            built = build_source_requests.override(task_id="build_requests")(
                source=source_name, recipes=recipes
            )

            if paced:
                # Paced source: one pod scrapes the built recipes serially with the
                # inter-request gap (see run_scraper_serial).
                executed = run_scraper_serial.override(task_id="execute")(
                    built_requests=built,
                    source=source_name,
                    default_delay=source_delay,
                )
            else:
                # Unpaced source: one parallel scrape pod per built recipe. Constants
                # ride on .partial; the per-recipe {recipe, request_uris} entries drive
                # .expand_kwargs, so zero pods are spent on omitted recipes.
                #
                # A yearly-chunked source's backfill fans one entry per YEAR, so cap
                # its concurrently-running mapped instances: a 24-year window must
                # not burst 24 simultaneous executions at the (WAF-guarded) origin.
                override_kwargs: dict[str, Any] = {"task_id": "execute"}
                if source_name in SOURCE_YEARLY_CHUNKED:
                    override_kwargs["max_active_tis_per_dagrun"] = (
                        CHUNKED_EXECUTE_MAX_PARALLELISM
                    )
                executed = (
                    run_scraper.override(**override_kwargs)
                    .partial(source=source_name, apply_delay=False, default_delay=0)
                    .expand_kwargs(built)
                )

            # One normalize pod per source (collapsed, per AGENTS.md): it loops the
            # built recipes in-process, so a config-less / empty recipe is an in-process
            # skip rather than a self-skipping pod. It reads the built list (build edge)
            # but must also wait for the scrapes, hence the explicit execute edge.
            normalized = normalize_source_results.override(task_id="normalize")(
                source=source_name, built_requests=built
            )
            executed >> normalized

            # One combined transform-load per source: every staged recipe is merged
            # in a SINGLE BigQuery job over the run's per-source staging wildcard, so
            # the shared dl_materials dims/fact are touched once per source per run
            # rather than once per recipe (which, for EIA's ~50 recipes, tripped
            # BigQuery's per-table update rate limit). ``normalized`` is the list of
            # stagings; the task filters skipped (None) entries and self-skips when the
            # whole source staged nothing.
            load_source_to_bigquery.override(task_id="load_bq")(
                source=source_name, stagings=normalized
            )
