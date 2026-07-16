"""Pure SQL builders for the cosmetics rankings landing + star (design §1–§7).

GCP-free (mirrors ``materials_bigquery_sql``) so the whole multi-statement script is
unit-testable as strings. One script, run as a single BigQuery job over a job-scoped
temporary external table (``raw_cosmetics_records``) covering one run's staged envelope
rows, does the entire transform:

    landing (dl_products.cosmetics_rankings)   <- verbatim corpus + envelope lineage
    dw_cosmetics.dim_channel                   <- seeded (5 rows), MERGE
    dw_cosmetics.dim_brand / dim_company       <- conformed, GENERATE_UUID on insert
    dw_cosmetics.dim_ranking_list              <- rank is only meaningful within a list
    dw_cosmetics.dim_item                      <- SCD2 (contents_hash = reformulation)
    dw_cosmetics.fct_ranking_snapshot          <- grain (snapshot_ts, channel, list, item)

Everything reads a single ``CREATE TEMP TABLE stg`` projection that parses each envelope
row's JSON once into typed columns + conformance keys + hashes, so the JSON-extraction
lives in one place and the dim/fact/landing statements stay readable (the materials
``fact_candidates`` instinct).

Surrogate keys are ``GENERATE_UUID()`` minted on dim INSERT (house pattern); FKs resolve
by joining dims on their natural keys. ``dim_item`` is SCD type 2: a changed attribute
hash closes the current version and opens a new one, and the fact resolves the version
covering ``snapshot_ts`` via a temporal join. The ingredient bridge is created but left
empty — parsing is a separate versioned pass (design §6/§8).

Known v1 simplification: a natural key that reformulates **within a single load batch**
(rare; multiple snapshots of one item in one backfill window) collapses to the batch's
latest attributes under one version spanning the batch. Cross-batch reformulation opens
versions correctly. Documented rather than solved until multi-version-per-batch is needed.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from external_data.common.cosmetics_schema import CHANNELS


# Bare external-table name the script references; resolved per-job to the run's staged
# NDJSON by a temporary external table definition (see cosmetics_bigquery).
RAW_RECORDS_TABLE = "raw_cosmetics_records"

UNKNOWN_BRAND = "(unknown)"

# An stg row is an *authoritative* item observation when it carries everything its
# channel can ever know: detail channels only when the detail pass ran (html_object
# set), the detail-less naver channels always. Unenriched detail-channel rows are
# treated as "no information" for SCD2 purposes (they still land in the fact).
_DETAILLESS_CHANNELS = tuple(sorted(c for c, ch in CHANNELS.items() if not ch["has_detail"]))
_AUTHORITATIVE_EXPR = (
    "(html_object IS NOT NULL OR channel_code IN ("
    + ", ".join(f"'{c}'" for c in _DETAILLESS_CHANNELS)
    + "))"
)

# Dataset and table ids: letters, digits, underscores. BigQuery allows nothing else,
# so a hyphen here (``dl-products``) is a config bug, not a quoting problem.
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")

# Project ids are a different alphabet: they REQUIRE hyphens in practice
# (``dev-dfml-platform``) and forbid underscores. Must start with a letter and may not
# end with a hyphen. Domain-scoped legacy ids (``example.com:project``) are not
# supported; one would fail loudly here rather than produce a mis-quoted path.
_PROJECT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9]$")


def _q(identifier: str, label: str) -> str:
    if not identifier or not _IDENT_RE.fullmatch(identifier):
        raise ValueError(f"{label} must match [A-Za-z0-9_]+, got {identifier!r}")
    return identifier


def _q_project(project_id: str) -> str:
    if not project_id or not _PROJECT_RE.fullmatch(project_id):
        raise ValueError(
            f"project_id must match [A-Za-z][A-Za-z0-9-]*[A-Za-z0-9], got {project_id!r}"
        )
    return project_id


def _table(project_id: str, dataset_id: str, name: str) -> str:
    return f"`{_q_project(project_id)}.{_q(dataset_id, 'dataset_id')}.{_q(name, 'table')}`"


# --- JSON extraction over the `row` column ---------------------------------


def _jv(column: str) -> str:
    """JSON_VALUE(row, '$."col"') — scalar text; bracket notation is rejected by BQ."""
    if '"' in column or "\\" in column:
        raise ValueError(f"column has an unsupported character: {column!r}")
    return f"JSON_VALUE(row, '$.\"{column}\"')"


def _jq(column: str) -> str:
    """JSON_QUERY(row, '$."col"') — sub-tree (objects/arrays) as JSON."""
    if '"' in column or "\\" in column:
        raise ValueError(f"column has an unsupported character: {column!r}")
    return f"JSON_QUERY(row, '$.\"{column}\"')"


def _str(column: str) -> str:
    return f"NULLIF({_jv(column)}, '')"


def _num(column: str) -> str:
    return f"SAFE_CAST({_str(column)} AS NUMERIC)"


def _int(column: str) -> str:
    return f"SAFE_CAST({_str(column)} AS INT64)"


def _contents_text() -> str:
    # item_contents is a STRING for lotte/olive, a JSON array for ssgdfs, NULL for naver.
    # Coalesce so an array lands as its JSON text; the downstream parser handles either.
    return f"COALESCE({_str('item_contents')}, TO_JSON_STRING({_jq('item_contents')}))"


# --- typed staging projection ----------------------------------------------

# Attributes whose change signals a new SCD2 version of dim_item (contents handled via
# its own hash). Order is fixed so the hash is stable across runs. Each entry names a
# column the `parsed` CTE projects: the hash is computed in `keyed`, where the raw
# `row` column is already out of scope, so it must read the projections — never
# JSON_VALUE(row, ...).
_ITEM_ATTR_COLUMNS: tuple[str, ...] = (
    "brand_canonical",
    "item_nm_kor",
    "item_nm_eng",
    "item_category",
    "item_category_01",
    "item_category_02",
    "item_origin",
    "item_volume",
    "item_skin_type",
    "item_functional",
    "company_mf_norm",
    "company_mah_norm",
    "company_fit",
    "product_url",
)


def stg_projection_sql() -> str:
    """CREATE TEMP TABLE stg: one typed, deduped row per staged envelope record.

    Parses the JSON once, computes conformance keys (brand/company/ranking-list/item
    natural keys) and the SCD2 attribute + contents hashes, and dedups to the **scraper
    grain** — which includes ``category_no``, because lottedfs emits one row per
    (product, category tag) and the landing table keeps that corpus verbatim. The star
    layer's list grain is coarser (``category_no`` is a multi-valued product tag, not a
    list selector); the fact merge collapses to it, guarded by
    ``list_grain_check_sql``. Rows with no resolvable item identity are dropped here
    (and counted by the ASSERT that follows).
    """
    brand_canonical = (
        "COALESCE(NULLIF(TRIM(" + _str("brand_nm_kor") + "), ''), "
        "NULLIF(TRIM(" + _str("brand_nm_eng") + "), ''), '" + UNKNOWN_BRAND + "')"
    )
    company_mf_norm = f"LOWER(TRIM({_str('company_mf')}))"
    company_mah_norm = f"LOWER(TRIM({_str('company_mah')}))"
    item_nk = f"COALESCE({_str('item_cd')}, {_str('prd_no')})"
    contents_text = _contents_text()

    attr_concat = ", ".join(f"IFNULL(CAST({col} AS STRING), '')" for col in _ITEM_ATTR_COLUMNS)

    return f"""CREATE TEMP TABLE stg AS
WITH parsed AS (
  SELECT
    NULLIF({_jv('source')}, '')                        AS channel_code,
    {_str('__snapshot_id')}                            AS snapshot_id,
    {_str('__envelope_uri')}                           AS envelope_uri,
    SAFE_CAST({_str('__scraped_at')} AS TIMESTAMP)     AS snapshot_ts,
    {_str('category')}                                 AS category,
    {_str('age_grp')}                                  AS age_grp,
    {_str('category_no')}                              AS category_no,
    {_int('page_no')}                                  AS page_no,
    {_int('rank')}                                     AS rank,
    {_str('brand_nm_kor')}                             AS brand_nm_kor,
    {_str('brand_nm_eng')}                             AS brand_nm_eng,
    {brand_canonical}                                  AS brand_canonical,
    {_str('item_cd')}                                  AS item_cd,
    {_str('ref_cd')}                                   AS ref_cd,
    {_str('prd_no')}                                   AS prd_no,
    {_str('prd_opt_no')}                               AS prd_opt_no,
    {item_nk}                                          AS item_nk,
    {_str('item_nm_kor')}                              AS item_nm_kor,
    {_str('item_nm_eng')}                              AS item_nm_eng,
    {_str('item_category')}                            AS item_category,
    {_str('item_category_01')}                         AS item_category_01,
    {_str('item_category_02')}                         AS item_category_02,
    {_str('item_origin')}                              AS item_origin,
    {contents_text}                                    AS item_contents,
    {_str('item_skin_type')}                           AS item_skin_type,
    {_str('item_volume')}                              AS item_volume,
    {_str('item_functional')}                          AS item_functional,
    {_num('item_score')}                               AS item_score,
    {_int('item_review_cnt')}                          AS item_review_cnt,
    {_str('item_sales_tag')}                           AS item_sales_tag,
    {_num('price_usd')}                                AS price_usd,
    {_num('price_krw')}                                AS price_krw,
    {_num('price_origin_usd')}                         AS price_origin_usd,
    {_num('price_origin_krw')}                         AS price_origin_krw,
    {_num('discount_rate')}                            AS discount_rate,
    {_str('company_mf')}                               AS company_mf,
    {_str('company_mah')}                              AS company_mah,
    {company_mf_norm}                                  AS company_mf_norm,
    {company_mah_norm}                                 AS company_mah_norm,
    {_str('company_fit')}                              AS company_fit,
    {_str('product_url')}                              AS product_url,
    {_str('html_object')}                              AS html_object,
    {_jq('_extra')}                                    AS extra_json
  FROM {RAW_RECORDS_TABLE}
),
keyed AS (
  SELECT
    *,
    DATE(snapshot_ts)                                                          AS snapshot_date,
    (price_origin_krw - price_krw)                                             AS discount_amount_krw,
    TO_HEX(SHA256(IFNULL(item_contents, '')))                                  AS contents_hash,
    TO_HEX(SHA256(CONCAT({attr_concat})))                                      AS attr_hash
  FROM parsed
  WHERE item_nk IS NOT NULL AND channel_code IS NOT NULL AND snapshot_ts IS NOT NULL
)
SELECT * FROM keyed
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_ts, channel_code, category, age_grp, category_no, item_nk
  ORDER BY rank
) = 1"""


# --- DDL (CREATE TABLE IF NOT EXISTS; never CREATE OR REPLACE) --------------


def create_tables_ddl(project_id: str, landing_dataset_id: str, star_dataset_id: str) -> str:
    """Idempotent bootstrap for the landing table and every star table.

    The dataset(s) are provisioned in deepfl-infra; the DAG owns the tables (matches the
    dl_materials split). ``CREATE TABLE IF NOT EXISTS`` so a fresh dataset self-bootstraps
    and steady runs no-op; never ``CREATE OR REPLACE`` (fail-fast on drift, house rule).
    """
    landing = _table(project_id, landing_dataset_id, "cosmetics_rankings")
    T = lambda name: _table(project_id, star_dataset_id, name)  # noqa: E731

    # Each CREATE is ``;``-terminated (a BigQuery multi-statement script needs a
    # terminator between statements), matching materials_bigquery_sql.create_tables_ddl.
    return ";\n\n".join(
        [
            f"""CREATE TABLE IF NOT EXISTS {landing} (
  snapshot_id STRING, snapshot_ts TIMESTAMP, snapshot_date DATE, envelope_uri STRING,
  channel_code STRING, category STRING, age_grp STRING, category_no STRING,
  page_no INT64, rank INT64,
  brand_nm_kor STRING, brand_nm_eng STRING,
  item_cd STRING, ref_cd STRING, prd_no STRING, prd_opt_no STRING,
  item_nm_kor STRING, item_nm_eng STRING,
  item_category STRING, item_category_01 STRING, item_category_02 STRING,
  item_origin STRING, item_contents STRING, item_skin_type STRING, item_volume STRING,
  item_functional STRING, item_score NUMERIC, item_review_cnt INT64, item_sales_tag STRING,
  price_usd NUMERIC, price_krw NUMERIC, price_origin_usd NUMERIC, price_origin_krw NUMERIC,
  discount_rate NUMERIC, company_mf STRING, company_mah STRING, company_fit STRING,
  product_url STRING, html_object STRING, extra JSON, loaded_at TIMESTAMP
)
PARTITION BY snapshot_date
CLUSTER BY channel_code, item_cd""",
            f"""CREATE TABLE IF NOT EXISTS {T('dim_channel')} (
  channel_sk STRING, channel_code STRING, channel_type STRING,
  currency_quoted STRING, audience STRING
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('dim_brand')} (
  brand_sk STRING, brand_canonical STRING, brand_nm_kor STRING, brand_nm_eng STRING,
  first_seen DATE
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('dim_company')} (
  company_sk STRING, company_name STRING, company_name_normalized STRING, first_seen DATE
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('dim_ranking_list')} (
  ranking_list_sk STRING, channel_sk STRING, channel_code STRING,
  category STRING, age_grp STRING, category_canonical STRING
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('dim_item')} (
  item_sk STRING, channel_code STRING, item_cd STRING, prd_no STRING, ref_cd STRING,
  brand_sk STRING, brand_canonical STRING,
  item_nm_kor STRING, item_nm_eng STRING,
  item_category STRING, item_category_01 STRING, item_category_02 STRING,
  item_origin STRING, item_volume STRING, item_skin_type STRING, item_functional STRING,
  company_mf_sk STRING, company_mah_sk STRING, company_fit STRING,
  product_url STRING, item_contents STRING, contents_hash STRING, attr_hash STRING,
  product_id STRING,
  valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOL,
  first_seen DATE, last_seen DATE
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('dim_ingredient')} (
  ingredient_sk STRING, name_kor STRING, name_inci STRING
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('br_item_ingredient')} (
  item_sk STRING, ingredient_sk STRING, position INT64, parser_version STRING
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('brand_source_map')} (
  channel_code STRING, brand_source_raw STRING, brand_sk STRING
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('ingredient_alias')} (
  alias STRING, ingredient_sk STRING
)""",
            f"""CREATE TABLE IF NOT EXISTS {T('fct_ranking_snapshot')} (
  snapshot_date DATE, snapshot_ts TIMESTAMP,
  channel_sk STRING, ranking_list_sk STRING, item_sk STRING, brand_sk STRING,
  rank INT64, page_no INT64, item_score NUMERIC, item_review_cnt INT64,
  price_krw NUMERIC, price_origin_krw NUMERIC, price_usd NUMERIC, price_origin_usd NUMERIC,
  discount_amount_krw NUMERIC, discount_rate NUMERIC,
  item_sales_tag STRING, snapshot_id STRING, envelope_uri STRING
)
PARTITION BY snapshot_date
CLUSTER BY channel_sk, brand_sk""",
        ]
    ) + ";"


# --- seeds & merges --------------------------------------------------------


def dim_channel_merge_sql(project_id: str, star_dataset_id: str) -> str:
    """Seed the five channels by hand (design §4.1); idempotent MERGE on channel_code."""
    rows = []
    for code, meta in CHANNELS.items():
        rows.append(
            "STRUCT('{code}' AS channel_code, '{ctype}' AS channel_type, "
            "'{cur}' AS currency_quoted, '{aud}' AS audience)".format(
                code=code,
                ctype=meta["channel_type"],
                cur=meta["currency_quoted"],
                aud=meta["audience"],
            )
        )
    values = ",\n    ".join(rows)
    channel = _table(project_id, star_dataset_id, "dim_channel")
    return f"""MERGE {channel} AS target
USING (
  SELECT * FROM UNNEST([
    {values}
  ])
) AS source
ON target.channel_code = source.channel_code
WHEN MATCHED THEN UPDATE SET
  channel_type = source.channel_type,
  currency_quoted = source.currency_quoted,
  audience = source.audience
WHEN NOT MATCHED THEN INSERT (channel_sk, channel_code, channel_type, currency_quoted, audience)
  VALUES (GENERATE_UUID(), source.channel_code, source.channel_type,
          source.currency_quoted, source.audience)"""


def dim_brand_merge_sql(project_id: str, star_dataset_id: str) -> str:
    brand = _table(project_id, star_dataset_id, "dim_brand")
    return f"""MERGE {brand} AS target
USING (
  SELECT brand_canonical,
         ANY_VALUE(brand_nm_kor) AS brand_nm_kor,
         ANY_VALUE(brand_nm_eng) AS brand_nm_eng,
         MIN(snapshot_date) AS first_seen
  FROM stg
  GROUP BY brand_canonical
) AS source
ON target.brand_canonical = source.brand_canonical
WHEN NOT MATCHED THEN INSERT (brand_sk, brand_canonical, brand_nm_kor, brand_nm_eng, first_seen)
  VALUES (GENERATE_UUID(), source.brand_canonical, source.brand_nm_kor,
          source.brand_nm_eng, source.first_seen)"""


def dim_company_merge_sql(project_id: str, star_dataset_id: str) -> str:
    company = _table(project_id, star_dataset_id, "dim_company")
    return f"""MERGE {company} AS target
USING (
  SELECT company_name_normalized,
         ANY_VALUE(company_name) AS company_name,
         MIN(snapshot_date) AS first_seen
  FROM (
    SELECT company_mf_norm AS company_name_normalized, company_mf AS company_name, snapshot_date
    FROM stg WHERE company_mf_norm IS NOT NULL AND company_mf_norm != ''
    UNION ALL
    SELECT company_mah_norm, company_mah, snapshot_date
    FROM stg WHERE company_mah_norm IS NOT NULL AND company_mah_norm != ''
  )
  GROUP BY company_name_normalized
) AS source
ON target.company_name_normalized = source.company_name_normalized
WHEN NOT MATCHED THEN INSERT (company_sk, company_name, company_name_normalized, first_seen)
  VALUES (GENERATE_UUID(), source.company_name, source.company_name_normalized, source.first_seen)"""


def dim_ranking_list_merge_sql(project_id: str, star_dataset_id: str) -> str:
    """List natural key is (channel, category, age_grp) — NOT category_no.

    ``category_no`` is a multi-valued product tag (a lottedfs product carries ~5
    ``nrmCatNo`` values, each duplicating the row with the same list-global rank), so
    keying on it fragments one 150-item list into ~25 overlapping pseudo-lists and fans
    the fact out per tag. The tag stays in landing verbatim; a ``br_item_category``
    bridge is the star-layer home if it is ever needed analytically.
    """
    rl = _table(project_id, star_dataset_id, "dim_ranking_list")
    channel = _table(project_id, star_dataset_id, "dim_channel")
    return f"""MERGE {rl} AS target
USING (
  SELECT s.channel_code,
         IFNULL(s.category, '') AS category,
         IFNULL(s.age_grp, '') AS age_grp,
         c.channel_sk
  FROM (SELECT DISTINCT channel_code, category, age_grp FROM stg) s
  JOIN {channel} c ON c.channel_code = s.channel_code
  GROUP BY channel_code, category, age_grp, c.channel_sk
) AS source
ON  target.channel_code = source.channel_code
AND IFNULL(target.category, '') = source.category
AND IFNULL(target.age_grp, '') = source.age_grp
WHEN NOT MATCHED THEN INSERT
  (ranking_list_sk, channel_sk, channel_code, category, age_grp, category_canonical)
  VALUES (GENERATE_UUID(), source.channel_sk, source.channel_code,
          NULLIF(source.category, ''), NULLIF(source.age_grp, ''), NULL)"""


def _item_incoming_cte(project_id: str, star_dataset_id: str) -> str:
    """One representative attribute row per (channel_code, item_nk) in the batch.

    Attributes come from the latest **authoritative** snapshot in the batch: a detail
    channel's row counts only when it was detail-passed (``html_object`` set), while the
    detail-less naver channels are always authoritative. An observation that merely
    lacked the detail pass (interrupted prototype run, flaky detail scrape) is "no
    information", not "attributes became null", so it never outranks an enriched sighting.
    valid_from is the earliest snapshot in the batch so one version spans the whole
    load (fact rows for earlier same-batch snapshots still resolve). brand/company SKs
    resolved by natural-key join.
    """
    brand = _table(project_id, star_dataset_id, "dim_brand")
    company = _table(project_id, star_dataset_id, "dim_company")
    return f"""item_incoming AS (
  SELECT
    a.channel_code, a.item_nk, a.item_cd, a.prd_no, a.ref_cd,
    b.brand_sk, a.brand_canonical,
    a.item_nm_kor, a.item_nm_eng, a.item_category, a.item_category_01, a.item_category_02,
    a.item_origin, a.item_volume, a.item_skin_type, a.item_functional,
    cmf.company_sk AS company_mf_sk, cmah.company_sk AS company_mah_sk, a.company_fit,
    a.product_url, a.item_contents, a.contents_hash, a.attr_hash, a.is_authoritative,
    a.valid_from, a.last_seen, a.first_seen
  FROM (
    SELECT
      channel_code, item_nk, item_cd, prd_no, ref_cd, brand_canonical,
      item_nm_kor, item_nm_eng, item_category, item_category_01, item_category_02,
      item_origin, item_volume, item_skin_type, item_functional,
      company_mf_norm, company_mah_norm, company_fit, product_url, item_contents,
      contents_hash, attr_hash,
      {_AUTHORITATIVE_EXPR} AS is_authoritative,
      MIN(snapshot_ts) OVER (PARTITION BY channel_code, item_nk) AS valid_from,
      MIN(snapshot_date) OVER (PARTITION BY channel_code, item_nk) AS first_seen,
      MAX(snapshot_date) OVER (PARTITION BY channel_code, item_nk) AS last_seen
    FROM stg
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY channel_code, item_nk
      ORDER BY {_AUTHORITATIVE_EXPR} DESC, snapshot_ts DESC) = 1
  ) a
  LEFT JOIN {brand} b ON b.brand_canonical = a.brand_canonical
  LEFT JOIN {company} cmf ON cmf.company_name_normalized = a.company_mf_norm
  LEFT JOIN {company} cmah ON cmah.company_name_normalized = a.company_mah_norm
)"""


def dim_item_expire_sql(project_id: str, star_dataset_id: str) -> str:
    """SCD2 step 1: close the current version whose attribute hash changed (design §4.3).

    Only an authoritative observation may close a version: a batch whose best sighting of
    an item is unenriched (detail pass never ran) must not downgrade an enriched current
    version to null attributes. Trade-off: a genuine attribute change observed only via
    unenriched rows waits for the next enriched sighting to open its version.
    """
    item = _table(project_id, star_dataset_id, "dim_item")
    return f"""MERGE {item} AS target
USING (
  WITH {_item_incoming_cte(project_id, star_dataset_id)}
  SELECT * FROM item_incoming
) AS source
ON  target.channel_code = source.channel_code
AND IFNULL(target.item_cd, target.prd_no) = source.item_nk
AND target.is_current
WHEN MATCHED AND target.attr_hash != source.attr_hash AND source.is_authoritative THEN UPDATE SET
  valid_to = source.valid_from,
  is_current = FALSE"""


def dim_item_insert_sql(project_id: str, star_dataset_id: str) -> str:
    """SCD2 step 2: insert brand-new items and freshly-opened versions.

    Runs after the expire MERGE, so a changed item has no current row and is inserted as a
    new version; an unchanged item still has its current row and is skipped; last_seen on
    the surviving current row is advanced.
    """
    item = _table(project_id, star_dataset_id, "dim_item")
    return f"""INSERT INTO {item} (
  item_sk, channel_code, item_cd, prd_no, ref_cd, brand_sk, brand_canonical,
  item_nm_kor, item_nm_eng, item_category, item_category_01, item_category_02,
  item_origin, item_volume, item_skin_type, item_functional,
  company_mf_sk, company_mah_sk, company_fit, product_url, item_contents,
  contents_hash, attr_hash, product_id, valid_from, valid_to, is_current, first_seen, last_seen
)
WITH {_item_incoming_cte(project_id, star_dataset_id)}
SELECT
  GENERATE_UUID(), i.channel_code, i.item_cd, i.prd_no, i.ref_cd, i.brand_sk, i.brand_canonical,
  i.item_nm_kor, i.item_nm_eng, i.item_category, i.item_category_01, i.item_category_02,
  i.item_origin, i.item_volume, i.item_skin_type, i.item_functional,
  i.company_mf_sk, i.company_mah_sk, i.company_fit, i.product_url, i.item_contents,
  i.contents_hash, i.attr_hash, CAST(NULL AS STRING), i.valid_from,
  CAST(NULL AS TIMESTAMP), TRUE, i.first_seen, i.last_seen
FROM item_incoming i
LEFT JOIN {item} d
  ON d.channel_code = i.channel_code
 AND IFNULL(d.item_cd, d.prd_no) = i.item_nk
 AND d.is_current
WHERE d.item_sk IS NULL"""


def dim_item_touch_sql(project_id: str, star_dataset_id: str) -> str:
    """Advance last_seen on the surviving current version of unchanged items."""
    item = _table(project_id, star_dataset_id, "dim_item")
    return f"""MERGE {item} AS target
USING (
  WITH {_item_incoming_cte(project_id, star_dataset_id)}
  SELECT * FROM item_incoming
) AS source
ON  target.channel_code = source.channel_code
AND IFNULL(target.item_cd, target.prd_no) = source.item_nk
AND target.is_current
WHEN MATCHED AND source.last_seen > target.last_seen THEN UPDATE SET
  last_seen = source.last_seen"""


def landing_merge_sql(project_id: str, landing_dataset_id: str) -> str:
    """Upsert the verbatim corpus into landing, keyed on the scraper grain (idempotent).

    The key keeps ``category_no``: landing preserves lottedfs's one-row-per-(product,
    category tag) output verbatim, which is what makes the tag recoverable for a future
    ``br_item_category`` bridge. Only the star layer collapses it.
    """
    landing = _table(project_id, landing_dataset_id, "cosmetics_rankings")
    cols = [
        "snapshot_id", "snapshot_ts", "snapshot_date", "envelope_uri", "channel_code",
        "category", "age_grp", "category_no", "page_no", "rank", "brand_nm_kor",
        "brand_nm_eng", "item_cd", "ref_cd", "prd_no", "prd_opt_no", "item_nm_kor",
        "item_nm_eng", "item_category", "item_category_01", "item_category_02",
        "item_origin", "item_contents", "item_skin_type", "item_volume", "item_functional",
        "item_score", "item_review_cnt", "item_sales_tag", "price_usd", "price_krw",
        "price_origin_usd", "price_origin_krw", "discount_rate", "company_mf", "company_mah",
        "company_fit", "product_url", "html_object",
    ]
    insert_cols = ", ".join(cols) + ", extra, loaded_at"
    insert_vals = ", ".join(f"source.{c}" for c in cols) + ", source.extra_json, CURRENT_TIMESTAMP()"
    updates = ",\n  ".join(f"{c} = source.{c}" for c in cols if c not in ("snapshot_ts", "snapshot_date"))
    return f"""MERGE {landing} AS target
USING stg AS source
ON  target.snapshot_ts = source.snapshot_ts
AND target.channel_code = source.channel_code
AND IFNULL(target.category, '') = IFNULL(source.category, '')
AND IFNULL(target.age_grp, '') = IFNULL(source.age_grp, '')
AND IFNULL(target.category_no, '') = IFNULL(source.category_no, '')
AND IFNULL(target.item_cd, target.prd_no) = source.item_nk
WHEN MATCHED THEN UPDATE SET
  {updates},
  extra = source.extra_json,
  loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"""


def fact_merge_sql(project_id: str, star_dataset_id: str) -> str:
    """MERGE the fact keyed on the full grain; FKs resolved by dim natural-key joins.

    The source collapses stg from scraper grain to the list grain — one row per
    ``(snapshot_ts, channel, category, age_grp, item_nk)`` — because lottedfs repeats a
    product once per ``category_no`` tag with the same rank; without the collapse every
    fact measure fans out per tag. Safe because ``list_grain_check_sql`` already
    asserted the duplicates agree on rank; the survivor is picked by lowest tag purely
    for determinism.

    ``item_sk`` is the SCD2 version whose validity window covers ``snapshot_ts`` (temporal
    join), so "which formulation was ranked that week" is answerable. ``discount_amount_krw``
    is the additive component; ``discount_rate`` rides along non-additive (design §2 additivity).
    """
    fact = _table(project_id, star_dataset_id, "fct_ranking_snapshot")
    channel = _table(project_id, star_dataset_id, "dim_channel")
    rl = _table(project_id, star_dataset_id, "dim_ranking_list")
    item = _table(project_id, star_dataset_id, "dim_item")
    brand = _table(project_id, star_dataset_id, "dim_brand")
    return f"""MERGE {fact} AS target
USING (
  SELECT
    s.snapshot_date, s.snapshot_ts,
    c.channel_sk, rl.ranking_list_sk, di.item_sk, b.brand_sk,
    s.rank, s.page_no, s.item_score, s.item_review_cnt,
    s.price_krw, s.price_origin_krw, s.price_usd, s.price_origin_usd,
    s.discount_amount_krw, s.discount_rate,
    s.item_sales_tag, s.snapshot_id, s.envelope_uri
  FROM (
    SELECT * FROM stg
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY snapshot_ts, channel_code, category, age_grp, item_nk
      ORDER BY IFNULL(category_no, '')
    ) = 1
  ) s
  JOIN {channel} c ON c.channel_code = s.channel_code
  JOIN {rl} rl
    ON  rl.channel_code = s.channel_code
    AND IFNULL(rl.category, '') = IFNULL(s.category, '')
    AND IFNULL(rl.age_grp, '') = IFNULL(s.age_grp, '')
  LEFT JOIN {brand} b ON b.brand_canonical = s.brand_canonical
  LEFT JOIN {item} di
    ON  di.channel_code = s.channel_code
    AND IFNULL(di.item_cd, di.prd_no) = s.item_nk
    AND s.snapshot_ts >= di.valid_from
    AND (di.valid_to IS NULL OR s.snapshot_ts < di.valid_to)
) AS source
ON  target.snapshot_ts = source.snapshot_ts
AND target.channel_sk = source.channel_sk
AND target.ranking_list_sk = source.ranking_list_sk
AND target.item_sk = source.item_sk
WHEN MATCHED THEN UPDATE SET
  snapshot_date = source.snapshot_date, brand_sk = source.brand_sk,
  rank = source.rank, page_no = source.page_no,
  item_score = source.item_score, item_review_cnt = source.item_review_cnt,
  price_krw = source.price_krw, price_origin_krw = source.price_origin_krw,
  price_usd = source.price_usd, price_origin_usd = source.price_origin_usd,
  discount_amount_krw = source.discount_amount_krw, discount_rate = source.discount_rate,
  item_sales_tag = source.item_sales_tag,
  snapshot_id = source.snapshot_id, envelope_uri = source.envelope_uri
WHEN NOT MATCHED THEN INSERT (
  snapshot_date, snapshot_ts, channel_sk, ranking_list_sk, item_sk, brand_sk,
  rank, page_no, item_score, item_review_cnt, price_krw, price_origin_krw,
  price_usd, price_origin_usd, discount_amount_krw, discount_rate,
  item_sales_tag, snapshot_id, envelope_uri
) VALUES (
  source.snapshot_date, source.snapshot_ts, source.channel_sk, source.ranking_list_sk,
  source.item_sk, source.brand_sk, source.rank, source.page_no, source.item_score,
  source.item_review_cnt, source.price_krw, source.price_origin_krw, source.price_usd,
  source.price_origin_usd, source.discount_amount_krw, source.discount_rate,
  source.item_sales_tag, source.snapshot_id, source.envelope_uri
)"""


def list_grain_check_sql() -> str:
    """ASSERT that collapsing category_no duplicates onto the list grain is lossless.

    The fact merge keeps one stg row per ``(snapshot_ts, channel, category, age_grp,
    item_nk)``; that is only sound while a product's rows differ solely in their
    ``category_no`` tag. If a product ever carries two different ranks under two tags,
    "its rank in the list" is undefined and the load must fail loudly here — before any
    table is written — rather than ship whichever rank the dedup happened to keep.
    """
    return (
        "ASSERT NOT EXISTS (\n"
        "  SELECT 1 FROM stg\n"
        "  GROUP BY snapshot_ts, channel_code, category, age_grp, item_nk\n"
        "  HAVING COUNT(DISTINCT rank) > 1\n"
        ") AS 'category_no duplicates disagree on rank; list grain "
        "(channel, category, age_grp) is unsound for this batch';"
    )


def raw_records_check_sql(expected_row_count: int | None = None) -> str:
    """Row-count probe or ASSERT over the staged records (post-identity-filter)."""
    if expected_row_count is None:
        return "SELECT COUNT(*) AS row_count FROM stg;"
    if not isinstance(expected_row_count, int) or expected_row_count < 0:
        raise ValueError("expected_row_count must be a non-negative integer or None")
    return (
        f"ASSERT (SELECT COUNT(*) FROM stg) = {expected_row_count} AS "
        f"'staged cosmetics rows != expected {expected_row_count}';"
    )


def fct_item_daily_view_sql(project_id: str, star_dataset_id: str) -> str:
    """Derived feature view: latest snapshot per day + flow features (design §5).

    Kept out of the base fact so per-snapshot reloads stay idempotent. ``review_cnt_delta``
    is NULL on an item's first appearance and biased low across chart exits/re-entries —
    documented, not imputed.
    """
    view = _table(project_id, star_dataset_id, "fct_item_daily")
    fact = _table(project_id, star_dataset_id, "fct_ranking_snapshot")
    return f"""CREATE OR REPLACE VIEW {view} AS
WITH latest AS (
  SELECT * FROM {fact}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY snapshot_date, channel_sk, ranking_list_sk, item_sk
    ORDER BY snapshot_ts DESC) = 1
)
SELECT
  latest.*,
  item_review_cnt - LAG(item_review_cnt) OVER w AS review_cnt_delta,
  LAG(rank) OVER w - rank                        AS rank_delta,
  COUNT(*) OVER (PARTITION BY channel_sk, ranking_list_sk, item_sk
                 ORDER BY snapshot_date
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS days_on_chart
FROM latest
WINDOW w AS (PARTITION BY channel_sk, ranking_list_sk, item_sk ORDER BY snapshot_date)"""


# --- whole-script assembly -------------------------------------------------


def combined_cosmetics_transform_sql(
    *,
    project_id: str,
    landing_dataset_id: str,
    star_dataset_id: str,
    expected_row_count: int | None = None,
) -> str:
    """Assemble the whole transform as one multi-statement script (one BigQuery job).

    Order: bootstrap DDL -> stg projection -> row check -> list-grain guard -> landing
    MERGE -> conformed dims (channel, brand, company, ranking_list) -> SCD2 dim_item
    (expire, insert, touch) -> fact MERGE -> derived view. The guard sits before every
    MERGE so an unsound batch writes nothing at all; dims precede the fact because the
    fact resolves their SKs; brand/company precede dim_item because item versions carry
    their FKs.
    """
    statements = [
        create_tables_ddl(project_id, landing_dataset_id, star_dataset_id),
        stg_projection_sql() + ";",
        raw_records_check_sql(expected_row_count),
        list_grain_check_sql(),
        landing_merge_sql(project_id, landing_dataset_id) + ";",
        dim_channel_merge_sql(project_id, star_dataset_id) + ";",
        dim_brand_merge_sql(project_id, star_dataset_id) + ";",
        dim_company_merge_sql(project_id, star_dataset_id) + ";",
        dim_ranking_list_merge_sql(project_id, star_dataset_id) + ";",
        dim_item_expire_sql(project_id, star_dataset_id) + ";",
        dim_item_insert_sql(project_id, star_dataset_id) + ";",
        dim_item_touch_sql(project_id, star_dataset_id) + ";",
        fact_merge_sql(project_id, star_dataset_id) + ";",
        fct_item_daily_view_sql(project_id, star_dataset_id) + ";",
    ]
    return "\n\n".join(statements)
