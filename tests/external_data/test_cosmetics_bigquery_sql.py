"""Structural tests for the cosmetics landing + star SQL builders.

Substring / ordering / count assertions (the materials_bigquery_sql style); no live
BigQuery. A dry-run against dev BigQuery is the pre-ship gate these cannot replace.
"""

from __future__ import annotations

import re

import pytest

from external_data.common import cosmetics_bigquery_sql as sql
from external_data.common.cosmetics_schema import CHANNELS


PROJ, LAND, STAR = "proj", "dl_products", "dw_cosmetics"


def _script(expected=None):
    return sql.combined_cosmetics_transform_sql(
        project_id=PROJ, landing_dataset_id=LAND, star_dataset_id=STAR,
        expected_row_count=expected,
    )


# --- assembly & ordering ---------------------------------------------------


def test_statement_order_dims_before_fact_brand_before_item():
    s = _script()
    # bootstrap DDL first
    assert s.index("CREATE TABLE IF NOT EXISTS") < s.index("CREATE TEMP TABLE stg")
    # landing + dims merge before the fact merge
    assert s.index("MERGE `proj.dl_products.cosmetics_rankings`") < s.index("fct_ranking_snapshot` AS target")
    assert s.index("MERGE `proj.dw_cosmetics.dim_brand`") < s.index("`proj.dw_cosmetics.dim_item` AS target")
    assert s.index("MERGE `proj.dw_cosmetics.dim_company`") < s.index("`proj.dw_cosmetics.dim_item` AS target")
    assert s.index("`proj.dw_cosmetics.dim_item`") < s.index("fct_ranking_snapshot` AS target")


def test_landing_and_star_tables_created_if_not_exists():
    ddl = sql.create_tables_ddl(PROJ, LAND, STAR)
    for name in ("cosmetics_rankings",):
        assert f"CREATE TABLE IF NOT EXISTS `proj.dl_products.{name}`" in ddl
    for name in ("dim_channel", "dim_brand", "dim_company", "dim_ranking_list", "dim_item",
                 "dim_ingredient", "br_item_ingredient", "fct_ranking_snapshot"):
        assert f"CREATE TABLE IF NOT EXISTS `proj.dw_cosmetics.{name}`" in ddl
    # never destructive on tables (fail-fast on drift, house rule)
    assert "CREATE OR REPLACE TABLE" not in ddl


def test_fact_and_landing_partitioned_and_clustered():
    ddl = sql.create_tables_ddl(PROJ, LAND, STAR)
    assert "PARTITION BY snapshot_date" in ddl
    assert "CLUSTER BY channel_sk, brand_sk" in ddl  # fact
    assert "CLUSTER BY channel_code, item_cd" in ddl  # landing


# --- staging projection ----------------------------------------------------


def test_stg_reads_json_with_dot_quote_paths_not_brackets():
    stg = sql.stg_projection_sql()
    assert "JSON_VALUE(row, '$.\"price_krw\"')" in stg
    assert '$["' not in stg  # bracket notation is rejected by BigQuery
    # lineage keys the loader stamps
    assert '$."__snapshot_id"' in stg and '$."__scraped_at"' in stg


def test_stg_contents_coalesces_string_and_array_forms():
    stg = sql.stg_projection_sql()
    # item_contents is a string (lotte/olive) or a JSON array (ssgdfs); both -> text
    assert "TO_JSON_STRING(JSON_QUERY(row, '$.\"item_contents\"'))" in stg


def test_stg_computes_additive_discount_amount_and_hashes():
    stg = sql.stg_projection_sql()
    assert "(price_origin_krw - price_krw)" in stg
    assert "AS discount_amount_krw" in stg
    assert "AS attr_hash" in stg and "AS contents_hash" in stg


def test_stg_raw_row_extraction_stays_inside_parsed():
    # The raw `row` column exists only in the external table read by the `parsed` CTE;
    # any JSON_VALUE/JSON_QUERY(row, ...) after its FROM is out of scope and fails at
    # runtime only ("Unrecognized name: row" — dry-run does not analyze script statements).
    stg = sql.stg_projection_sql()
    _, after_parsed = stg.split(f"FROM {sql.RAW_RECORDS_TABLE}", 1)
    assert "JSON_VALUE(row" not in after_parsed
    assert "JSON_QUERY(row" not in after_parsed


def test_stg_attr_hash_reads_projected_columns():
    stg = sql.stg_projection_sql()
    keyed = stg.split("keyed AS (", 1)[1]
    for col in sql._ITEM_ATTR_COLUMNS:
        assert f"IFNULL(CAST({col} AS STRING), '')" in keyed


def test_stg_dedups_on_scraper_grain_keeping_category_no():
    # stg keeps category_no in its grain: landing preserves lottedfs's one row per
    # (product, category tag) verbatim. Only the star layer collapses the tag.
    stg = sql.stg_projection_sql()
    assert "QUALIFY ROW_NUMBER() OVER (" in stg
    assert "PARTITION BY snapshot_ts, channel_code, category, age_grp, category_no, item_nk" in stg


# --- dims ------------------------------------------------------------------


def test_dim_channel_seeds_all_five_channels():
    merge = sql.dim_channel_merge_sql(PROJ, STAR)
    for code in CHANNELS:
        assert f"'{code}' AS channel_code" in merge
    assert merge.count("channel_code") >= 5
    assert "GENERATE_UUID()" in merge


def test_dims_mint_uuid_surrogate_keys():
    # GENERATE_UUID on insert is the chosen SK strategy across all dims
    for builder in (sql.dim_brand_merge_sql, sql.dim_company_merge_sql,
                    sql.dim_ranking_list_merge_sql):
        assert "GENERATE_UUID()" in builder(PROJ, STAR)
    assert "GENERATE_UUID()" in sql.dim_item_insert_sql(PROJ, STAR)


def test_ranking_list_grain_excludes_category_no():
    # category_no is a multi-valued lottedfs product tag (~5 per product, same rank);
    # keying the list on it fragments one real list into overlapping pseudo-lists.
    merge = sql.dim_ranking_list_merge_sql(PROJ, STAR)
    assert "category_no" not in merge
    assert "SELECT DISTINCT channel_code, category, age_grp FROM stg" in merge
    ddl = sql.create_tables_ddl(PROJ, LAND, STAR)
    rl_ddl = ddl.split("dim_ranking_list`", 1)[1].split(")", 1)[0]
    assert "category_no" not in rl_ddl
    # landing still carries the tag verbatim
    assert "category_no STRING" in ddl


def test_unknown_brand_sentinel_present():
    assert sql.UNKNOWN_BRAND == "(unknown)"
    assert "(unknown)" in sql.stg_projection_sql()


# --- SCD2 dim_item ---------------------------------------------------------


def test_scd2_expire_closes_changed_current_version():
    expire = sql.dim_item_expire_sql(PROJ, STAR)
    assert "target.is_current" in expire
    assert ("WHEN MATCHED AND target.attr_hash != source.attr_hash "
            "AND source.is_authoritative THEN UPDATE SET") in expire
    assert "valid_to = source.valid_from" in expire
    assert "is_current = FALSE" in expire


def test_scd2_driving_row_prefers_authoritative_observation():
    # An unenriched detail-channel row (no html_object) is "no information": it must not
    # outrank an older enriched sighting, and it must never close an enriched version.
    # Detail-less naver channels are always authoritative.
    expire = sql.dim_item_expire_sql(PROJ, STAR)
    authoritative = "(html_object IS NOT NULL OR channel_code IN ('naverbest', 'superpoint'))"
    assert f"{authoritative} AS is_authoritative" in expire
    assert f"ORDER BY {authoritative} DESC, snapshot_ts DESC" in expire
    # insert/touch share the same incoming CTE, so the preference applies everywhere
    assert f"ORDER BY {authoritative} DESC, snapshot_ts DESC" in sql.dim_item_insert_sql(PROJ, STAR)


def test_scd2_insert_only_when_no_current_version():
    insert = sql.dim_item_insert_sql(PROJ, STAR)
    assert "WHERE d.item_sk IS NULL" in insert
    assert "TRUE" in insert  # is_current for the new version


def test_scd2_insert_nulls_are_typed():
    # A bare NULL in an INSERT ... SELECT list is typed INT64 by BigQuery and refuses to
    # coerce into product_id STRING / valid_to TIMESTAMP (runtime failure; dry-run passes).
    insert = sql.dim_item_insert_sql(PROJ, STAR)
    assert "CAST(NULL AS STRING)" in insert    # product_id
    assert "CAST(NULL AS TIMESTAMP)" in insert  # valid_to
    select_body = insert.split("SELECT", 1)[1]
    assert not re.search(r"(?<!AS )NULL\s*,", select_body.replace("CAST(NULL", "CAST(_"))


# --- fact ------------------------------------------------------------------


def test_fact_resolves_item_via_temporal_scd2_join():
    fact = sql.fact_merge_sql(PROJ, STAR)
    assert "s.snapshot_ts >= di.valid_from" in fact
    assert "di.valid_to IS NULL OR s.snapshot_ts < di.valid_to" in fact
    # grain match keys
    assert "target.snapshot_ts = source.snapshot_ts" in fact
    assert "target.item_sk = source.item_sk" in fact
    # additive discount component stored; rate carried but never summed here
    assert "discount_amount_krw = source.discount_amount_krw" in fact


def test_fact_collapses_category_no_duplicates_to_list_grain():
    # lottedfs repeats a product once per category_no tag with the same rank; the fact
    # source must keep exactly one row per list-grain key (deterministic survivor) and
    # the ranking-list join must not key on the tag.
    fact = sql.fact_merge_sql(PROJ, STAR)
    assert "PARTITION BY snapshot_ts, channel_code, category, age_grp, item_nk" in fact
    assert "ORDER BY IFNULL(category_no, '')" in fact
    assert "rl.category_no" not in fact


def test_list_grain_guard_asserts_rank_agreement_before_any_merge():
    guard = sql.list_grain_check_sql()
    assert "GROUP BY snapshot_ts, channel_code, category, age_grp, item_nk" in guard
    assert "COUNT(DISTINCT rank) > 1" in guard
    # positioned after stg exists but before the first MERGE, so an unsound batch
    # writes nothing at all
    s = _script()
    assert s.index("CREATE TEMP TABLE stg") < s.index("category_no duplicates disagree")
    assert s.index("category_no duplicates disagree") < s.index("MERGE `")


# --- checks & view ---------------------------------------------------------


def test_row_check_variants():
    assert sql.raw_records_check_sql(None).strip() == "SELECT COUNT(*) AS row_count FROM stg;"
    assert "ASSERT (SELECT COUNT(*) FROM stg) = 42" in sql.raw_records_check_sql(42)
    with pytest.raises(ValueError):
        sql.raw_records_check_sql(-1)


def test_expected_row_count_flows_into_script():
    assert "ASSERT (SELECT COUNT(*) FROM stg) = 7" in _script(expected=7)


def test_daily_view_has_flow_features():
    view = sql.fct_item_daily_view_sql(PROJ, STAR)
    assert "CREATE OR REPLACE VIEW `proj.dw_cosmetics.fct_item_daily`" in view
    assert "review_cnt_delta" in view and "rank_delta" in view and "days_on_chart" in view
    assert "LAG(item_review_cnt) OVER w" in view


# --- validation ------------------------------------------------------------


def test_rejects_bad_identifiers():
    with pytest.raises(ValueError):
        sql.create_tables_ddl("proj;drop", LAND, STAR)
    with pytest.raises(ValueError):
        sql.combined_cosmetics_transform_sql(
            project_id=PROJ, landing_dataset_id="dl-products", star_dataset_id=STAR)


def test_accepts_hyphenated_project_id():
    """Real GCP project ids carry hyphens (dev-dfml-platform); dataset/table ids cannot."""
    ddl = sql.create_tables_ddl("dev-dfml-platform", LAND, STAR)
    assert f"`dev-dfml-platform.{LAND}.cosmetics_rankings`" in ddl


@pytest.mark.parametrize(
    "project_id",
    ["proj;drop", "proj-", "-proj", "pro`j", "proj space", "proj.other", "proj_x", ""],
)
def test_rejects_bad_project_ids(project_id):
    with pytest.raises(ValueError, match="project_id"):
        sql.create_tables_ddl(project_id, LAND, STAR)


def test_raw_records_table_name():
    assert sql.RAW_RECORDS_TABLE == "raw_cosmetics_records"
    assert "FROM raw_cosmetics_records" in sql.stg_projection_sql()
