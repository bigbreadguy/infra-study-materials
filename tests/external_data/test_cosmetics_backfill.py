"""Pure normalizer tests: prototype snapshots -> scraper-shaped envelopes.

Fixtures are trimmed real rows (schemas captured from a live snapshot). The pinning tests
assert that a backfilled row matches the scraper's DETAIL_COLUMNS contract per source so the
backfill and live paths cannot drift silently (plan §2.6).
"""

from __future__ import annotations

import pytest

from external_data.common import cosmetics_backfill as cb
from external_data.common.cosmetics_schema import DETAIL_COLUMNS


SNAP = "20260707/T092542"
BUCKET = "cnp-bucket"
PREFIX = "cnp-scraping-raw"


def _norm(source, base_rows, detail_rows=None):
    return cb.normalize_records(
        source, base_rows=base_rows, detail_rows=detail_rows,
        snapshot=SNAP, raw_bucket=BUCKET, raw_prefix=PREFIX,
    )


# --- snapshot id / timestamps ----------------------------------------------


def test_snapshot_scraped_at_is_kst_to_utc():
    # T092542 == 2026-07-07T09:25:42+09:00 -> 00:25:42Z
    assert cb.snapshot_scraped_at(SNAP) == "2026-07-07T00:25:42Z"


def test_parse_snapshot_id_normalizes_and_validates():
    assert cb.parse_snapshot_id("20260707/T092542") == "20260707T092542"
    assert cb.parse_snapshot_id("20260707T092542") == "20260707T092542"
    with pytest.raises(ValueError):
        cb.parse_snapshot_id("2026-07-07 09:25")


def test_result_object_name_is_snapshot_keyed_and_deterministic():
    name = cb.result_object_name(SNAP, "lottedfs.best_rankings")
    assert name == "scrape/results/cnp_backfill/20260707T092542/lottedfs.best_rankings.json"
    # independent of run: same snapshot + recipe -> same object (idempotent overwrite)
    assert name == cb.result_object_name("20260707T092542", "lottedfs.best_rankings")


def test_to_utc_z_handles_epoch_and_iso_and_nullish():
    assert cb.to_utc_z("1783383942") == "2026-07-07T00:25:42Z"
    assert cb.to_utc_z("2026-07-07T09:25:42+09:00") == "2026-07-07T00:25:42Z"
    assert cb.to_utc_z("nan") is None
    assert cb.to_utc_z(None) is None


def test_scalar_coercions():
    assert cb.to_int("551,212") == 551212
    assert cb.to_int("nan") is None and cb.to_int("") is None
    assert cb.to_float("358") == 358.0
    assert cb.to_float("nan") is None
    assert cb.to_str("  x ") == "x" and cb.to_str("nan") is None


# --- CSV parsing -----------------------------------------------------------


def test_parse_csv_strips_bom():
    text = "﻿category,rank\n스킨케어,1\n"
    rows = cb.parse_csv(text)
    assert rows == [{"category": "스킨케어", "rank": "1"}]


# --- schema integrity ------------------------------------------------------


def test_every_record_is_exactly_detail_columns_plus_extra():
    rows = [{"rank": "1", "product_id": "1", "product_no": "1", "name": "x",
             "sale_price": "10", "price_origin": "20", "discount_rate": "50",
             "review_score": "4.5", "review_count": "3", "category_name": "c",
             "product_url": "u"}]
    (rec,) = _norm("naverbest", rows)
    assert set(rec) == set(DETAIL_COLUMNS) | {"_extra"}


# --- lottedfs: base CSV left-joined to detail NDJSON, NDJSON wins -----------


def _lotte_csv():
    return {
        "category": "스킨케어", "age_group": "20대", "page_no": "1", "rank": "1",
        "brand": "에스티로더", "product": "Advanced Night Repair 어드밴스드 나이트 리페어",
        "price_usd": "358", "price_krw": "551212", "price_origin": "", "discount_rate": "",
        "prd_no": "20000729163", "prd_opt_no": "20000903400", "cat_no": "1000000019",
        "ref_cd": "", "item_cd": "", "detail_url": "https://lotte/p/20000729163",
        "collected_at": "1783383974",
    }


def _lotte_detail():
    return {
        "category": "스킨케어", "age_grp": "20대", "page_no": 1, "rank": 1,
        "brand_nm": "에스티로더", "item_cd": "2071391667", "ref_cd": "PLW501",
        "item_nm_kor": "어드밴스드 나이트 리페어", "item_nm_eng": "Advanced Night Repair",
        "item_origin": "영국", "item_score": 4.9, "item_review_cnt": 1517,
        "item_category": "리페어 세럼/에센스", "item_contents": "정제수, 글리세린",
        "item_volume": "100ml x 2", "price_usd": 358.0, "price_krw": 551212,
        "prd_no": "20000729163", "company_mf": "ESTEE LAUDER",
        "create_dt": "2026-07-07T09:26:28+09:00",
        "_extra": {"layout": "brand", "item_functional": "주름개선 기능성 화장품",
                   "category_path": ["스킨케어"], "spec": {"제조국": "영국"}},
    }


def test_lottedfs_join_ndjson_wins_and_functional_lifted():
    (rec,) = _norm("lottedfs", [_lotte_csv()], [_lotte_detail()])
    assert rec["source"] == "lottedfs"
    # NDJSON detail wins over the empty listing item_cd
    assert rec["item_cd"] == "2071391667"
    assert rec["ref_cd"] == "PLW501"
    # item_functional lifted out of nested _extra into a top-level column
    assert rec["item_functional"] == "주름개선 기능성 화장품"
    assert rec["item_contents"] == "정제수, 글리세린"
    # product_url comes from the base listing (NDJSON has none)
    assert rec["product_url"] == "https://lotte/p/20000729163"
    # detail-joined row references its baked HTML in place under the raw prefix
    assert rec["html_object"] == (
        "gs://cnp-bucket/cnp-scraping-raw/20260707/T092542/lottedfs_details/20000729163.html"
    )
    # per-row collected_at prefers the detail build time, converted to UTC Z
    assert rec["collected_at"] == "2026-07-07T00:26:28Z"
    # overflow preserved, nothing dropped
    assert "spec" in rec["_extra"] and "category_path" in rec["_extra"]


def test_html_object_suppressed_when_baked_page_missing():
    # Interrupted prototype run: the detail NDJSON row exists but its HTML never got
    # baked. The join still enriches; only the dead gs:// link is suppressed.
    (rec,) = cb.normalize_records(
        "lottedfs", base_rows=[_lotte_csv()], detail_rows=[_lotte_detail()],
        snapshot=SNAP, raw_bucket=BUCKET, raw_prefix=PREFIX, html_codes=set(),
    )
    assert rec["html_object"] is None
    assert rec["item_cd"] == "2071391667"  # detail enrichment kept


def test_html_object_kept_when_page_listed_and_none_skips_check():
    (rec,) = cb.normalize_records(
        "lottedfs", base_rows=[_lotte_csv()], detail_rows=[_lotte_detail()],
        snapshot=SNAP, raw_bucket=BUCKET, raw_prefix=PREFIX, html_codes={"20000729163"},
    )
    assert rec["html_object"].endswith("/lottedfs_details/20000729163.html")
    # html_codes=None (the default) trusts the join, matching pre-existing behavior
    (rec_none,) = _norm("lottedfs", [_lotte_csv()], [_lotte_detail()])
    assert rec_none["html_object"] == rec["html_object"]


def test_lottedfs_unjoined_row_has_no_html_and_keeps_listing_values():
    (rec,) = _norm("lottedfs", [_lotte_csv()], [])  # no detail match
    assert rec["html_object"] is None
    assert rec["brand_nm_kor"] == "에스티로더"
    assert rec["price_usd"] == 358.0
    # bilingual split applied to the listing product name
    assert rec["item_nm_eng"] == "Advanced Night Repair"
    assert rec["item_nm_kor"].startswith("어드밴스드")
    # listing price_origin maps to the USD list price for a USD-quoting duty-free channel
    assert rec["price_origin_usd"] is None  # empty in this fixture
    assert rec["collected_at"] == "2026-07-07T00:26:14Z"  # epoch 1783383974


# --- ssgdfs: USD prices, list item_contents preserved ----------------------


def test_ssgdfs_list_contents_preserved_and_origin_usd():
    csv = {"category": "뷰티", "page_no": "1", "rank": "1", "brand": "에스티 로더",
           "brand_en": "ESTEE LAUDER", "product": "Advanced Night Repair",
           "price_usd": "358", "price_origin": "nan", "discount_rate": "nan",
           "price_krw": "551212", "goos_cd": "101030100639",
           "detail_url": "https://ssg/p", "collected_at": "1783383945"}
    detail = {"item_cd": "101030100639", "item_contents": ["정제수", "글리세린"],
              "brand_nm_kor": "에스티 로더", "item_review_cnt": 967,
              "create_dt": "2026-07-07T09:25:47+09:00", "_extra": {"goos_cd": "101030100639"}}
    (rec,) = _norm("ssgdfs", [csv], [detail])
    assert rec["brand_nm_eng"] == "ESTEE LAUDER"
    assert rec["item_contents"] == ["정제수", "글리세린"]  # SSG list kept as-is
    assert rec["price_usd"] == 358.0
    assert rec["price_origin_usd"] is None  # "nan" coerced away
    assert rec["item_review_cnt"] == 967
    assert rec["html_object"].endswith("ssgdfs_details/101030100639.html")


# --- oliveyoung: sales tag formatting, category split ----------------------


def test_oliveyoung_sales_tag_and_category():
    csv = {"category": "스킨케어", "page_no": "1", "rank": "1", "brand": "블랑네이처",
           "product": "어성초 토너", "price_krw": "19900", "price_origin": "38000",
           "discount_rate": "48.0", "review_score": "5.5", "goods_no": "A000000255290",
           "goods_category": "01 > 스킨케어 > 스킨/토너", "flags": "세일,쿠폰,증정",
           "detail_url": "https://oy/p", "collected_at": "1783383943"}
    (rec,) = _norm("oliveyoung", [csv])  # detail-less path (no join)
    assert rec["item_cd"] == "A000000255290"
    assert rec["item_sales_tag"] == "[세일, 쿠폰, 증정]"
    assert rec["item_category_01"] == "스킨/토너"
    assert rec["item_score"] == 5.5
    assert rec["price_origin_krw"] == 38000.0
    assert rec["html_object"] is None  # no detail row joined


# --- naver feeds: detail-less, identity keys, _raw preserved ---------------


def test_naverbest_detailless_identity_and_reviews():
    row = {"category_name": "화장품/미용", "rank": 1, "product_id": "91175117452",
           "product_no": "91175117452", "name": "헤어 본딩 3종", "sale_price": 21000,
           "price_origin": 75000, "review_score": 4.7, "review_count": 112,
           "discount_rate": 72, "product_url": "https://smartstore/p",
           "collected_dt": "2026-07-07T09:25:42+09:00",
           "_raw": {"nvMid": "91175117452"}, "mall_name": "코스알엑스"}
    (rec,) = _norm("naverbest", [row])
    assert rec["item_cd"] == "91175117452"  # nvMid == flat product_id
    assert rec["item_score"] == 4.7 and rec["item_review_cnt"] == 112
    assert rec["price_krw"] == 21000 and rec["price_origin_krw"] == 75000.0
    assert rec["item_contents"] is None and rec["company_mf"] is None  # no detail pass
    assert rec["html_object"] is None
    assert rec["_extra"]["mall_name"] == "코스알엑스" and "_raw" in rec["_extra"]


def test_superpoint_detailless_identity():
    row = {"rank": 1, "product_id": "10170067994", "product_no": "10121076495",
           "name": "탈모샴푸", "discounted_price": 37000, "sale_price": 38000,
           "discount_ratio": 2, "product_url": "https://smartstore/p",
           "collected_at": 1783383942, "_raw": {"id": "10170067994"}}
    (rec,) = _norm("superpoint", [row])
    assert rec["item_cd"] == "10170067994" and rec["prd_no"] == "10121076495"
    assert rec["price_krw"] == 37000 and rec["price_origin_krw"] == 38000.0
    assert rec["discount_rate"] == 2
    assert rec["category"] is None  # superpoint has no per-category label


# --- envelope --------------------------------------------------------------


def test_build_envelope_shape_and_provenance():
    (rec,) = _norm("naverbest", [{"rank": "1", "product_id": "9", "name": "x"}])
    env = cb.build_envelope("naver_best.best_rankings", snapshot=SNAP, records=[rec],
                            raw_bucket=BUCKET, raw_prefix=PREFIX)
    assert env["schema_version"] == "1"
    assert env["recipe"] == "naver_best.best_rankings"
    assert env["status"] == "success" and env["error"] is None
    assert env["scraped_at"] == "2026-07-07T00:25:42Z"
    backfill = env["params"]["backfill"]
    assert backfill["snapshot_id"] == "20260707T092542"
    assert backfill["normalizer_version"] == cb.NORMALIZER_VERSION
    assert backfill["raw_prefix"] == "gs://cnp-bucket/cnp-scraping-raw/20260707/T092542/"
    assert env["data"] == [rec]


# --- file resolution -------------------------------------------------------


def test_rankings_csv_candidates_prefer_plain_then_checkpoint():
    assert cb.rankings_csv_candidates("lottedfs") == (
        "lottedfs_best_rankings.csv", "lottedfs_best_rankings_checkpoint.csv")
    assert cb.rankings_csv_candidates("naverbest")[0] == "naver_best_rankings.csv"


def test_base_and_detail_file_wiring():
    assert cb.base_file_kind("lottedfs") == "csv"
    assert cb.base_file_kind("naverbest") == "ndjson"
    assert cb.detail_ndjson_name("ssgdfs") == "ssgdfs_detail_pages.ndjson"
    assert cb.detail_ndjson_name("superpoint") is None
    assert cb.base_ndjson_name("superpoint") == "naver_super_point_products.ndjson"
    assert cb.base_ndjson_name("lottedfs") is None


def test_unknown_source_raises():
    with pytest.raises(ValueError):
        _norm("tiktok", [{"rank": "1"}])
