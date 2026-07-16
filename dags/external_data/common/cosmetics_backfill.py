"""Pure normalizer: historical cosmetics snapshots -> scraper-shaped envelopes.

The prototype uploaded one snapshot per ``cnp-scraping-raw/{YYYYMMDD}/T{HHMMSS}/``
prefix (a verbatim copy of the local scraper's ``data/`` dir). This module turns each
(snapshot, source) into a **result envelope** identical in shape to what the live Cloud
Run scraper writes, so history and live data are one uniform corpus for the BigQuery
loader (plan §2.3–§2.7).

Kept free of Airflow/GCS imports (mirrors ``scrape_request`` / ``materials_result``): the
functions take already-read file text and return dicts, so they unit-test directly and a
pinning fixture can pin a backfilled row against a captured live-run row per source.

Per-source shape (verified against a real snapshot):

* **lottedfs / oliveyoung / ssgdfs** — base rows come from the rankings CSV (the complete
  ranked set), left-joined to the detail NDJSON by the site's identity key; NDJSON values
  win where present (they are already merged in the prototype's near-final shape). The
  plain ``{source}_best_rankings.csv`` is preferred, falling back to
  ``{source}_best_rankings_checkpoint.csv`` when absent (some snapshots ship only the
  checkpoint). ``html_object`` points at the baked detail HTML **in place** under the raw
  prefix (plan §2.5); a row gets one only when it was detail-passed (i.e. joined).
* **superpoint / naverbest** — detail-less (naver blocks product-detail scraping). Base
  rows come from the rankings NDJSON (complete, and it carries the raw API payload under
  ``_raw``); every detail column stays ``None`` and ``html_object`` is ``None``.

Snapshot directory timestamps are **KST** (the local-dev machine); verified against one
snapshot's in-row ``collected_dt`` (``T092542`` == ``2026-07-07T09:25:42+09:00``). The
envelope ``scraped_at`` is that KST wall-clock converted to UTC ``Z``.
"""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from external_data.common.cosmetics_schema import (
    DETAIL_COLUMNS,
    NORMALIZER_VERSION,
    RECIPES,
    recipe_source,
    source_has_detail,
)


# Wire-contract version shared with the scraper envelope (matches scrape_request).
SCHEMA_VERSION = "1"

_KST = timezone(timedelta(hours=9))
_UTC = timezone.utc

# A snapshot id is ``{YYYYMMDD}/T{HHMMSS}`` on GCS; normalized to ``{YYYYMMDD}T{HHMMSS}``
# for the envelope object name and degenerate lineage.
_SNAPSHOT_RE = re.compile(r"^(?P<date>\d{8})[/T]?T?(?P<time>\d{6})$")
# Values that stand in for "empty" in the prototype CSVs (pandas writes literal "nan").
_NULLISH = {"", "nan", "none", "null", "na"}


# --- scalar coercions ------------------------------------------------------


def _clean(value: Any) -> Any:
    """None for nullish scalars ('', 'nan', 'None'), else the value stripped if a str."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return None if stripped.lower() in _NULLISH else stripped
    return value


def to_int(value: Any) -> int | None:
    """Digits-only int (mirrors the scraper's ``to_int``); None when empty/nullish."""
    value = _clean(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^0-9-]", "", str(value))
    if digits in ("", "-"):
        return None
    return int(digits)


def to_float(value: Any) -> float | None:
    """Float from a price-like string/number; None when empty/nullish/non-numeric."""
    value = _clean(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        out = float(value)
    else:
        try:
            out = float(str(value).replace(",", ""))
        except (TypeError, ValueError):
            return None
    return None if out != out else out  # NaN != NaN


def to_str(value: Any) -> str | None:
    value = _clean(value)
    return None if value is None else str(value)


def split_bilingual_name(name: str | None) -> dict[str, str]:
    """Split ``"English 한글"`` into (eng, kor); mirrors the scraper's lottedfs split."""
    name = re.sub(r"\s+", " ", (name or "")).strip()
    if not name:
        return {"item_nm_eng": "", "item_nm_kor": ""}
    m = re.search(r"[가-힣]", name)
    if not m:
        return {"item_nm_eng": name, "item_nm_kor": ""}
    return {"item_nm_eng": name[: m.start()].strip(" -|·"), "item_nm_kor": name[m.start():].strip()}


def _sales_tag(flags: Any) -> str:
    """Format an oliveyoung ``flags`` cell into the scraper's ``[a, b, c]`` tag string."""
    flags = _clean(flags)
    if not flags:
        return ""
    parts = [p.strip() for p in str(flags).split(",") if p.strip()]
    return f"[{', '.join(parts)}]" if parts else ""


# --- timestamps ------------------------------------------------------------


def to_utc_z(value: Any) -> str | None:
    """Normalize an epoch (int/str) or tz-aware ISO string to ``YYYY-MM-DDTHH:MM:SSZ``.

    The prototype rows carry either an epoch ``collected_at`` or an ISO ``+09:00``
    ``collected_dt`` / ``create_dt``. Both denote the same instant; normalize to UTC ``Z``
    so backfilled rows match the scraper's ``collected_at`` contract. Returns None on an
    unparseable value rather than guessing.
    """
    value = _clean(value)
    if value is None:
        return None
    # Epoch seconds (CSV writes them as digit strings).
    if isinstance(value, (int, float)) or (isinstance(value, str) and re.fullmatch(r"\d{9,11}", value)):
        return datetime.fromtimestamp(int(value), tz=_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    text = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_KST)  # naive prototype times are KST wall-clock
    return dt.astimezone(_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_snapshot_id(snapshot: str) -> str:
    """Normalize a snapshot ref (``YYYYMMDD/THHMMSS`` or ``YYYYMMDDTHHMMSS``) -> canonical.

    Canonical form is ``{YYYYMMDD}T{HHMMSS}`` (the envelope-object segment). Raises on a
    malformed ref so a wrong operator window fails loudly.
    """
    m = _SNAPSHOT_RE.match((snapshot or "").strip().strip("/"))
    if not m:
        raise ValueError(f"snapshot must be 'YYYYMMDD/THHMMSS', got {snapshot!r}")
    return f"{m.group('date')}T{m.group('time')}"


def snapshot_scraped_at(snapshot: str) -> str:
    """Envelope ``scraped_at`` = the KST snapshot wall-clock as UTC ``Z`` (plan §2.4)."""
    canonical = parse_snapshot_id(snapshot)
    dt = datetime.strptime(canonical, "%Y%m%dT%H%M%S").replace(tzinfo=_KST)
    return dt.astimezone(_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- GCS layout ------------------------------------------------------------


def raw_snapshot_prefix(raw_prefix: str, snapshot: str) -> str:
    """Object prefix of a raw snapshot: ``{raw_prefix}/{YYYYMMDD}/T{HHMMSS}`` (no bucket)."""
    canonical = parse_snapshot_id(snapshot)
    date, time = canonical[:8], canonical[9:]
    return f"{raw_prefix.strip('/')}/{date}/T{time}"


def result_object_name(snapshot: str, recipe: str) -> str:
    """Deterministic backfill envelope object: keyed by snapshot id, not run_id (plan §2.4).

    Re-running any backfill window overwrites the same object, so it is idempotent and
    independent of which manual run produced it; a future loader sweeps one stable prefix.
    """
    if not recipe:
        raise ValueError("recipe must be a non-empty string")
    return f"scrape/results/cnp_backfill/{parse_snapshot_id(snapshot)}/{recipe}.json"


def html_object_uri(raw_bucket: str, raw_snapshot_prefix_: str, source: str, code: str) -> str:
    """URI of a baked detail page referenced in place under the raw prefix (plan §2.5)."""
    return f"gs://{raw_bucket}/{raw_snapshot_prefix_}/{source}_details/{code}.html"


# --- file parsing ----------------------------------------------------------


def parse_csv(text: str) -> list[dict[str, str]]:
    """Parse rankings CSV text (BOM tolerated) into ordered row dicts."""
    if not text:
        return []
    return list(csv.DictReader(io.StringIO(text.lstrip("﻿"))))


def parse_ndjson(text: str) -> list[dict[str, Any]]:
    """Parse NDJSON text into row dicts (blank lines skipped)."""
    rows: list[dict[str, Any]] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


# --- per-source listing mappers (CSV/base row -> DETAIL_COLUMNS subset) -----
#
# Each returns the identity code (join key + html filename) and the DETAIL_COLUMNS the
# listing carries. Detail overlay (below) wins where it has a value.


def _base_lottedfs(row: Mapping[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    name = split_bilingual_name(row.get("product"))
    code = to_str(row.get("prd_no")) or ""
    cols = {
        "category": to_str(row.get("category")),
        "age_grp": to_str(row.get("age_group")),
        "page_no": to_int(row.get("page_no")),
        "rank": to_int(row.get("rank")),
        "brand_nm_kor": to_str(row.get("brand")),
        "item_cd": to_str(row.get("item_cd")),
        "ref_cd": to_str(row.get("ref_cd")),
        "prd_no": to_str(row.get("prd_no")),
        "prd_opt_no": to_str(row.get("prd_opt_no")),
        "category_no": to_str(row.get("cat_no")),
        "item_nm_kor": name["item_nm_kor"] or to_str(row.get("product")),
        "item_nm_eng": name["item_nm_eng"] or None,
        "price_usd": to_float(row.get("price_usd")),
        "price_krw": to_int(row.get("price_krw")),
        "price_origin_usd": to_float(row.get("price_origin")),  # lottedfs quotes USD
        "discount_rate": to_float(row.get("discount_rate")),
        "product_url": to_str(row.get("detail_url")),
    }
    return code, cols, {"href": to_str(row.get("href"))}


def _base_oliveyoung(row: Mapping[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    code = to_str(row.get("goods_no")) or ""
    breadcrumb = [s.strip() for s in str(row.get("goods_category") or "").split(">") if s.strip()]
    deepest = breadcrumb[-1] if breadcrumb else None
    cols = {
        "category": to_str(row.get("category")),
        "page_no": to_int(row.get("page_no")),
        "rank": to_int(row.get("rank")),
        "brand_nm_kor": to_str(row.get("brand")),
        "item_cd": code or None,
        "item_nm_kor": to_str(row.get("product")),
        "item_category_01": deepest,
        "item_score": to_float(row.get("review_score")),
        "item_sales_tag": _sales_tag(row.get("flags")) or None,
        "price_krw": to_int(row.get("price_krw")),
        "price_origin_krw": to_float(row.get("price_origin")),
        "discount_rate": to_float(row.get("discount_rate")),
        "product_url": to_str(row.get("detail_url")),
    }
    extra = {
        "goods_category": to_str(row.get("goods_category")),
        "filter_label": to_str(row.get("filter_label")),
        "flags": to_str(row.get("flags")),
        "item_no": to_str(row.get("item_no")),
        "href": to_str(row.get("href")),
    }
    return code, cols, extra


def _base_ssgdfs(row: Mapping[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    code = to_str(row.get("goos_cd")) or ""
    cols = {
        "category": to_str(row.get("category")),
        "page_no": to_int(row.get("page_no")),
        "rank": to_int(row.get("rank")),
        "brand_nm_kor": to_str(row.get("brand")),
        "brand_nm_eng": to_str(row.get("brand_en")),
        "item_cd": code or None,
        "item_nm_kor": to_str(row.get("product")),
        "price_usd": to_float(row.get("price_usd")),
        "price_krw": to_int(row.get("price_krw")),
        "price_origin_usd": to_float(row.get("price_origin")),  # ssgdfs quotes USD
        "discount_rate": to_float(row.get("discount_rate")),
        "product_url": to_str(row.get("detail_url")),
    }
    extra = {
        "goos_cd": code or None,
        "img_url": to_str(row.get("img_url")),
        "login_required": to_str(row.get("login_required")),
    }
    return code, cols, extra


def _base_naverbest(row: Mapping[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    # Detail-less; identity item_cd = nvMid (== flat product_id). Everything the API
    # payload carries beyond the fixed columns rides in _extra (with _raw when present).
    code = to_str(row.get("product_id")) or ""
    cols = {
        "category": to_str(row.get("category_name")),
        "rank": to_int(row.get("rank")),
        "item_cd": code or None,
        "prd_no": to_str(row.get("product_no")),
        "item_nm_kor": to_str(row.get("name")),
        "item_score": to_float(row.get("review_score")),
        "item_review_cnt": to_int(row.get("review_count")),
        "price_krw": to_int(row.get("sale_price")),
        "price_origin_krw": to_float(row.get("price_origin")),
        "discount_rate": to_float(row.get("discount_rate")),
        "product_url": to_str(row.get("product_url")),
    }
    return code, cols, _naver_extra(row)


def _base_superpoint(row: Mapping[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    code = to_str(row.get("product_id")) or ""  # == _raw.id
    cols = {
        "rank": to_int(row.get("rank")),
        "item_cd": code or None,
        "prd_no": to_str(row.get("product_no")),
        "item_nm_kor": to_str(row.get("name")),
        "price_krw": to_int(row.get("discounted_price")),
        "price_origin_krw": to_float(row.get("sale_price")),
        "discount_rate": to_float(row.get("discount_ratio")),
        "product_url": to_str(row.get("product_url")),
    }
    return code, cols, _naver_extra(row)


def _naver_extra(row: Mapping[str, Any]) -> dict[str, Any]:
    """Carry all non-DETAIL_COLUMNS naver/superpoint fields (incl. _raw) into _extra."""
    return {k: v for k, v in row.items() if k not in _NAVER_PROMOTED}


# Keys the naver base mappers already promote to DETAIL_COLUMNS; the rest go to _extra.
_NAVER_PROMOTED = frozenset(
    {
        "category_name", "rank", "product_id", "product_no", "name", "review_score",
        "review_count", "sale_price", "discounted_price", "price_origin", "discount_rate",
        "discount_ratio", "product_url",
    }
)


# --- detail overlay (NDJSON near-final row -> DETAIL_COLUMNS) ---------------

# The prototype detail NDJSON is near-DETAIL_COLUMNS; only these top-level keys need a
# rename, and ``item_functional`` is nested under ``_extra`` in the DFS files.
_DETAIL_RENAME = {"brand_nm": "brand_nm_kor", "item_nm": "item_nm_kor"}


def _detail_columns(row: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Map one near-final detail row to (DETAIL_COLUMNS subset, _extra overflow).

    Renames the two legacy keys, lifts ``item_functional`` out of the nested ``_extra``,
    and sends every remaining non-schema field to ``_extra`` so nothing is dropped.
    """
    cols: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    nested_extra = row.get("_extra")
    for key, value in row.items():
        if key == "_extra":
            continue
        mapped = _DETAIL_RENAME.get(key, key)
        if mapped in DETAIL_COLUMNS:
            cols[mapped] = value
        else:
            extra[key] = value
    if isinstance(nested_extra, Mapping):
        for key, value in nested_extra.items():
            if key == "item_functional" and "item_functional" in DETAIL_COLUMNS:
                cols["item_functional"] = value
            else:
                extra.setdefault(key, value)
    return cols, extra


_BASE_MAPPERS = {
    "lottedfs": _base_lottedfs,
    "oliveyoung": _base_oliveyoung,
    "ssgdfs": _base_ssgdfs,
    "naverbest": _base_naverbest,
    "superpoint": _base_superpoint,
}

# Where a source's base rows come from, and its identity key in the detail NDJSON.
_BASE_FILE = {  # source -> "csv" | "ndjson"
    "lottedfs": "csv", "oliveyoung": "csv", "ssgdfs": "csv",
    "naverbest": "ndjson", "superpoint": "ndjson",
}
# Detail NDJSON identity key per DFS source (matches the base row's identity code).
_DETAIL_KEY = {"lottedfs": "prd_no", "oliveyoung": "item_cd", "ssgdfs": "item_cd"}


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or (isinstance(value, float) and value != value)


def normalize_records(
    source: str,
    *,
    base_rows: Iterable[Mapping[str, Any]],
    detail_rows: Iterable[Mapping[str, Any]] | None = None,
    snapshot: str,
    raw_bucket: str,
    raw_prefix: str,
    html_codes: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Normalize one (snapshot, source) into DETAIL_COLUMNS-keyed envelope records.

    Base rows are the complete ranked set; each is left-joined to a detail row by the
    source's identity key, with detail values winning where present. Detail-less sources
    (naver) pass ``detail_rows=None`` and every detail column stays ``None``. Rows that
    joined a detail page get an in-place ``html_object`` under the raw prefix.

    ``html_codes`` is the set of identity codes whose baked ``{source}_details/{code}.html``
    actually exists (the caller lists the prefix once). An interrupted prototype run can
    carry a detail NDJSON row whose page was never baked; its join still enriches the
    record, but ``html_object`` stays ``None`` instead of a dead ``gs://`` link. ``None``
    skips the check (trust the join).
    """
    if source not in _BASE_MAPPERS:
        raise ValueError(f"unknown cosmetics source {source!r}")
    mapper = _BASE_MAPPERS[source]
    has_detail = source_has_detail(source)
    snap_prefix = raw_snapshot_prefix(raw_prefix, snapshot)
    known_html = None if html_codes is None else set(html_codes)

    detail_by_key: dict[str, Mapping[str, Any]] = {}
    if has_detail and detail_rows is not None:
        key_field = _DETAIL_KEY[source]
        for drow in detail_rows:
            key = to_str(drow.get(key_field))
            if key:
                detail_by_key.setdefault(key, drow)

    records: list[dict[str, Any]] = []
    for row in base_rows:
        code, base_cols, base_extra = mapper(row)
        record = {column: None for column in DETAIL_COLUMNS}
        record.update({k: v for k, v in base_cols.items() if k in DETAIL_COLUMNS})
        extra = {k: v for k, v in base_extra.items() if not _is_empty(v)}

        html_object = None
        drow = detail_by_key.get(code) if has_detail else None
        if drow is not None:
            detail_cols, detail_extra = _detail_columns(drow)
            for column, value in detail_cols.items():
                if not _is_empty(value):
                    record[column] = value
            extra.update({k: v for k, v in detail_extra.items() if not _is_empty(v)})
            if code and (known_html is None or code in known_html):
                html_object = html_object_uri(raw_bucket, snap_prefix, source, code)

        # Per-row collected_at: prefer the detail build time, else the listing time.
        collected = to_utc_z(
            record.get("collected_at")
            or (drow or {}).get("create_dt")
            or row.get("collected_dt")
            or row.get("collected_at")
        )
        record["source"] = source
        record["collected_at"] = collected
        record["html_object"] = html_object
        record["_extra"] = extra
        records.append(record)
    return records


# --- envelope --------------------------------------------------------------


def build_envelope(
    recipe: str,
    *,
    snapshot: str,
    records: list[dict[str, Any]],
    raw_bucket: str,
    raw_prefix: str,
) -> dict[str, Any]:
    """Assemble a scraper-shaped success envelope for one (snapshot, recipe).

    Mirrors the live envelope (``schema_version``/``recipe``/``params``/``scraped_at``/
    ``status``/``data``/``error``). Provenance rides in the params echo (plan §2.4);
    ``scraped_at`` is the KST snapshot time in UTC; per-row ``collected_at`` stays intact.
    """
    snap_prefix = raw_snapshot_prefix(raw_prefix, snapshot)
    return {
        "schema_version": SCHEMA_VERSION,
        "recipe": recipe,
        "params": {
            "backfill": {
                "raw_prefix": f"gs://{raw_bucket}/{snap_prefix}/",
                "snapshot_id": parse_snapshot_id(snapshot),
                "normalizer_version": NORMALIZER_VERSION,
            }
        },
        "scraped_at": snapshot_scraped_at(snapshot),
        "status": "success",
        "data": records,
        "error": None,
    }


# --- source file resolution (names only; the DAG does the GCS reads) --------


def rankings_csv_candidates(source: str) -> tuple[str, ...]:
    """Rankings CSV basenames to try in order (plain, then checkpoint fallback)."""
    stem = _CSV_STEM[source]
    return (f"{stem}.csv", f"{stem}_checkpoint.csv")


def detail_ndjson_name(source: str) -> str | None:
    """Detail NDJSON basename for a DFS source; None for the detail-less naver feeds."""
    if not source_has_detail(source):
        return None
    return f"{source}_detail_pages.ndjson"


def base_ndjson_name(source: str) -> str | None:
    """Base rankings NDJSON basename for a detail-less source; None for DFS sources."""
    if _BASE_FILE[source] != "ndjson":
        return None
    return f"{_CSV_STEM[source]}.ndjson"


def base_file_kind(source: str) -> str:
    """'csv' (DFS) or 'ndjson' (naver) — where this source's base rows come from."""
    return _BASE_FILE[source]


# Prototype filename stems differ from the canonical source (naver_super_point, naver_best).
_CSV_STEM = {
    "lottedfs": "lottedfs_best_rankings",
    "oliveyoung": "oliveyoung_best_rankings",
    "ssgdfs": "ssgdfs_best_rankings",
    "naverbest": "naver_best_rankings",
    "superpoint": "naver_super_point_products",
}


# Recipes eligible for backfill: all five when the snapshot carries their files. The DAG
# skips (with a report reason) any whose files are absent, so an operator subset via
# ``backfill_sources`` still works without code change.
BACKFILL_SOURCES: tuple[str, ...] = tuple(recipe_source(r) for r in RECIPES)
