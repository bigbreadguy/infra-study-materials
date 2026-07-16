"""Vendored cosmetics envelope schema + channel metadata (Airflow-free, pure).

This repo does **not** import the scraper package, so the ordered union column list
is vendored here as a frozen tuple with a ``NORMALIZER_VERSION``. Re-sync against the
authoritative source when the scraper's schema changes:

    dfml-scraper/engine/cosmetics/schema.py :: DETAIL_COLUMNS

Both the backfill normalizer (``cosmetics_backfill``) and the BigQuery loader
(``cosmetics_bigquery_sql``) read this module so a synthesized backfill row and a
live-scraped row share one column contract. A per-site pinning fixture guards the two
paths against silent drift (plan §2.6).

The five live recipes are ``<source>.best_rankings``. Two of them (``naverbest`` /
``superpoint``) are **detail-less**: naver blocks product-detail scraping, so those
rows carry only listing fields and every detail column stays ``None`` (ingredients,
origin, companies, volume). The star models them as first-class *ranking* observations
with no enrichment (design §6); ingredient/company promotion simply never fires for
them.
"""

from __future__ import annotations

from typing import Mapping


# Bump in lockstep with a change to DETAIL_COLUMNS or the normalizer's field mapping;
# it rides in every backfill envelope's params echo so a re-normalized corpus is
# distinguishable from an older one.
NORMALIZER_VERSION = 1

# Ordered union schema, vendored verbatim from
# dfml-scraper/engine/cosmetics/schema.py::DETAIL_COLUMNS. Order is authoritative for
# the envelope rows; the landing table columns follow it.
DETAIL_COLUMNS: tuple[str, ...] = (
    "source",             # site: lottedfs / oliveyoung / ssgdfs / superpoint / naverbest
    "category",           # ranking category label (e.g. 스킨케어 / 뷰티)
    "age_grp",            # lottedfs age filter label (None elsewhere)
    "page_no",            # listing page number where captured (None where n/a)
    "rank",               # 1-based rank within (category[, age])
    "brand_nm_kor",       # brand, Korean
    "brand_nm_eng",       # brand, English (None where the site gives only one)
    "item_cd",            # site product code / goods id
    "ref_cd",             # reference code (Lotte/SSG); None elsewhere
    "prd_no",             # numeric product id (Lotte/SSG/superpoint productNo)
    "prd_opt_no",         # option id (Lotte)
    "category_no",        # listing category code (Lotte nrmCatNo)
    "item_nm_kor",        # product name, Korean
    "item_nm_eng",        # product name, English (from the bilingual split)
    "item_category",      # detail breadcrumb, deepest segment
    "item_category_01",   # detail breadcrumb, level 1 (Olive Young / SSG)
    "item_category_02",   # detail breadcrumb, level 2
    "item_origin",        # 제조국
    "item_contents",      # 전성분 ingredients (str, or list for SSG)
    "item_skin_type",     # 제품 주요 사양 / skin type
    "item_volume",        # 내용물의 용량 또는 중량
    "item_functional",    # 기능성 화장품 여부
    "item_score",         # review score
    "item_review_cnt",    # review count
    "item_sales_tag",     # sale / time-window tag(s)
    "price_usd",          # sale price, USD (Lotte/SSG)
    "price_krw",          # sale price, KRW
    "price_origin_usd",   # list price, USD (Lotte/SSG)
    "price_origin_krw",   # list price, KRW
    "discount_rate",      # % discount
    "company_mf",         # 화장품제조업자 (manufacturer)
    "company_mah",        # 화장품책임판매업자 (marketing authorization holder)
    "company_fit",        # 맞춤형화장품판매업자 (fit / custom seller)
    "product_url",        # canonical product/detail URL
    "html_object",        # gs:// URI (or local path) of the baked detail HTML; None if none
    "collected_at",       # UTC ISO-8601 (Z) capture time
)

# The lineage columns the scraper's build_record stamps itself; the normalizer sets
# them explicitly and never lets a mapped field collide with them.
RESERVED_COLUMNS = frozenset({"source", "html_object", "collected_at"})


# --- Recipes / sources -----------------------------------------------------

# The five live recipes, in the DAG's static fan-out order. A recipe is
# ``<source>.<flow>``; every cosmetics flow is ``best_rankings``.
RECIPES: tuple[str, ...] = (
    "lottedfs.best_rankings",
    "oliveyoung.best_rankings",
    "ssgdfs.best_rankings",
    "naver_superpoint.best_rankings",
    "naver_best.best_rankings",
)


def recipe_source(recipe: str) -> str:
    """The ``source`` discriminator for a ``<source>.best_rankings`` recipe.

    ``naver_superpoint`` / ``naver_best`` are the recipe names; the envelope ``source``
    column the scraper stamps is ``superpoint`` / ``naverbest`` (see CHANNELS). Map the
    recipe prefix onto that canonical source so both paths agree.
    """
    if not recipe or "." not in recipe:
        raise ValueError(f"recipe must be '<source>.best_rankings', got {recipe!r}")
    prefix = recipe.split(".", 1)[0]
    return _RECIPE_PREFIX_TO_SOURCE.get(prefix, prefix)


_RECIPE_PREFIX_TO_SOURCE: dict[str, str] = {
    "lottedfs": "lottedfs",
    "oliveyoung": "oliveyoung",
    "ssgdfs": "ssgdfs",
    "naver_superpoint": "superpoint",
    "naver_best": "naverbest",
}


# --- Channel dimension seed ------------------------------------------------

# The five channels, seeded by hand (design §4.1). ``channel_type`` is the
# analytically load-bearing attribute: duty-free rank (inbound-tourism demand) vs
# domestic rank (domestic consumption) are different demand signals. ``currency_quoted``
# documents why USD prices exist only for Lotte/SSG. ``has_detail`` records whether the
# source runs a product-detail pass (naver blocks it), i.e. whether its rows can ever
# carry ingredients/companies/origin.
CHANNELS: Mapping[str, Mapping[str, object]] = {
    "lottedfs": {
        "channel_code": "lottedfs",
        "channel_type": "duty_free",
        "currency_quoted": "USD_KRW",
        "audience": "inbound_tourism",
        "has_detail": True,
    },
    "ssgdfs": {
        "channel_code": "ssgdfs",
        "channel_type": "duty_free",
        "currency_quoted": "USD_KRW",
        "audience": "inbound_tourism",
        "has_detail": True,
    },
    "oliveyoung": {
        "channel_code": "oliveyoung",
        "channel_type": "domestic_retail",
        "currency_quoted": "KRW",
        "audience": "domestic",
        "has_detail": True,
    },
    "superpoint": {
        "channel_code": "superpoint",
        "channel_type": "marketplace",
        "currency_quoted": "KRW",
        "audience": "domestic",
        "has_detail": False,
    },
    "naverbest": {
        "channel_code": "naverbest",
        "channel_type": "marketplace",
        "currency_quoted": "KRW",
        "audience": "domestic",
        "has_detail": False,
    },
}


def source_has_detail(source: str) -> bool:
    """Whether ``source`` runs a product-detail pass (False for the naver feeds)."""
    channel = CHANNELS.get(source)
    return bool(channel and channel["has_detail"])
