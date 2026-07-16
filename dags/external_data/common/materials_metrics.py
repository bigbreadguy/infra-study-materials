"""Loader and validator for the materials metric mapping config.

One JSON file per recipe maps the recipe's result columns to curated star-schema
metrics. The files live under ``dags/external_data/configs/materials_metrics/``
and are committed to the repo, so they ship in the image and are visible to every
Airflow pod.

For grain-based recipes (the dpanda Bloomberg ``dpanda_bloomberg.<category>.json``
files) this config is also the single source of truth for the Mongo extraction
grains: ``grain_targets_from_config`` collapses the metrics to one target per
``(dataset_id, grain_id)`` pair, carrying each grain's curated ``description`` and
``time_grain`` (as ``freq``). There is no separate grain catalog.

The config is the single source of truth for what gets loaded: which datasource a
recipe belongs to, which category each metric falls under (category_0/1/2
hierarchy), which rows each metric selects (``match``), which measure column
carries the value, the optional previous-year column to split off, the optional
per-metric time grain, and the curated metric name/description/unit. A recipe with
no config file loads nothing (the DAG skips its transform-load step), so adding a
recipe to SOURCES before its mapping lands produces no errors.

A category may be set once at the recipe level (applied to every metric) and/or
overridden per metric; every metric must resolve to exactly one category because
dim_metrics.category_id is REQUIRED.

Every string here is embedded into the transform-load SQL as a literal or a JSON
path, so values are rejected if they carry quotes or backslashes; metric and
category names are further restricted to a SQL-safe identifier slug.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


# Committed config files live under dags/external_data/configs/materials_metrics/,
# grouped into a per-datasource subdirectory named by the recipe's datasource prefix
# (e.g. kosa/kosa.steel_scrap_import.json, eia/eia.prod_world.json,
# dpanda_bloomberg/dpanda_bloomberg.copper.json, yfinance/yfinance.fx_tryusd.json).
# Datasources without a subdirectory stay flat (e.g. kosis.manufacturing_operation.json,
# cssc.customs_trade.json). config_path resolves either layout.
CONFIG_DIR = (
    Path(__file__).resolve().parent.parent / "configs" / "materials_metrics"
)

# Metric and category names key their dim tables and are embedded as SQL literals,
# so keep them to a stable identifier slug rather than free text.
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
# A grain-based metric's match.dataset_id is the Mongo dataset ObjectId: 24
# lowercase hex chars, mirroring the Mongo compound key and the BigQuery merge key.
_DATASET_ID_PATTERN = re.compile(r"^[0-9a-f]{24}$")
# time_grain is embedded as a SQL string literal; keep it a short token.
_TIME_GRAIN_PATTERN = re.compile(r"^[A-Za-z0-9]{1,8}$")
# Period format is embedded as the first PARSE_DATE argument; restrict to the
# strptime tokens periods actually need.
_PERIOD_FORMAT_PATTERN = re.compile(r"^[%A-Za-z0-9._/\- ]{1,16}$")
# floor_period is a recipe's earliest-available scrape month, written "YYYY-MM"
# (month 01..12). It is not embedded in SQL; the request builder reads it to clamp
# the scrape start. A strict shape keeps a typo from masquerading as a valid floor.
_FLOOR_PERIOD_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
# floor_date is the date-grain analog of floor_period: a daily recipe's earliest-
# available scrape day, written "YYYY-MM-DD". The request builder reads it to clamp
# the ISO start_date of a date-range recipe (e.g. yfinance, whose series starts
# 2015-01-01). Like floor_period it is request-side only, never embedded in SQL.
_FLOOR_DATE_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
# A scrape block's recipe names a scraper registry key ("<target>.<flow>"); param
# keys bind to the recipe's keyword-only signature, so both stay identifier slugs.
_SCRAPE_RECIPE_PATTERN = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+$")
_SCRAPE_PARAM_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,64}$")

_DEFAULT_PERIOD_COLUMN = "시점"
_DEFAULT_PERIOD_FORMAT = "%Y.%m"
_DEFAULT_TIME_GRAIN = "M"

_CATEGORY_LEVEL_KEYS = ("category_0", "category_1", "category_2")


def _reject_sql_unsafe(value: str, label: str) -> str:
    # The value is embedded into SQL as a single-quoted literal and, for column
    # names, into a JSON path wrapped in double quotes; reject the characters that
    # would break out of either so a bad config edit fails here, not mid pipeline.
    if "'" in value or '"' in value or "\\" in value:
        raise ValueError(f"{label} must not contain quotes or backslashes")
    return value


def _require_str(entry: dict, key: str, label: str) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must set {key} to a non-empty string")
    return _reject_sql_unsafe(value, f"{label}.{key}")


def _optional_str(entry: dict, key: str, label: str) -> str | None:
    if key not in entry or entry.get(key) in (None, ""):
        return None
    value = entry.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{label}.{key} must be a string when present")
    return _reject_sql_unsafe(value, f"{label}.{key}")


def parse_floor_period(value: Any, label: str) -> tuple[int, int]:
    """Parse a ``"YYYY-MM"`` floor string into a ``(year, month)`` tuple."""
    if not isinstance(value, str):
        raise ValueError(f"{label} floor_period must be a 'YYYY-MM' string")
    match = _FLOOR_PERIOD_PATTERN.fullmatch(value.strip())
    if not match:
        raise ValueError(
            f"{label} floor_period must be formatted 'YYYY-MM' with month 01..12, "
            f"got {value!r}"
        )
    return int(match.group(1)), int(match.group(2))


def parse_floor_date(value: Any, label: str) -> str:
    """Parse a ``"YYYY-MM-DD"`` floor string, returning it normalized (stripped)."""
    if not isinstance(value, str):
        raise ValueError(f"{label} floor_date must be a 'YYYY-MM-DD' string")
    stripped = value.strip()
    if not _FLOOR_DATE_PATTERN.fullmatch(stripped):
        raise ValueError(
            f"{label} floor_date must be formatted 'YYYY-MM-DD' with a valid "
            f"month/day, got {value!r}"
        )
    return stripped


def _parse_floor(parsed: dict, source: str) -> tuple[int, int] | None:
    """A recipe's earliest-available scrape period, ``(year, month)`` or None.

    KOSA hard-fails (by design) a request whose start precedes a metric's earliest
    published period, so the request builder clamps the scrape start up to this
    floor. Each floor is per recipe (e.g. steel_scrap_domestic reaches back to
    1979-01, ~20 years before the others). Optional: sources with no static floor
    (the dpanda Bloomberg recipes) omit ``floor_period`` and return None.
    """
    if "floor_period" not in parsed or parsed.get("floor_period") in (None, ""):
        return None
    return parse_floor_period(parsed["floor_period"], source)


def _parse_datasource(parsed: dict, source: str) -> dict[str, str]:
    datasource = parsed.get("datasource")
    if not isinstance(datasource, dict):
        raise ValueError(f"{source} must set datasource to a json object")
    return {
        "name": _require_str(datasource, "name", f"{source} datasource"),
        "description": _require_str(
            datasource, "description", f"{source} datasource"
        ),
    }


def _parse_category(entry: dict, label: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise ValueError(f"{label} category must be a json object")
    name = _require_str(entry, "name", f"{label} category")
    if not _NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"{label} category name must match {_NAME_PATTERN.pattern} "
            "(an identifier slug, no spaces)"
        )
    category: dict[str, Any] = {"name": name}
    for level in _CATEGORY_LEVEL_KEYS:
        category[level] = _optional_str(entry, level, f"{label} category")
    category["description"] = _optional_str(entry, "description", f"{label} category")
    return category


def _parse_match(entry: dict, label: str) -> dict[str, str]:
    match = entry.get("match")
    if not isinstance(match, dict) or not match:
        raise ValueError(f"{label} must set match to a non-empty json object")
    parsed: dict[str, str] = {}
    for key, value in match.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"{label} match keys must be non-empty strings")
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"{label} match value for {key} must be a non-empty string"
            )
        _reject_sql_unsafe(key, f"{label} match key {key}")
        _reject_sql_unsafe(value, f"{label} match value for {key}")
        parsed[key] = value
    return parsed


def _parse_measure(entry: Any, label: str) -> dict[str, Any]:
    """One measured field of a metric: a column plus optional suffix/unit override.

    A measure may be the bare column name (string) or an object
    ``{"column", "suffix"?, "unit"?, "description_suffix"?}``. ``suffix`` (default
    the column) is appended to the metric name so one grain expands into distinct
    field-metrics (e.g. ``..._open``, ``..._volume``); ``unit`` overrides the base
    metric unit (volume/open-interest differ from a price unit).
    """
    if isinstance(entry, str):
        entry = {"column": entry}
    if not isinstance(entry, dict):
        raise ValueError(f"{label} measure must be a string or json object")
    column = _require_str(entry, "column", label)
    suffix = entry.get("suffix", column)
    if not isinstance(suffix, str) or not _NAME_PATTERN.fullmatch(suffix):
        raise ValueError(
            f"{label} measure suffix must match {_NAME_PATTERN.pattern}"
        )
    return {
        "column": column,
        "suffix": suffix,
        "unit": _optional_str(entry, "unit", label),
        "description_suffix": _optional_str(entry, "description_suffix", label),
    }


def _parse_measures(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} measures must be a non-empty json array")
    return [_parse_measure(measure, label) for measure in value]


# Default row field carrying a per-row ordinal rank (stamped at staging, e.g. the
# SHFE trading-day-to-delivery-month delta via ``stamp_contract_ranks``).
_RANK_COLUMN_DEFAULT = "contract_rank"
# A curve is short; this is generous headroom so a typo'd count fails rather than
# silently generating thousands of metrics.
_RANK_EXPANSION_MAX = 60


def _parse_rank_expansion(entry: Any, label: str) -> dict[str, Any]:
    """One ``rank_expansion`` block: fan a metric across a per-period ordinal rank.

    A metric (after any ``measures`` expansion) is duplicated into ``count`` copies,
    each pinned to one rank ``0..count-1`` on the ``column`` row field and named
    ``<name>_<rank>`` -- the SHFE daily-futures shape, where each contract is ranked
    by its delivery month's delta from the trading day and one metric becomes
    ``<name>_open_0 .. <name>_open_12``. ``column`` (default ``contract_rank``) is the
    staging-stamped rank field the match filters on; the rank is the **trailing**
    name suffix so it composes with a measure suffix as ``<base>_<measure>_<rank>``.
    """
    if not isinstance(entry, dict):
        raise ValueError(f"{label} rank_expansion must be a json object")
    unknown = sorted(set(entry) - {"column", "count", "description_suffix"})
    if unknown:
        raise ValueError(
            f"{label} rank_expansion has unknown keys {unknown}; only "
            "column/count/description_suffix are allowed"
        )
    column = entry.get("column", _RANK_COLUMN_DEFAULT)
    if not isinstance(column, str) or not column:
        raise ValueError(f"{label} rank_expansion column must be a non-empty string")
    _reject_sql_unsafe(column, f"{label} rank_expansion column")
    count = entry.get("count")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or not 1 <= count <= _RANK_EXPANSION_MAX
    ):
        raise ValueError(
            f"{label} rank_expansion count must be an integer in "
            f"1..{_RANK_EXPANSION_MAX}"
        )
    return {
        "column": column,
        "count": count,
        "description_suffix": _optional_str(
            entry, "description_suffix", f"{label} rank_expansion"
        ),
    }


def _expand_ranks(
    base_leaves: list[dict[str, Any]],
    rank_expansion: dict[str, Any],
    label: str,
) -> list[dict[str, Any]]:
    """Fan each base leaf metric into one metric per rank ``0..count-1``.

    The rank is appended to the (already measure-suffixed) name, so the final metric
    is ``<name>_<measure>_<rank>``, and added to the leaf's ``match`` on the rank
    column so each stored metric selects exactly its rank's row per period.
    """
    column = rank_expansion["column"]
    count = rank_expansion["count"]
    suffix_label = rank_expansion["description_suffix"]
    ranked: list[dict[str, Any]] = []
    for leaf in base_leaves:
        if leaf.get("previous_year_column"):
            raise ValueError(
                f"{label} rank_expansion is not supported with previous_year_column"
            )
        if column in leaf["match"]:
            raise ValueError(
                f"{label} rank_expansion column {column!r} collides with a match key"
            )
        for rank in range(count):
            ranked_name = f"{leaf['name']}_{rank}"
            if not _NAME_PATTERN.fullmatch(ranked_name):
                raise ValueError(
                    f"{label} rank-expanded metric name {ranked_name!r} must match "
                    f"{_NAME_PATTERN.pattern}"
                )
            description = (
                f"{leaf['description']} ({suffix_label} {rank})"
                if suffix_label
                else f"{leaf['description']} (rank {rank})"
            )
            ranked.append({
                **leaf,
                "name": ranked_name,
                "description": description,
                "match": {**leaf["match"], column: str(rank)},
            })
    return ranked


def _parse_metric(
    entry: dict,
    index: int,
    source: str,
    *,
    default_category: dict[str, Any] | None,
    default_time_grain: str,
    default_measures: list[dict[str, Any]] | None,
    default_rank_expansion: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Parse one config metric into one or more star-schema metrics.

    A metric measures one field (``measure_column``, the KOSA shape) or several
    (``measures`` / a recipe-level default), in which case it expands into one
    metric per field named ``<name>_<suffix>``. An optional ``rank_expansion`` then
    fans every resulting leaf across a per-period ordinal rank, appending ``_<rank>``
    (so ``<name>_<measure>_<rank>``). Returns the fully expanded list.
    """
    label = f"{source} metrics[{index}]"
    if not isinstance(entry, dict):
        raise ValueError(f"{label} must be a json object")

    name = _require_str(entry, "name", label)
    if not _NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"{label} name must match {_NAME_PATTERN.pattern} "
            "(an identifier slug, no spaces)"
        )

    if "category" in entry:
        category = _parse_category(entry["category"], label)
    elif default_category is not None:
        category = default_category
    else:
        raise ValueError(
            f"{label} must set category, or the recipe must set a default category"
        )

    if "time_grain" in entry:
        time_grain = entry["time_grain"]
        if not isinstance(time_grain, str) or not _TIME_GRAIN_PATTERN.fullmatch(
            time_grain
        ):
            raise ValueError(
                f"{label} time_grain must be a short alphanumeric token"
            )
    else:
        time_grain = default_time_grain

    description = _require_str(entry, "description", label)
    base_unit = _optional_str(entry, "unit", label)

    has_measure_column = "measure_column" in entry
    has_measures = "measures" in entry
    if has_measure_column and has_measures:
        raise ValueError(
            f"{label} must set either measure_column or measures, not both"
        )

    if "rank_expansion" in entry:
        rank_expansion = _parse_rank_expansion(entry["rank_expansion"], label)
    else:
        rank_expansion = default_rank_expansion

    if has_measures:
        measures = _parse_measures(entry["measures"], label)
    elif not has_measure_column and default_measures is not None:
        measures = default_measures
    else:
        # Single-field metric (the KOSA shape): name unchanged, optional
        # previous-year split.
        measure_column = _require_str(entry, "measure_column", label)
        base_leaves = [{
            "name": name,
            "description": description,
            "unit": base_unit,
            "match": _parse_match(entry, label),
            "measure_column": measure_column,
            "previous_year_column": _optional_str(
                entry, "previous_year_column", label
            ),
            "time_grain": time_grain,
            "category": category["name"],
            "_category": category,
        }]
        if rank_expansion is None:
            return base_leaves
        return _expand_ranks(base_leaves, rank_expansion, label)

    if "previous_year_column" in entry:
        raise ValueError(
            f"{label} previous_year_column is not supported with measures"
        )

    match = _parse_match(entry, label)
    base_leaves = []
    for measure in measures:
        expanded_name = f"{name}_{measure['suffix']}"
        if not _NAME_PATTERN.fullmatch(expanded_name):
            raise ValueError(
                f"{label} expanded metric name {expanded_name!r} must match "
                f"{_NAME_PATTERN.pattern}"
            )
        suffix_text = measure["description_suffix"] or measure["suffix"]
        base_leaves.append({
            "name": expanded_name,
            "description": f"{description} {suffix_text}",
            "unit": measure["unit"] if measure["unit"] is not None else base_unit,
            "match": match,
            "measure_column": measure["column"],
            "previous_year_column": None,
            "time_grain": time_grain,
            "category": category["name"],
            "_category": category,
        })

    if rank_expansion is None:
        return base_leaves
    return _expand_ranks(base_leaves, rank_expansion, label)


def _collect_categories(
    metrics: list[dict[str, Any]], source: str
) -> list[dict[str, Any]]:
    """Distinct category definitions referenced by the metrics, keyed by name.

    A category name must define one consistent hierarchy: the same name with
    conflicting category_0/1/2/description is a config error (it would make the
    single dim_categories row ambiguous).
    """
    by_name: dict[str, dict[str, Any]] = {}
    for metric in metrics:
        category = metric["_category"]
        existing = by_name.get(category["name"])
        if existing is None:
            by_name[category["name"]] = category
        elif existing != category:
            raise ValueError(
                f"{source} defines category {category['name']!r} with "
                "conflicting hierarchy/description in different metrics"
            )
    return [
        {k: v for k, v in category.items()}
        for category in by_name.values()
    ]


def parse_materials_config(raw: str | dict, source: str) -> dict[str, Any]:
    """Parse and validate one recipe's materials metric mapping.

    Returns a dict with datasource, period_column, period_format, time_grain, a
    distinct categories list, and a non-empty metrics list (each metric carrying
    its category name and resolved time_grain). Raises ValueError with a precise
    message on any malformed entry so a bad config edit fails fast in one obvious
    place.
    """
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{source} must be valid json: {exc}") from exc
    else:
        parsed = raw

    if not isinstance(parsed, dict):
        raise ValueError(f"{source} must be a json object")

    period_column = parsed.get("period_column", _DEFAULT_PERIOD_COLUMN)
    if not isinstance(period_column, str) or not period_column:
        raise ValueError(f"{source} period_column must be a non-empty string")
    _reject_sql_unsafe(period_column, f"{source} period_column")

    period_format = parsed.get("period_format", _DEFAULT_PERIOD_FORMAT)
    if not isinstance(period_format, str) or not _PERIOD_FORMAT_PATTERN.fullmatch(
        period_format
    ):
        raise ValueError(
            f"{source} period_format must be a short strptime pattern "
            f"matching {_PERIOD_FORMAT_PATTERN.pattern}"
        )

    time_grain = parsed.get("time_grain", _DEFAULT_TIME_GRAIN)
    if not isinstance(time_grain, str) or not _TIME_GRAIN_PATTERN.fullmatch(
        time_grain
    ):
        raise ValueError(
            f"{source} time_grain must be a short alphanumeric token"
        )

    default_category = (
        _parse_category(parsed["category"], source)
        if "category" in parsed
        else None
    )

    default_measures = (
        _parse_measures(parsed["measures"], source)
        if "measures" in parsed
        else None
    )

    default_rank_expansion = (
        _parse_rank_expansion(parsed["rank_expansion"], source)
        if "rank_expansion" in parsed
        else None
    )

    raw_metrics = parsed.get("metrics")
    if not isinstance(raw_metrics, list) or not raw_metrics:
        raise ValueError(f"{source} metrics must be a non-empty json array")

    metrics = []
    seen_names = set()
    for index, entry in enumerate(raw_metrics):
        for metric in _parse_metric(
            entry,
            index,
            source,
            default_category=default_category,
            default_time_grain=time_grain,
            default_measures=default_measures,
            default_rank_expansion=default_rank_expansion,
        ):
            if metric["name"] in seen_names:
                raise ValueError(
                    f"{source} has duplicate metric name {metric['name']}"
                )
            seen_names.add(metric["name"])
            metrics.append(metric)

    categories = _collect_categories(metrics, source)

    return {
        "datasource": _parse_datasource(parsed, source),
        "period_column": period_column,
        "period_format": period_format,
        "time_grain": time_grain,
        "floor": _parse_floor(parsed, source),
        "categories": categories,
        "metrics": metrics,
    }


def config_path(recipe: str) -> Path:
    # Config files are grouped into per-datasource subdirectories named by the
    # recipe's datasource prefix (the part before the first dot), e.g.
    # ``eia/eia.prod_world.json`` or ``yfinance/yfinance.fx_tryusd.json``. Recipes
    # whose datasource has no subdirectory stay flat under CONFIG_DIR (e.g.
    # ``kosis.manufacturing_operation.json``), so fall back to the flat path when the grouped
    # one is absent -- which also gives is_file()/has_materials_config a stable
    # path to report False for recipes with no committed config at all.
    datasource = recipe.split(".", 1)[0]
    grouped = CONFIG_DIR / datasource / f"{recipe}.json"
    if grouped.is_file():
        return grouped
    return CONFIG_DIR / f"{recipe}.json"


def has_materials_config(recipe: str) -> bool:
    """True when a mapping file exists for the recipe (drives DAG skip)."""
    return config_path(recipe).is_file()


def load_recipe_floor(recipe: str) -> tuple[int, int] | None:
    """Read just a recipe's scrape floor ``(year, month)``, or None when unset.

    Used at request-build time to clamp the scrape start before invoking the
    scraper. Reads only ``floor_period`` so it stays decoupled from the
    transform-load config's metric validation -- a recipe legitimately scrapes
    (build -> execute) before its full metric mapping lands. Returns None when the
    recipe has no config file or the file declares no floor.
    """
    path = config_path(recipe)
    if not path.is_file():
        return None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"materials metrics file {recipe}.json must be valid json: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"materials metrics file {recipe}.json must be a json object")
    return _parse_floor(parsed, f"materials metrics file {recipe}.json")


def load_recipe_date_floor(recipe: str) -> str | None:
    """Read just a date-grain recipe's scrape floor ``"YYYY-MM-DD"``, or None.

    The date-range analog of :func:`load_recipe_floor`: used at request-build time to
    clamp the ISO ``start_date`` of a daily recipe (e.g. yfinance) up to its earliest-
    available day before invoking the scraper. Reads only ``floor_date`` so it stays
    decoupled from the transform-load config's metric validation (a recipe legitimately
    scrapes before its full metric mapping lands). Returns None when the recipe has no
    config file or the file declares no ``floor_date``.
    """
    path = config_path(recipe)
    if not path.is_file():
        return None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"materials metrics file {recipe}.json must be valid json: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"materials metrics file {recipe}.json must be a json object")
    if "floor_date" not in parsed or parsed.get("floor_date") in (None, ""):
        return None
    return parse_floor_date(
        parsed["floor_date"], f"materials metrics file {recipe}.json"
    )


def parse_scrape_block(parsed: dict, source: str) -> dict[str, Any] | None:
    """Parse a config file's optional top-level ``scrape`` block, or None when absent.

    The scrape block turns a materials config file into a self-contained recipe
    registration (the dpanda_bloomberg model): instead of the DAG's static SOURCES
    entry pointing at a scraper recipe that *bakes* its series identity, the file
    names one of the scraper's PARAMETERIZED recipes plus the identity params it
    should be invoked with::

        "scrape": {
            "recipe": "fred.series",
            "params": {"series_id": "DCOILWTICO"}
        }

    ``recipe`` is the scraper registry key (``"<target>.<flow>"``); ``params`` is an
    optional flat object of JSON scalars -- or flat lists of scalars (gacc's
    ``aliases``) -- merged into the request params (the DAG adds the shared period
    window on top per the source's period kind, and conf can override them per run
    like any recipe query). Values are request-side only -- never embedded in SQL --
    but keys must bind to the recipe's keyword signature, so both are kept to
    identifier slugs and malformed entries fail loudly here.
    """
    if "scrape" not in parsed or parsed.get("scrape") in (None, {}):
        return None
    scrape = parsed["scrape"]
    if not isinstance(scrape, dict):
        raise ValueError(f"{source} scrape must be a json object")

    unknown = sorted(set(scrape) - {"recipe", "params"})
    if unknown:
        raise ValueError(
            f"{source} scrape has unknown keys {unknown}; only recipe/params are allowed"
        )

    recipe = scrape.get("recipe")
    if not isinstance(recipe, str) or not _SCRAPE_RECIPE_PATTERN.fullmatch(recipe):
        raise ValueError(
            f"{source} scrape recipe must be a '<target>.<flow>' scraper registry "
            f"key matching {_SCRAPE_RECIPE_PATTERN.pattern}, got {recipe!r}"
        )

    params = scrape.get("params", {})
    if not isinstance(params, dict):
        raise ValueError(f"{source} scrape params must be a json object")
    def _ensure_scalar(key: str, value: Any) -> None:
        if isinstance(value, bool):
            return
        if not isinstance(value, (str, int, float)):
            raise ValueError(
                f"{source} scrape param {key} must be a JSON scalar "
                f"(string/number/bool), got {type(value).__name__}"
            )

    for key, value in params.items():
        if not isinstance(key, str) or not _SCRAPE_PARAM_KEY_PATTERN.fullmatch(key):
            raise ValueError(
                f"{source} scrape param keys must match "
                f"{_SCRAPE_PARAM_KEY_PATTERN.pattern}, got {key!r}"
            )
        if isinstance(value, list):
            # One level of list-of-scalars (e.g. gacc's aliases); nesting stays out.
            for item in value:
                _ensure_scalar(key, item)
            continue
        _ensure_scalar(key, value)

    return {"recipe": recipe, "params": dict(params)}


def discover_scrape_recipes(source: str) -> dict[str, dict[str, Any]]:
    """Config-declared dynamic recipe instances of one source.

    Scans the source's committed config files (``CONFIG_DIR/<source>/<source>.*.json``
    plus the flat ``CONFIG_DIR/<source>.*.json`` fallback, mirroring ``config_path``)
    for a top-level ``scrape`` block. Returns ``{instance: {"recipe", "params"}}``
    where *instance* is the file stem -- the identity the whole pipeline keys on
    (request/result/staging objects, floor clamp, trigger-form entry, normalize, and
    the transform-load mapping), exactly like a statically-registered recipe -- and
    ``recipe`` is the parameterized scraper registry key the request payload names.

    The scrape recipe must belong to the instance's source (same ``<target>.`` prefix)
    so the DAG's per-source credential env injection and the scraper target line up; a
    mismatch fails loudly. Files without a scrape block are the normal
    statically-registered case and are skipped. A grouped and a flat file with the
    same stem resolve to the grouped one (``config_path``'s precedence).
    """
    grouped_dir = CONFIG_DIR / source
    candidates: list[Path] = []
    if grouped_dir.is_dir():
        candidates.extend(sorted(grouped_dir.glob(f"{source}.*.json")))
    candidates.extend(sorted(CONFIG_DIR.glob(f"{source}.*.json")))

    instances: dict[str, dict[str, Any]] = {}
    for path in candidates:
        instance = path.stem
        if instance in instances:  # grouped file shadows its flat twin
            continue
        label = f"materials metrics file {path.name}"
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{label} must be valid json: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{label} must be a json object")
        scrape = parse_scrape_block(parsed, label)
        if scrape is None:
            continue
        if scrape["recipe"].split(".", 1)[0] != source:
            raise ValueError(
                f"{label} scrape recipe {scrape['recipe']!r} must belong to "
                f"source {source!r} (same '<target>.' prefix)"
            )
        instances[instance] = scrape
    return instances


def grain_targets_from_config(recipe: str) -> list[dict[str, str]]:
    """Unique Mongo extraction grains for a grain-based recipe.

    The materials config is the single source of truth for each grain's identity,
    description, and freq. Collapses the recipe's metrics to one target per
    ``(dataset_id, grain_id)`` pair -- the Mongo compound key and every BigQuery
    merge key -- carrying the curated ``description`` and the resolved
    ``time_grain`` as ``freq``. Returns the targets a category's extractor pulls
    from Mongo; ``[]`` when the recipe has no config file.

    Only metrics whose ``match`` names both a ``grain_id`` and a ``dataset_id``
    contribute, so column-matched recipes (KOSA) yield no grain targets. A metric
    with ``enabled: false`` drops its grain from extraction. Two metrics naming the
    same pair must agree on description and freq, or the config contradicts itself
    and fails loudly. Validates the same grain-identity fields the old grain
    catalog did, so a bad config edit fails here rather than mid pipeline.
    """
    path = config_path(recipe)
    if not path.is_file():
        return []
    source = f"materials metrics file {recipe}.json"
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{source} must be valid json: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{source} must be a json object")

    recipe_time_grain = parsed.get("time_grain", _DEFAULT_TIME_GRAIN)
    metrics = parsed.get("metrics")
    if not isinstance(metrics, list):
        return []

    targets: list[dict[str, str]] = []
    by_pair: dict[tuple[str, str], dict[str, str]] = {}
    for index, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            continue
        match = metric.get("match")
        if not isinstance(match, dict):
            continue
        grain_id = match.get("grain_id")
        dataset_id = match.get("dataset_id")
        # Column-matched metrics (no grain identity) name no extraction grain.
        if not grain_id or not dataset_id:
            continue
        # A disabled metric drops its grain from the Mongo pull, mirroring the
        # old grain catalog's enabled flag.
        if metric.get("enabled", True) is False:
            continue

        label = f"{source} metrics[{index}]"
        if not isinstance(dataset_id, str) or not _DATASET_ID_PATTERN.fullmatch(
            dataset_id
        ):
            raise ValueError(
                f"{label} match dataset_id must be a twenty four character "
                "lowercase hex object id"
            )
        if not isinstance(grain_id, str):
            raise ValueError(f"{label} match grain_id must be a string")
        description = metric.get("description")
        if not isinstance(description, str) or not description:
            raise ValueError(f"{label} must set description to a non-empty string")
        _reject_sql_unsafe(description, f"{label}.description")
        freq = metric.get("time_grain", recipe_time_grain)
        if not isinstance(freq, str) or not _TIME_GRAIN_PATTERN.fullmatch(freq):
            raise ValueError(
                f"{label} time_grain must be a short alphanumeric token"
            )

        target = {
            "dataset_id": dataset_id,
            "grain_id": grain_id,
            "description": description,
            "freq": freq,
        }
        pair = (dataset_id, grain_id)
        existing = by_pair.get(pair)
        if existing is not None:
            if existing != target:
                raise ValueError(
                    f"{source} describes grain {pair} with conflicting "
                    "description/time_grain in different metrics"
                )
            continue
        by_pair[pair] = target
        targets.append(target)

    return targets


def load_materials_config(recipe: str) -> dict[str, Any] | None:
    """Read and parse one recipe's mapping, or None when no file exists.

    Every metric carries its own ``description`` and ``time_grain`` inline
    (``time_grain`` falling back to the recipe default); grain-based recipes are
    the single source of truth for those, mirrored into the extraction grains by
    ``grain_targets_from_config``.
    """
    path = config_path(recipe)
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"materials metrics file {recipe}.json must be valid json: {exc}"
        ) from exc
    return parse_materials_config(
        parsed,
        source=f"materials metrics file {recipe}.json",
    )
