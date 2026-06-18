"""Loader and validator for the gitignored materials metric mapping config.

One JSON file per recipe maps the recipe's Korean result columns to curated
star-schema metrics. The files are local-only (gitignored via /dags/local/) but
live under dags/ so the compose volume mount makes them visible in every Airflow
container -- the same pattern as common/grain_targets.py.

The config is the single source of truth for what gets loaded: which datasource a
recipe belongs to, which rows each metric selects (``match``), which measure
column carries the value, the optional previous-year column to split off, and the
curated metric name/description/unit. A recipe with no config file loads nothing
(the DAG skips its transform-load tasks), so a fresh clone produces no errors.

Every string here is embedded into the transform-load SQL as a literal or a JSON
path, so values are rejected if they carry quotes or backslashes; metric names
are further restricted to a SQL-safe identifier slug.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


# Local-only config files: dags/local/materials_metrics/<recipe>.json, e.g.
# dags/local/materials_metrics/kosa.steel_scrap_import.json
CONFIG_DIR = Path(__file__).resolve().parent.parent / "local" / "materials_metrics"

# Metric names key dim_metrics and are embedded as SQL literals, so keep them to a
# stable identifier slug rather than free text.
_METRIC_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
# time_grain is embedded as a SQL string literal; keep it a short token.
_TIME_GRAIN_PATTERN = re.compile(r"^[A-Za-z0-9]{1,8}$")
# Period format is embedded as the first PARSE_DATE argument; restrict to the
# strptime tokens KOSA periods actually need.
_PERIOD_FORMAT_PATTERN = re.compile(r"^[%A-Za-z0-9._/\- ]{1,16}$")

_DEFAULT_PERIOD_COLUMN = "시점"
_DEFAULT_PERIOD_FORMAT = "%Y.%m"
_DEFAULT_TIME_GRAIN = "M"


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


def _parse_metric(entry: dict, index: int, source: str) -> dict[str, Any]:
    label = f"{source} metrics[{index}]"
    if not isinstance(entry, dict):
        raise ValueError(f"{label} must be a json object")

    name = _require_str(entry, "name", label)
    if not _METRIC_NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"{label} name must match {_METRIC_NAME_PATTERN.pattern} "
            "(an identifier slug, no spaces)"
        )

    return {
        "name": name,
        "description": _require_str(entry, "description", label),
        "unit": _optional_str(entry, "unit", label),
        "match": _parse_match(entry, label),
        "measure_column": _require_str(entry, "measure_column", label),
        "previous_year_column": _optional_str(
            entry, "previous_year_column", label
        ),
    }


def parse_materials_config(raw: str | dict, source: str) -> dict[str, Any]:
    """Parse and validate one recipe's materials metric mapping.

    Returns a dict with datasource, period_column, period_format, time_grain, and
    a non-empty metrics list. Raises ValueError with a precise message on any
    malformed entry so a bad config edit fails fast in one obvious place.
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

    raw_metrics = parsed.get("metrics")
    if not isinstance(raw_metrics, list) or not raw_metrics:
        raise ValueError(f"{source} metrics must be a non-empty json array")

    metrics = []
    seen_names = set()
    for index, entry in enumerate(raw_metrics):
        metric = _parse_metric(entry, index, source)
        if metric["name"] in seen_names:
            raise ValueError(
                f"{source} has duplicate metric name {metric['name']}"
            )
        seen_names.add(metric["name"])
        metrics.append(metric)

    return {
        "datasource": _parse_datasource(parsed, source),
        "period_column": period_column,
        "period_format": period_format,
        "time_grain": time_grain,
        "metrics": metrics,
    }


def config_path(recipe: str) -> Path:
    return CONFIG_DIR / f"{recipe}.json"


def has_materials_config(recipe: str) -> bool:
    """True when a mapping file exists for the recipe (drives DAG skip)."""
    return config_path(recipe).is_file()


def load_materials_config(recipe: str) -> dict[str, Any] | None:
    """Read and parse one recipe's mapping, or None when no file exists."""
    path = config_path(recipe)
    if not path.is_file():
        return None
    return parse_materials_config(
        path.read_text(encoding="utf-8"),
        source=f"materials metrics file {recipe}.json",
    )
