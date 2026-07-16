"""Pure helpers turning a scraper result envelope into BigQuery-ready NDJSON.

The scraper writes one pretty-printed envelope object per recipe
(``{recipe, params, status, data: [...], ...}``). BigQuery external tables need
newline-delimited JSON, and the records' keys are Korean and contain spaces
(e.g. ``"국내수입 물량"``), which are awkward as external-table column names. So
each record is wrapped under an ASCII ``row`` JSON column; the transform SQL then
reads fields via ``JSON_VALUE(row, '$["국내수입 물량"]')``.

Non-finite floats (``NaN``/``Infinity``/``-Infinity``) in the records are coerced
to JSON ``null`` before serialization (see :func:`_json_safe`): ``json.dumps``
defaults to emitting them as bare tokens, which are not legal JSON and abort
BigQuery's NDJSON reader *before* any SQL filter runs. yfinance returns ``NaN``
for a not-yet-settled day, so this keeps the staged file valid; the transform's
candidate filter then drops the null value naturally.

Kept free of Airflow/GCP imports so it is unit-testable in isolation, mirroring
common/scrape_request.py.
"""

from __future__ import annotations

import json
import math
import re
from typing import Any, Mapping


# KOSA browser recipes deliberately over-fetch the current calendar year: KOSA's
# Steeldata period dropdowns only list already-published months, so a request for an
# unpublished month no-ops the month select and "check all" sweeps the whole
# published year-to-date. The scraper returns that raw superset as status:success
# and does NOT slice (the unsliced envelope stays in GCS as the audit trail) --
# slicing back to the requested range is this pipeline's job. Past-year requests
# return the exact range, so the slice is then a no-op.
_DEFAULT_PERIOD_COLUMN = "시점"


def envelope_records(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the envelope's data records, or [] when there are none.

    Only ``status == "success"`` envelopes contribute rows; a failed or empty
    envelope yields no records so the caller can skip the load cleanly.
    """
    if not isinstance(envelope, dict):
        raise ValueError("result envelope must be a json object")
    if envelope.get("status") != "success":
        return []
    data = envelope.get("data")
    if not data:
        return []
    if not isinstance(data, list):
        raise ValueError("result envelope data must be a json array")
    records = []
    for index, record in enumerate(data):
        if not isinstance(record, dict):
            raise ValueError(f"result envelope data[{index}] must be a json object")
        records.append(record)
    return records


def parse_period_ym(value: Any, *, period_column: str = _DEFAULT_PERIOD_COLUMN) -> tuple[int, int]:
    """Parse a ``"YYYY.MM"`` period field into a ``(year, month)`` tuple.

    Every kosa result row carries a ``시점`` formatted ``"YYYY.MM"`` (e.g.
    ``"2024.01"``); range slicing compares these as integer ``(year, month)``
    tuples. Raises ValueError on a missing or malformed period so a contract
    violation fails loudly rather than silently dropping a row.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"{period_column} must be a 'YYYY.MM' string, got {value!r}"
        )
    parts = value.strip().split(".")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"{period_column} must be formatted 'YYYY.MM', got {value!r}")
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise ValueError(
            f"{period_column} must be formatted 'YYYY.MM', got {value!r}"
        ) from exc


def _period_bound(params: Mapping[str, Any], year_key: str, month_key: str) -> tuple[int, int]:
    try:
        return int(params[year_key]), int(params[month_key])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"request params must carry integer-valued {year_key}/{month_key}"
        ) from exc


def slice_records_to_requested_range(
    records: list[dict[str, Any]],
    params: Mapping[str, Any] | None,
    *,
    period_column: str = _DEFAULT_PERIOD_COLUMN,
) -> list[dict[str, Any]]:
    """Trim records to the requested closed ``[start..end]`` month range.

    Keeps rows where ``(start_year, start_month) <= 시점 <= (end_year, end_month)``,
    comparing on integer ``(year, month)`` tuples parsed from each row's
    ``period_column`` and the request ``params`` (whose months are zero-padded
    strings). Both ends inclusive. Corrects the KOSA current-year over-fetch
    documented above; the slice is **idempotent** -- a no-op when the returned
    range already equals the requested range (the past-year case, where KOSA
    honors the exact range). An empty result is benign (the requested month is not
    published yet); the caller loads zero rows rather than failing.
    """
    if not records:
        return []
    if not isinstance(params, Mapping):
        raise ValueError("result envelope must carry a params object to slice by range")
    start = _period_bound(params, "start_year", "start_month")
    end = _period_bound(params, "end_year", "end_month")
    return [
        record
        for record in records
        if start <= parse_period_ym(record.get(period_column), period_column=period_column) <= end
    ]


# EIA's Venezuela crude series is quarterly, with a ``"YYYY-Q#"`` period BigQuery's
# PARSE_DATE cannot parse (there is no quarter token). Map each quarter to the first day
# of its first month so the row loads with a ``%Y-%m-%d`` period_format.
_QUARTER_PERIOD_PATTERN = re.compile(r"^(\d{4})-Q([1-4])$")
_QUARTER_START_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


def quarter_period_to_iso(value: Any) -> Any:
    """Convert a ``"YYYY-Q#"`` quarter period to its quarter-start ISO date.

    Q1->``YYYY-01-01``, Q2->``YYYY-04-01``, Q3->``YYYY-07-01``, Q4->``YYYY-10-01``. A
    value that is not in quarter form is returned unchanged, so the conversion is
    idempotent (an already-converted date passes through) and safe on any record set.
    """
    if not isinstance(value, str):
        return value
    match = _QUARTER_PERIOD_PATTERN.fullmatch(value.strip())
    if not match:
        return value
    year, quarter = int(match.group(1)), int(match.group(2))
    return f"{year:04d}-{_QUARTER_START_MONTH[quarter]:02d}-01"


def normalize_quarter_periods(
    records: list[dict[str, Any]], period_column: str
) -> list[dict[str, Any]]:
    """Rewrite each record's ``period_column`` from ``YYYY-Q#`` to its quarter-start date.

    Returns new record dicts (originals are not mutated); rows whose period is already a
    date pass through unchanged. Applied at staging for quarterly recipes, whose period
    has no PARSE_DATE quarter token.
    """
    converted: list[dict[str, Any]] = []
    for record in records:
        if period_column in record:
            record = {
                **record,
                period_column: quarter_period_to_iso(record[period_column]),
            }
        converted.append(record)
    return converted


# e-Stat's monthly time-axis code is ``YYYY00MMMM`` -- a 4-digit year, the constant
# ``00``, then the 2-digit month repeated twice (e.g. ``2024000303`` = 2024-03,
# ``2020001111`` = 2020-11). BigQuery cannot PARSE_DATE that, so rewrite it to ``YYYY-MM``
# (which a ``%Y-%m`` period_format parses, day defaulting to 01, like the EIA monthlies).
_ESTAT_TIME_PATTERN = re.compile(r"^(\d{4})00(0[1-9]|1[0-2])\d{2}$")


def estat_time_to_ym(value: Any) -> Any:
    """Convert an e-Stat ``YYYY00MMMM`` monthly time code to ``YYYY-MM``.

    A value not in that form is returned unchanged, so the conversion is idempotent (an
    already-normalized ``YYYY-MM`` passes through) and safe on any record set.
    """
    if not isinstance(value, str):
        return value
    match = _ESTAT_TIME_PATTERN.fullmatch(value.strip())
    if not match:
        return value
    return f"{match.group(1)}-{match.group(2)}"


# e-Stat raw rows key fields with ``@``-prefixes and the value under ``$`` (e.g.
# ``{"@time": ..., "@cat01": ..., "@unit": ..., "$": ...}``). BigQuery's JSON_VALUE path
# escapes a key as ``$."<key>"`` (dot + double quotes; bracket notation is rejected), which
# makes a key literally named ``$`` (``$."$"``) and the ``@``-keys awkward/fragile. Rename
# them to plain ASCII at staging so the materials config reads clean column names.
_ESTAT_KEY_RENAME = {
    "@time": "time",
    "@tab": "tab",
    "@cat01": "cat01",
    "@cat02": "cat02",
    "@cat03": "cat03",
    "@area": "area",
    "@unit": "unit",
    "$": "value",
}


def rename_estat_keys(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rewrite e-Stat's ``@``-prefixed / ``$`` row keys to plain names (see map above).

    Returns new record dicts (originals untouched); keys not in the map pass through. A
    no-op on already-renamed rows, so it is idempotent and safe to apply once at staging.
    """
    return [
        {_ESTAT_KEY_RENAME.get(key, key): value for key, value in record.items()}
        for record in records
    ]


def normalize_estat_periods(
    records: list[dict[str, Any]], period_column: str
) -> list[dict[str, Any]]:
    """Rewrite each record's ``period_column`` from the e-Stat time code to ``YYYY-MM``.

    Returns new record dicts (originals untouched); rows already in ``YYYY-MM`` pass
    through. Applied at staging for estat recipes, whose ``@time`` code BigQuery cannot
    parse directly.
    """
    converted: list[dict[str, Any]] = []
    for record in records:
        if period_column in record:
            record = {
                **record,
                period_column: estat_time_to_ym(record[period_column]),
            }
        converted.append(record)
    return converted


# CFTC Commitments-of-Traders rows carry the report date as an ISO *timestamp*
# ``YYYY-MM-DDT00:00:00.000`` (Socrata's floating-timestamp shape), not a bare date.
# BigQuery's SAFE.PARSE_DATE consumes the whole string, so the trailing ``T00:...``
# makes a ``%Y-%m-%d`` parse return NULL and the row drops silently before the MERGE.
# Strip to the leading date so a ``%Y-%m-%d`` period_format parses it (weekly grain,
# each report_date a Tuesday). The period_format regex also forbids ``:`` and caps at
# 16 chars, so a full timestamp strptime pattern is not an option -- normalize here.
_ISO_TIMESTAMP_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})T")


def iso_timestamp_to_date(value: Any) -> Any:
    """Convert an ISO ``YYYY-MM-DDT...`` timestamp to its leading ``YYYY-MM-DD`` date.

    A value not in that form (already a bare date, or non-string) is returned
    unchanged, so the conversion is idempotent (an already-normalized date passes
    through) and safe on any record set.
    """
    if not isinstance(value, str):
        return value
    match = _ISO_TIMESTAMP_PATTERN.match(value.strip())
    if not match:
        return value
    return match.group(1)


def normalize_iso_timestamp_periods(
    records: list[dict[str, Any]], period_column: str
) -> list[dict[str, Any]]:
    """Rewrite each record's ``period_column`` from an ISO timestamp to its date.

    Returns new record dicts (originals untouched); rows already in ``YYYY-MM-DD``
    pass through. Applied at staging for cftc recipes, whose report-date column is an
    ISO timestamp BigQuery cannot PARSE_DATE with a date-only format.
    """
    converted: list[dict[str, Any]] = []
    for record in records:
        if period_column in record:
            record = {
                **record,
                period_column: iso_timestamp_to_date(record[period_column]),
            }
        converted.append(record)
    return converted


# Leading ``YYYY-MM`` of a period value -- matches both the trading day
# (``YYYY-MM-DD``) and the delivery month (``YYYY-MM``), so one pattern extracts
# the (year, month) pair the rank delta is computed from.
_YEAR_MONTH_PATTERN = re.compile(r"^(\d{4})-(\d{2})")


def _year_month_ordinal(value: Any) -> int | None:
    """Return ``year*12 + (month-1)`` for a leading ``YYYY-MM``, else None."""
    if not isinstance(value, str):
        return None
    match = _YEAR_MONTH_PATTERN.match(value.strip())
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    if not 1 <= month <= 12:
        return None
    return year * 12 + (month - 1)


def stamp_contract_ranks(
    records: list[dict[str, Any]],
    *,
    period_column: str,
    order_column: str = "delivery_month",
    rank_column: str = "contract_rank",
) -> list[dict[str, Any]]:
    """Stamp each futures row with its delivery-month delta from the trading day.

    The SHFE daily-futures scraper emits every listed delivery-month contract per
    trading day (one row per ``(date, delivery_month)``); dominant/near-month
    selection is the DAG's job. Each row's rank is the **calendar month delta**
    between ``order_column`` (``delivery_month``, ``YYYY-MM``) and the month of
    ``period_column`` (the trading day, ``YYYY-MM-DD``), stamped on ``rank_column``
    as a **string**: on 2026-07-13 the al2607 contract (delivery 2026-07) is ``"0"``,
    al2608 is ``"1"``, and so on. The materials config's ``rank_expansion`` then
    turns deltas 0..N-1 into the ``<name>_<rank>`` metrics, so ``<name>_k`` always
    means "the contract delivering k months after the observation month" -- a fixed
    month offset, NOT the k-th listed contract. Ranks are therefore per-row facts,
    not per-day positions, and a day may lack some ranks: once the spot-month
    contract delists mid-month, that day has no ``"0"`` row, and deltas past the
    config's count fall through the merge unmatched (no cap is applied here).

    Returns new record dicts (originals untouched) and never drops a row, so the
    staged count still equals the scraped count (the raw-count ASSERT holds). A row
    whose period or delivery month cannot be parsed as ``YYYY-MM...`` is passed
    through unstamped -- a contract-shape violation the upstream scraper does not
    produce, kept non-fatal so a stray row never fails the whole load (unstamped
    rows match no rank metric and fall through the merge). Input order is preserved.
    """
    stamped: list[dict[str, Any]] = []
    for record in records:
        period_ordinal = _year_month_ordinal(record.get(period_column))
        delivery_ordinal = _year_month_ordinal(record.get(order_column))
        if period_ordinal is None or delivery_ordinal is None:
            stamped.append({**record})
            continue
        stamped.append({**record, rank_column: str(delivery_ordinal - period_ordinal)})
    return stamped


# The five MiFID position-holder categories the LME COT scraper emits per report date;
# the derived open interest is defined over exactly this set (the sum of their total
# longs), so a date missing any of them yields a null open interest rather than a
# silently-partial sum.
_LME_COT_CATEGORIES = frozenset(
    {
        "investment_firms_credit_institutions",
        "investment_funds",
        "other_financial_institutions",
        "commercial_undertakings",
        "compliance_operators",
    }
)


def _minus(long_value: Any, short_value: Any) -> float | int | None:
    """``long - short`` with null propagation (either side missing -> None)."""
    if not isinstance(long_value, (int, float)) or isinstance(long_value, bool):
        return None
    if not isinstance(short_value, (int, float)) or isinstance(short_value, bool):
        return None
    return long_value - short_value


def derive_lme_cot_summary(
    records: list[dict[str, Any]], *, period_column: str
) -> list[dict[str, Any]]:
    """Append one derived ``market_summary`` row per LME COT report date.

    The LME COT scraper emits the *Number of Positions* block as-published: one row per
    (as-of date, MiFID category) with ``risk_reducing`` / ``other`` / ``total`` long/short
    columns. The curated metrics are **derivations across those rows** — the DAG's job,
    like the SHFE contract rank — so this stamps them at staging as one synthetic row per
    ``(period, metal)`` with ``category="market_summary"`` (a value the scraper never
    emits, so it cannot collide), which the materials config's ``match`` selects:

    - ``commercial_long`` / ``commercial_short`` — Commercial Undertakings, risk-reducing
      (positions directly related to commercial activities);
    - ``investment_funds_long`` / ``investment_funds_short`` — Investment Funds, total;
    - the two ``*_net`` — long − short, null-propagating;
    - ``open_interest`` — the sum of ALL five categories' total longs; null when any
      category's total long is missing (never a silently-partial sum).

    Original rows pass through untouched (unmatched rows fall through the merge), so
    later configs can still curate per-category metrics from the same staging.
    """
    by_key: dict[tuple[Any, Any], dict[str, dict[str, Any]]] = {}
    for record in records:
        key = (record.get(period_column), record.get("metal"))
        category = record.get("category")
        if isinstance(category, str):
            by_key.setdefault(key, {})[category] = record

    summaries: list[dict[str, Any]] = []
    for (period, metal), categories in by_key.items():
        commercial = categories.get("commercial_undertakings", {})
        funds = categories.get("investment_funds", {})
        total_longs = [
            categories[name].get("total_long")
            for name in _LME_COT_CATEGORIES
            if name in categories
        ]
        open_interest = (
            sum(total_longs)
            if len(total_longs) == len(_LME_COT_CATEGORIES)
            and all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in total_longs
            )
            else None
        )
        summaries.append(
            {
                period_column: period,
                "metal": metal,
                "category": "market_summary",
                "commercial_long": commercial.get("risk_reducing_long"),
                "commercial_short": commercial.get("risk_reducing_short"),
                "commercial_net": _minus(
                    commercial.get("risk_reducing_long"),
                    commercial.get("risk_reducing_short"),
                ),
                "investment_funds_long": funds.get("total_long"),
                "investment_funds_short": funds.get("total_short"),
                "investment_funds_net": _minus(
                    funds.get("total_long"), funds.get("total_short")
                ),
                "open_interest": open_interest,
            }
        )
    return records + summaries


def _json_safe(value: Any) -> Any:
    """Coerce non-finite floats to None so the result serializes as valid JSON.

    Python's ``json.dumps`` defaults to ``allow_nan=True``, emitting the bare
    tokens ``NaN``/``Infinity``/``-Infinity`` -- none of which are legal JSON, so
    BigQuery's NDJSON reader aborts on them ("Parser terminated before end of
    string") *before* any SQL filter runs. yfinance yields ``NaN`` for a not-yet-
    settled day (today, weekends, holidays), so rewrite every non-finite float to
    JSON ``null``: the file stays valid, and the transform's
    ``NULLIF(JSON_VALUE(...), '') IS NOT NULL`` candidate filter drops the null
    value naturally, while the raw row-count ASSERT still matches (the row is kept,
    just emptied). Recurses through nested dicts/lists so it is source-agnostic.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def records_to_ndjson(records: list[dict[str, Any]]) -> str:
    """Wrap each record under an ASCII ``row`` key and join as NDJSON.

    ensure_ascii=False keeps the Korean values readable in GCS and lets BigQuery
    parse them as UTF-8. Non-finite floats are coerced to JSON ``null`` (see
    :func:`_json_safe`) so the output is always valid JSON. Returns "" for no
    records.
    """
    lines = [
        json.dumps(
            {"row": _json_safe(record)}, ensure_ascii=False, separators=(",", ":")
        )
        for record in records
    ]
    return "\n".join(lines)


def envelope_to_ndjson(envelope: dict[str, Any]) -> str:
    """Convenience: envelope -> wrapped NDJSON (empty string when no rows)."""
    return records_to_ndjson(envelope_records(envelope))
