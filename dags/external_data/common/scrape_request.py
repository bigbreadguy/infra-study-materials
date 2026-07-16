"""Pure helpers for building scraper Cloud Run Job requests.

Kept free of Airflow imports so they can be unit-tested directly (mirrors how
common/gcs_object.py is structured and tested). The DAG supplies the Airflow
context; these functions only do data shaping and date math.
"""

from __future__ import annotations

import calendar
import re
from datetime import date, timedelta
from typing import Any
from typing import Mapping


# Wire-contract version shared with the scraper job (PRD section 6). Bump in lockstep.
SCHEMA_VERSION = "1"

# ISO 'YYYY-MM-DD' shape for the date-range (daily-grain) recipes. The clamp below
# compares dates as strings, which is only sound for this fixed-width zero-padded
# format, so a malformed date fails loudly rather than mis-sorting.
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_year_month(year: int, month: int, lookback_months: int = 0) -> tuple[int, int]:
    """Shift (year, month) back by ``lookback_months``, handling year rollover.

    The DAG converts the run point to KST first, then passes its year/month here.
    KOSA publishes monthly statistics with a lag, so a lookback avoids requesting a
    not-yet-published month. Pure integer math keeps this trivially testable.
    """

    if month < 1 or month > 12:
        raise ValueError(f"month must be 1..12, got {month}")
    if lookback_months < 0:
        raise ValueError(f"lookback_months must be >= 0, got {lookback_months}")

    total = year * 12 + (month - 1) - lookback_months
    return total // 12, total % 12 + 1


def month_window_range(
    end_year: int, end_month: int, window_months: int
) -> tuple[int, int, int, int]:
    """Closed ``[end-(window-1) .. end]`` month range from a window width.

    The daily-monitor default for month-grain sources: a width-``window`` window
    *ending* at the run's current month, so e.g. ``window=2`` is ``[prev .. current]``.
    A range (not a single month) keeps KOSIS robust day-to-day -- it returns the
    published earlier month and simply omits the not-yet-published current one rather
    than erroring -- and re-scraping the earlier month daily is an idempotent no-op
    through the ``fact_values`` MERGE. ``window`` is floored at 1 (a 1-month window is
    the single current month). Pure integer math via :func:`resolve_year_month`.
    """

    window = max(1, int(window_months))
    start_year, start_month = resolve_year_month(end_year, end_month, window - 1)
    return start_year, start_month, end_year, end_month


def year_month_from_iso(iso_date: str) -> tuple[int, int]:
    """Extract ``(year, month)`` from an ISO ``YYYY-MM-DD`` date.

    Lets one unified ISO ``start_date``/``end_date`` range drive the month-grain
    sources too: the operator names dates, and the month-grain branch derives the
    enclosing ``(year, month)`` bounds from them.
    """

    _ensure_iso_date(iso_date, "date")
    return int(iso_date[:4]), int(iso_date[5:7])


def clamp_period_to_floor(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    floor_year: int,
    floor_month: int,
) -> tuple[int, int, int, int] | None:
    """Clamp a ``[start..end]`` month range up to a per-recipe floor.

    KOSA hard-fails (by design) a scrape whose start precedes a metric's earliest
    published period, so the caller raises ``start`` to the recipe's floor first.
    Each window end is a single joint ``(year, month)`` period compared as an
    ordered pair -- never field by field, so a below-floor month inside an
    above-floor year is left alone rather than "corrected" into a wrong date.

    Returns the adjusted ``(start_year, start_month, end_year, end_month)``:

    - the whole range is below the floor (even ``end`` < floor) -> ``None``: the
      caller treats it as an empty/invalid request and emits no range to the
      scraper rather than an inverted one;
    - only ``start`` is below the floor -> ``start`` raised to the floor, ``end``
      untouched;
    - otherwise -> the range returned unchanged.

    Pure integer math with no logging: the caller compares the returned start
    against its input and logs at WARN when the clamp actually moved it, so a
    masked bug (a wrong year from an unset default or typo) stays observable.
    """

    floor = (floor_year, floor_month)
    if (end_year, end_month) < floor:
        return None
    if (start_year, start_month) < floor:
        return floor_year, floor_month, end_year, end_month
    return start_year, start_month, end_year, end_month


def _ensure_iso_date(value: str, label: str) -> str:
    if not isinstance(value, str) or not _ISO_DATE_PATTERN.fullmatch(value):
        raise ValueError(f"{label} must be an ISO 'YYYY-MM-DD' date, got {value!r}")
    return value


def month_bounds_iso(year: int, month: int) -> tuple[str, str]:
    """Return the first and last calendar day of ``(year, month)`` as ISO strings.

    The date-range (daily-grain) recipes derive their default window from the run's
    month: a scheduled ``@monthly`` run pulls the whole resolved month of daily rows
    (``[first day .. last day]``), mirroring how the month-grain recipes derive a
    single month from the run date. Pure ``calendar`` math keeps this testable.
    """

    if month < 1 or month > 12:
        raise ValueError(f"month must be 1..12, got {month}")
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def recent_days_range_iso(end_date: str, window_days: int) -> tuple[str, str]:
    """Closed ISO ``[end-(window-1) .. end]`` date range from a window width.

    The daily-monitor default for date-grain (daily) sources. The DAG passes
    *yesterday* (KST) as ``end`` so the window never requests a not-yet-traded
    "today", and a width of a few days guarantees the range spans at least one
    trading day across weekends/holidays (so an empty result means real breakage and
    is left to fail, not a benign no-data day). ``window`` is floored at 1. Pure
    ``date`` math; ``end`` is validated as a fixed-width ISO string.
    """

    _ensure_iso_date(end_date, "end_date")
    days = max(1, int(window_days))
    end = date.fromisoformat(end_date)
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end_date


def clamp_date_range_to_floor(
    start_date: str,
    end_date: str,
    floor_date: str,
) -> tuple[str, str] | None:
    """Clamp a closed ISO ``[start..end]`` date range up to a per-recipe date floor.

    The daily-grain analog of :func:`clamp_period_to_floor`: a series has an
    earliest-available date (e.g. ``TRYUSD=X`` history starts ``2015-01-01``), so the
    caller raises ``start`` to that floor before scraping. Dates are compared as
    fixed-width ISO strings (chronological == lexicographic for ``YYYY-MM-DD``):

    - the whole range is below the floor (even ``end`` < floor) -> ``None`` (an
      empty/invalid request, like the month clamp);
    - only ``start`` is below the floor -> ``start`` raised to the floor, ``end``
      untouched;
    - otherwise -> the range returned unchanged.

    Pure with no logging: the caller compares the returned start against its input
    and logs at WARN when the clamp actually moved it.
    """

    _ensure_iso_date(start_date, "start_date")
    _ensure_iso_date(end_date, "end_date")
    _ensure_iso_date(floor_date, "floor_date")
    if end_date < floor_date:
        return None
    if start_date < floor_date:
        return floor_date, end_date
    return start_date, end_date


def clamp_date_range_to_ceiling(
    start_date: str,
    end_date: str,
    ceiling_date: str,
) -> tuple[str, str] | None:
    """Clamp a closed ISO ``[start..end]`` date range down to a latest-allowed date.

    The mirror of :func:`clamp_date_range_to_floor`: a date-grain scrape must never
    request a day whose data is not yet published. The monitor default already ends
    at yesterday (KST); an explicit backfill window must obey the same ceiling
    (e.g. shfe's daily kx file is served intraday with every settlement price blank
    until after the Beijing close, so requesting "today" fails or scrapes nothing).
    Dates are compared as fixed-width ISO strings:

    - the whole range is above the ceiling (even ``start`` > ceiling) -> ``None``
      (nothing published to request);
    - only ``end`` is above the ceiling -> ``end`` lowered to the ceiling,
      ``start`` untouched;
    - otherwise -> the range returned unchanged.

    Pure with no logging: the caller compares the returned end against its input
    and logs at WARN when the clamp actually moved it.
    """

    _ensure_iso_date(start_date, "start_date")
    _ensure_iso_date(end_date, "end_date")
    _ensure_iso_date(ceiling_date, "ceiling_date")
    if start_date > ceiling_date:
        return None
    if end_date > ceiling_date:
        return start_date, ceiling_date
    return start_date, end_date


def split_date_range_yearly(
    start_date: str, end_date: str
) -> list[tuple[str, str]]:
    """Split a closed ISO ``[start..end]`` date range into per-calendar-year chunks.

    A yearly-chunked source (shfe) fetches every trading day of its window
    serially inside ONE Cloud Run execution, so a decades-deep backfill window in
    a single request exceeds the job's task timeout. The caller builds one request
    per returned chunk instead, bounding each execution to at most one year of
    trading days. Chunks are contiguous and closed on both ends: the first starts
    at ``start_date``, the last ends at ``end_date``, and interior years span
    ``01-01..12-31``. A window inside a single year returns exactly one chunk
    (the input range). Pure string math on the fixed-width ISO format; an
    inverted range fails loudly rather than returning an empty split.
    """

    _ensure_iso_date(start_date, "start_date")
    _ensure_iso_date(end_date, "end_date")
    if end_date < start_date:
        raise ValueError(
            f"end_date {end_date!r} must not precede start_date {start_date!r}"
        )

    start_year, end_year = int(start_date[:4]), int(end_date[:4])
    return [
        (
            start_date if year == start_year else f"{year:04d}-01-01",
            end_date if year == end_year else f"{year:04d}-12-31",
        )
        for year in range(start_year, end_year + 1)
    ]


def merge_params(defaults: Mapping[str, Any], conf: Mapping[str, Any] | None) -> dict[str, Any]:
    """Overlay dag_run.conf onto the default query params (conf wins)."""

    merged = dict(defaults)
    if conf:
        for key, value in conf.items():
            merged[key] = value
    return merged


def build_request_payload(recipe: str, params: Mapping[str, Any]) -> dict[str, Any]:
    """Assemble the {schema_version, recipe, params} request object (PRD 6.1).

    output_uri is intentionally absent: Airflow passes it as the OUTPUT_URI env
    override, and the job treats it as opaque (PRD 6.1 / Q4).
    """

    if not recipe:
        raise ValueError("recipe must be a non-empty string")
    return {
        "schema_version": SCHEMA_VERSION,
        "recipe": recipe,
        "params": dict(params),
    }


def request_object_name(run_id: str, recipe: str, chunk: str | None = None) -> str:
    # One DAG run fans out across several recipes, so the object is keyed by both the
    # run_id and the recipe to avoid collisions between recipes of the same run. A
    # yearly-chunked backfill (split_date_range_yearly) builds several requests per
    # recipe, so each chunk's objects additionally carry the chunk key (its year).
    suffix = f".{chunk}" if chunk else ""
    return f"scrape/requests/{run_id}/{recipe}{suffix}.json"


def result_object_name(run_id: str, recipe: str, chunk: str | None = None) -> str:
    suffix = f".{chunk}" if chunk else ""
    return f"scrape/results/{run_id}/{recipe}{suffix}.json"


def staging_object_name(run_id: str, recipe: str) -> str:
    # NDJSON re-emit of a result's records, read by the BigQuery transform-load
    # through a job-scoped external table. Keyed by run_id + recipe like the
    # request/result objects.
    return f"scrape/staging/{run_id}/{recipe}.ndjson"


def staging_source_wildcard(run_id: str, source: str) -> str:
    # A wildcard over all of one source's staged objects for a run. staging objects
    # are ``scrape/staging/{run_id}/{recipe}.ndjson`` and a recipe is
    # ``{source}.{flow}``, so ``{source}.*`` matches exactly this source's recipes
    # (and only this run's, since run_id is in the path). The per-source combined
    # load reads this through one job-scoped external table.
    return f"scrape/staging/{run_id}/{source}.*"


def gcs_uri(bucket: str, object_name: str) -> str:
    return f"gs://{bucket}/{object_name}"
