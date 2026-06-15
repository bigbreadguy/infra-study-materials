"""Pure helpers for building scraper Cloud Run Job requests.

Kept free of Airflow imports so they can be unit-tested directly (mirrors how
common/gcs_object.py is structured and tested). The DAG supplies the Airflow
context; these functions only do data shaping and date math.
"""

from __future__ import annotations

from typing import Any
from typing import Mapping


# Wire-contract version shared with the scraper job (PRD section 6). Bump in lockstep.
SCHEMA_VERSION = "1"


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


def request_object_name(run_id: str) -> str:
    return f"scrape/requests/{run_id}.json"


def result_object_name(run_id: str) -> str:
    return f"scrape/results/{run_id}.json"


def gcs_uri(bucket: str, object_name: str) -> str:
    return f"gs://{bucket}/{object_name}"
