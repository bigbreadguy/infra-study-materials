"""Bloomberg market index manual backfill: MongoDB -> GCS Parquet -> BigQuery.

One parameterized, manually triggered DAG backfills an explicit half-open UTC
window for one category. It exists separately from the scheduled DAGs so the
scheduled path stays a pure D-1 single day and the backfill carries its own
trigger form, raw format, and chunking.

Backfill specifics:

- **Parquet raw, read through a GCS temp table.** The extractor writes one
  Parquet object per grain per year-chunk; the combined merge reads them through
  a job-scoped temporary external table. The merge SQL is identical to the
  scheduled NDJSON path because JSON_VALUE/JSON_QUERY accept the STRING columns
  Parquet stores the Extended-JSON fields in.

- **Per-year chunks.** fact_values is yearly-partitioned, so the merge itself
  has no partition-cap reason to chunk. The run still loops one combined job per
  calendar year to bound per-chunk pod memory (one year of Mongo docs at a time)
  and to make each year an independently idempotent extract + merge, so a failed
  year replays without redoing the whole window.

- **Shared pool, no overlap.** The single task holds the shared ``dpanda_bloomberg``
  size-1 pool for the whole window, so it never overlaps a scheduled run
  mid-MERGE. The trade is that scheduled runs for every category wait behind the
  backfill until it finishes; that is accepted for a deliberate bulk operation.

- **Optional surgical filter.** ``metric_names`` restricts the backfill to the
  grains backing those curated metrics, so a multi-decade backfill of a few daily
  series (e.g. the LME cash/3M prices) pulls only those grains from Mongo and
  uploads only their objects per chunk, instead of dragging the whole category's
  grains through every year. The transform config stays whole; unextracted
  metrics merge no candidates and are untouched. Names must belong to the chosen
  category, so grains spanning categories (copper + nickel) are run once per
  category. Empty backfills the whole category.

Trigger params: category (one of the configured categories), extract_start_date
(UTC, inclusive), extract_end_date (UTC, exclusive), metric_names (optional
list of curated metric names within the category).

Required Airflow Variables and GCP access are identical to the scheduled DAG.
"""

# pyrefly: ignore [missing-import]
import pendulum
# pyrefly: ignore [missing-import]
from pendulum import datetime
# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task

try:
    # pyrefly: ignore [missing-import]
    from airflow.sdk import Param
except ImportError:  # pragma: no cover - import location varies across 3.x
    # pyrefly: ignore [missing-import]
    from airflow.models.param import Param

from external_data.common.dpanda_bloomberg_ingest import (
    CATEGORIES,
    POOL_NAME,
    run_backfill,
)


BACKFILL_DAG_ID = "external_data__dpanda_bloomberg__backfill"


def _resolve_backfill_window(params):
    """Half-open [start, end) UTC window from the trigger params.

    Both dates are required and define a half-open range with no D-1 lookback;
    the operator names the exact interval to bulk-ingest. Each date is floored
    to its UTC day so the window aligns to logical-date partitions.
    """
    raw_start = params.get("extract_start_date")
    raw_end = params.get("extract_end_date")
    if not raw_start or not raw_end:
        raise ValueError(
            "Backfill requires both extract_start_date and extract_end_date"
        )
    start = pendulum.parse(str(raw_start), tz="UTC").start_of("day")
    end = pendulum.parse(str(raw_end), tz="UTC").start_of("day")
    if end <= start:
        raise ValueError(f"Backfill end {end} must be after start {start}")
    return start, end


with DAG(
    dag_id=BACKFILL_DAG_ID,
    description=(
        "Manual Bloomberg market index backfill: MongoDB -> GCS Parquet -> "
        "BigQuery dim and fact merges over an explicit UTC window, one combined "
        "job per calendar year."
    ),
    # Logical dates are managed in zulu time; this DAG never schedules, so the
    # start date only anchors manual runs.
    start_date=datetime(1968, 1, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    # One backfill at a time; it also holds the shared pool, so a second backfill
    # would queue regardless.
    max_active_runs=1,
    # A decades-long backfill is long and idempotent per chunk; an automatic
    # whole-run replay would be expensive and surprising, so the operator reruns
    # on failure instead.
    default_args={"retries": 0},
    params={
        "category": Param(
            default=sorted(CATEGORIES)[0],
            type="string",
            enum=sorted(CATEGORIES),
            title="Category",
            description="Which grain-target category to backfill.",
        ),
        "extract_start_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            title="Backfill start (UTC date, inclusive)",
        ),
        "extract_end_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            title="Backfill end (UTC date, exclusive)",
        ),
        "metric_names": Param(
            default=[],
            type="array",
            items={"type": "string"},
            title="Metric names (optional surgical filter)",
            description=(
                "Curated metric names to backfill (e.g. Com_LME_Cu_Cash). When "
                "set, only the grains backing these metrics are pulled and merged, "
                "so a long backfill of a few series does not drag the whole "
                "category. Names must belong to the selected category. Empty = "
                "backfill the whole category."
            ),
        ),
    },
    tags=[
        "external_data",
        "dpanda",
        "bloomberg",
        "mongo",
        "gcs",
        "bigquery",
        "backfill",
    ],
) as dag:
    @task(pool=POOL_NAME)
    def ingest_backfill() -> dict:
        """Backfill the param window for one category, one job per year.

        Holds the shared pool for the whole window so it never overlaps a
        scheduled run; loops per-year combined BigQuery jobs to bound per-chunk
        pod memory. Idempotent per chunk, so a manual rerun replays safely.
        """
        context = get_current_context()
        params = context["params"]
        category = params["category"]
        if category not in CATEGORIES:
            raise ValueError(
                f"Unknown category {category!r}; expected one of "
                f"{sorted(CATEGORIES)}"
            )
        window = _resolve_backfill_window(params)
        metric_names = params.get("metric_names") or None
        return run_backfill(
            category=category,
            window=window,
            context=context,
            metric_names=metric_names,
        )

    ingest_backfill()
