"""Bloomberg ingestion (scheduled): MongoDB -> GCS NDJSON -> BigQuery dl_materials.

Each configured category owns one @daily DAG and runs as a single task so the
KubernetesExecutor schedules exactly one pod per run (one pod per DAG). The task
pulls one full UTC day of samples for every enabled grain target over a single
MongoDB connection, reshapes each document into the dl_materials ``{"row": {...}}``
record shape, uploads one NDJSON object per grain under a run-scoped GCS prefix,
then transforms every grain in a SINGLE BigQuery job: one multi-statement script
reads all of the run's objects through a job-scoped temporary external table and
bootstraps + merges dim_datasources, dim_categories, dim_metrics, and fact_values
(the shared materials transform) for every metric at once. The compute runs on
Mongo, GCS, and BigQuery; the pod only orchestrates I/O, so collapsing the work
into one pod and one BigQuery job avoids paying pod-start, Atlas connection, and
per-table metadata-mutation costs per grain.

The extract targets the previous completed UTC day (D-1), not the logical date's
own day, because the @daily schedule fires at 00:00Z before the source has
published the in-progress day. Most daily runs are legitimately quiet: alongside
daily grains a category also carries week, month, quarter, and year grains that
only publish on their period boundary, so an interval with no samples for some or
all grains is normal and never warns or fails.

Manual bulk backfill lives in a SEPARATE DAG
(``dpanda_bloomberg_backfill_pipeline.py``): this scheduled DAG only ever runs
the default D-1 single day. Both DAGs write the dl_materials tables, so every
task takes the shared size-1 ``dpanda_bloomberg`` pool, which serializes writes
across all scheduled DAGs and the backfill so a backfill never overlaps a
scheduled run mid-MERGE (per-DAG ``max_active_runs=1`` only serializes within
one DAG). The pool must be provisioned in the deployment.

The ingest engine, observability, and the single-job transform live in
``external_data.common.dpanda_bloomberg_ingest`` (which drives the shared
``external_data.common.materials_bigquery`` transform from the per-category
mapping config ``configs/materials_metrics/dpanda_bloomberg.<category>.json``);
this file only wires the scheduled DAGs and resolves the D-1 window.

Required Airflow Variables (all namespaced dpanda_):
- dpanda_mongo_conn_id, dpanda_mongo_database_name, dpanda_mongo_collection_name
- dpanda_gcs_bucket_name, dpanda_gcs_raw_prefix
- dpanda_bigquery_project_id, dpanda_bigquery_dataset_id, dpanda_bigquery_region

All GCP access runs as the Airflow workload identity service account through
Application Default Credentials, so no Airflow GCP connection is required; per
dev policy tasks use no impersonation chains.
"""

from datetime import timedelta

# pyrefly: ignore [missing-import]
from pendulum import datetime
# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task

from external_data.common.dpanda_bloomberg_ingest import (
    CATEGORIES,
    POOL_NAME,
    ingest,
)


# The @daily schedule fires at 00:00Z, the start of the logical date's own day,
# before the source has published it. Extract the previous completed zulu day.
EXTRACTION_LOOKBACK_DAYS = 1


def _resolve_interval(context):
    # The logical date is authoritative because cron trigger timetables derive
    # the data interval from the trigger wall clock, not from an explicitly
    # supplied logical date. Trigger logical dates must be given as utc midnights
    # or the run resolves to the prior zulu date.
    run_point = context["logical_date"] or context["data_interval_start"]
    if run_point is None:
        raise ValueError(
            "Run provides neither a logical date nor a data interval start "
            "to resolve the extraction date"
        )
    # Resolve the run to its zulu calendar day, then step back one day. The
    # schedule fires at 00:00Z (09:00 KST) at the very start of the logical
    # date's day, when the source has not yet published that day's samples.
    # Targeting the previous, completed zulu day (D-1) gives the source a full
    # day to populate before extraction.
    run_day = run_point.in_timezone("UTC").start_of("day")
    if run_point != run_day:
        print(
            f"Logical date {run_point} is not a zulu midnight; "
            f"resolved to zulu date {run_day.date()}"
        )
    start_date = run_day.subtract(days=EXTRACTION_LOOKBACK_DAYS)
    end_date = start_date.add(days=1)
    return start_date, end_date


def _build_dag(category, settings):
    with DAG(
        dag_id=f"external_data__dpanda_bloomberg__{category}",
        description=(
            "Bloomberg market index samples: MongoDB -> GCS NDJSON -> BigQuery "
            f"dim and fact merges for {category} grain targets."
        ),
        # Logical dates are managed in zulu time because the source database
        # keys day grained rows by utc timestamps.
        start_date=datetime(1968, 1, 2, tz="UTC"),
        schedule=settings["schedule"],
        catchup=False,
        # Concurrent runs can double-insert the same merge key through BigQuery
        # MERGE snapshot isolation, so only one run may be active at a time. The
        # shared pool extends that protection across the backfill DAG.
        max_active_runs=1,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True,
        },
        tags=[
            "external_data",
            "dpanda",
            "bloomberg",
            "mongo",
            "gcs",
            "bigquery",
            category,
        ],
    ) as dag:
        @task(pool=POOL_NAME)
        def ingest_scheduled() -> dict:
            """Extract every grain (D-1) and transform them in one BigQuery job.

            One task means the KubernetesExecutor schedules one pod per run; one
            BigQuery job means one set of merges for the whole category. Retries
            replay the whole idempotent run.
            """
            context = get_current_context()
            return ingest(
                category=category,
                window=_resolve_interval(context),
                raw_format="ndjson",
                context=context,
            )

        ingest_scheduled()

    return dag


for _category, _settings in CATEGORIES.items():
    _dag = _build_dag(_category, _settings)
    globals()[_dag.dag_id] = _dag
