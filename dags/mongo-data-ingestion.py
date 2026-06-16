"""Mongo market index ingestion: MongoDB -> GCS NDJSON -> BigQuery.

Each configured category owns one DAG. One batch extract task pulls one
full UTC day of samples for every enabled grain target in the category
over a single MongoDB connection and uploads one NDJSON object per grain
to GCS; the daily volume per grain is a handful of documents, so per task
fixed costs (worker start, variable fetches, Mongo connection) dominate
and the batch pays them once per category instead of once per grain. The
extract targets the previous completed UTC day (D-1), not the logical
date's own day, because the @daily schedule fires at 00:00Z before the
source has published the in-progress day. Mapped per grain chains then
read each object through a per-query temporary external table definition
(no persistent raw table) and merge dim_grains, dim_metrics, and
fact_values.

Grain target config files are local-only (gitignored) and live in:
  dags/local/grain_targets/<category>.json

Categories are discovered from the json file stems at parse time, so adding
or removing a config file adds or removes the matching DAG on the next
dag-processor refresh. Only the glob runs at parse time; the json itself is
parsed inside the extract task, so a malformed file fails that category's
run, never DAG import.
"""

from datetime import timedelta, timezone

from pendulum import datetime

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task, task_group
# pyrefly: ignore [missing-import]
from airflow.sdk import Variable
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pyrefly: ignore [missing-import]
from airflow.providers.mongo.hooks.mongo import MongoHook
# pyrefly: ignore [missing-import]
from bson import ObjectId, json_util

from common.bigquery_market_index import (
    BigQueryTransformConfig,
    run_merge_dim_grains,
    run_merge_dim_metrics,
    run_merge_fact_values,
    run_validate_raw_data,
)
from common.gcs_object import upload_replacing_object
from common.grain_targets import (
    list_grain_target_categories,
    load_grain_targets,
)


# Categories default to manual trigger; map a category name to a schedule
# here once it is verified, e.g. {"nickel": "@daily"}.
SCHEDULE_OVERRIDES: dict[str, str | None] = {
    "copper": "@daily",
    "market_macro": "@daily",
    "nickel": "@daily"
}

# The @daily schedule fires at 00:00Z, the start of the logical date's
# own day, before the source has published it. Extract the previous
# completed zulu day instead.
EXTRACTION_LOOKBACK_DAYS = 1


def _require_variable(name, value):
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _required_airflow_variable(name):
    return _require_variable(name, Variable.get(name, default=""))


def _derive_raw_gcs_uri(bucket_name, raw_prefix):
    normalized_prefix = raw_prefix.strip("/")
    if normalized_prefix:
        return f"gs://{bucket_name}/{normalized_prefix}/*"

    return f"gs://{bucket_name}/*"


def _load_ingestion_config():
    gcs_bucket_name = _required_airflow_variable("gcs_bucket_name")
    gcs_raw_prefix = _required_airflow_variable("gcs_raw_prefix")

    return {
        "mongo_conn_id": _required_airflow_variable("mongo_conn_id"),
        "mongo_database_name": _required_airflow_variable("mongo_database_name"),
        "mongo_collection_name": _required_airflow_variable("mongo_collection_name"),
        "gcp_conn_id": _required_airflow_variable("gcp_conn_id"),
        "gcs_bucket_name": gcs_bucket_name,
        "gcs_raw_prefix": gcs_raw_prefix,
        "gcs_impersonation_chain": _required_airflow_variable(
            "gcs_impersonation_chain"
        ),
        "bigquery_project_id": _required_airflow_variable("bigquery_project_id"),
        "bigquery_dataset_id": _required_airflow_variable("bigquery_dataset_id"),
        "bigquery_region": _required_airflow_variable("bigquery_region"),
        "raw_gcs_uri": _derive_raw_gcs_uri(gcs_bucket_name, gcs_raw_prefix),
        "bigquery_impersonation_chain": _required_airflow_variable(
            "bigquery_impersonation_chain"
        ),
    }


def _build_gcs_object_name(logical_date, grain_id, raw_prefix):
    logical_date = logical_date.astimezone(timezone.utc)
    logical_date_path = logical_date.strftime("%Y/%m/%d/%H")
    logical_date_token = logical_date.strftime("%Y%m%dT%H%M%S%z")
    path_parts = [
        raw_prefix.strip("/"),
        f"grain_id={grain_id}",
        logical_date_path,
        f"raw-{logical_date_token}.ndjson",
    ]
    return "/".join(part for part in path_parts if part)


def _extract_grain_lines(
    collection,
    target,
    data_interval_start,
    data_interval_end,
    category,
):
    grain_id = target["grain_id"]
    dataset_id = ObjectId(target["dataset_id"])

    # An indexed point probe separates a wrong dataset id to grain id
    # mapping, which must fail loudly, from a day with no data, which is
    # a legitimate quiet day.
    if collection.find_one(
        {"datasetId": dataset_id, "grainId": grain_id}, {"_id": 1}
    ) is None:
        raise ValueError(
            f"No documents match datasetId {target['dataset_id']} with "
            f"grainId {grain_id}; check grain targets file {category}.json"
        )

    # Lead with datasetId so the find stays on the compound index over
    # datasetId, grainId, and ts.
    return [
        json_util.dumps(doc)
        for doc in collection.find({
            "datasetId": dataset_id,
            "grainId": grain_id,
            "ts": {
                "$gte": data_interval_start,
                "$lt": data_interval_end,
            }
        })
    ]


def _extract_raw_data_to_gcs(
    targets,
    data_interval_start,
    data_interval_end,
    category,
):
    """Extract the interval for every grain target over one Mongo session.

    Returns one raw location per grain with samples. A grain with no samples
    in the interval is a legitimate quiet day: it yields no raw location, so
    the downstream mapped transform chains expand only over grains with
    data. Quiet runs leave any previously uploaded object for the interval
    in place. One bad grain (wrong mapping, failed upload) fails the whole
    category extract; retries rerun every grain, which stays idempotent
    because uploads replace the per grain interval object.
    """
    config = _load_ingestion_config()
    bucket_name = _require_variable("gcs_bucket_name", config["gcs_bucket_name"])
    print(f"Extracting data for interval: {data_interval_start} to {data_interval_end}")

    gcs_hook = GCSHook(
        gcp_conn_id=config["gcp_conn_id"],
        impersonation_chain=config["gcs_impersonation_chain"],
    )
    raw_locations = []
    quiet_grains = []

    with MongoHook(mongo_conn_id=config["mongo_conn_id"]) as hook:
        collection = hook.get_conn().get_database(
            config["mongo_database_name"]
        ).get_collection(config["mongo_collection_name"])

        for target in targets:
            grain_id = target["grain_id"]
            lines = _extract_grain_lines(
                collection,
                target,
                data_interval_start,
                data_interval_end,
                category,
            )

            if not lines:
                quiet_grains.append(grain_id)
                continue

            object_name = _build_gcs_object_name(
                data_interval_start, grain_id, config["gcs_raw_prefix"]
            )
            payload = "\n".join(lines) + "\n"
            uploaded_object_name = upload_replacing_object(
                gcs_hook,
                bucket_name=bucket_name,
                object_name=object_name,
                data=payload,
                mime_type="application/x-ndjson",
            )
            print(
                f"Uploaded {len(lines)} documents to "
                f"gs://{bucket_name}/{uploaded_object_name}"
            )
            raw_locations.append({
                "bucket": bucket_name,
                "object": uploaded_object_name,
                "document_count": len(lines),
                "grain_id": grain_id,
                "grain_description": target["description"],
                "time_grain": target["freq"],
            })

    if quiet_grains:
        print(
            f"No samples between {data_interval_start} and "
            f"{data_interval_end} for {len(quiet_grains)} quiet grains: "
            f"{', '.join(quiet_grains)}"
        )
    print(
        f"Extracted {len(raw_locations)} grain objects "
        f"out of {len(targets)} targets"
    )
    return raw_locations


def _bigquery_transform_config(config):
    return BigQueryTransformConfig(
        project_id=_require_variable(
            "bigquery_project_id",
            config["bigquery_project_id"],
        ),
        dataset_id=_require_variable(
            "bigquery_dataset_id",
            config["bigquery_dataset_id"],
        ),
        region=_require_variable("bigquery_region", config["bigquery_region"]),
        raw_gcs_uri=_require_variable("raw_gcs_uri", config["raw_gcs_uri"]),
        expected_raw_row_count=config.get("expected_raw_row_count"),
        grain_description=config.get("grain_description"),
        time_grain=config.get("time_grain"),
    )


def _bigquery_client(config):
    hook = BigQueryHook(
        gcp_conn_id=config["gcp_conn_id"],
        impersonation_chain=config["bigquery_impersonation_chain"],
        location=config["bigquery_region"],
        use_legacy_sql=False,
    )
    return hook.get_client(
        project_id=config["bigquery_project_id"],
        location=config["bigquery_region"],
    )


def _raw_location_from_upstream(upstream_result):
    if isinstance(upstream_result, dict) and "raw_location" in upstream_result:
        return upstream_result["raw_location"]

    return upstream_result


def _run_bigquery_step(upstream_result, step_name, runner):
    raw_location = _raw_location_from_upstream(upstream_result)
    config = _load_ingestion_config()

    config["grain_description"] = raw_location.get("grain_description")
    config["time_grain"] = raw_location.get("time_grain")

    # Scope the job's temporary external table definition to the exact object
    # this run uploaded so each run only reprocesses its own interval;
    # clearing a past run backfills it. The definition lives inside the job,
    # so concurrent grain chains never share raw table state.
    config["raw_gcs_uri"] = f"gs://{raw_location['bucket']}/{raw_location['object']}"
    if step_name == "validate_raw_data":
        config["expected_raw_row_count"] = raw_location.get("document_count")

    transform_config = _bigquery_transform_config(config)
    client = _bigquery_client(config)
    result = runner(client, transform_config)
    print(
        f"Completed BigQuery transform step {step_name} with job "
        f"{result['job_id']} for raw object "
        f"gs://{raw_location['bucket']}/{raw_location['object']} "
        f"using {transform_config.raw_gcs_uri}"
    )
    return {
        "step": step_name,
        "job_id": result["job_id"],
        "location": result["location"],
        "raw_location": raw_location,
    }


def _build_dag(category, schedule):
    with DAG(
        dag_id=f"mongo-data-ingestion-{category}",
        description=(
            "Mongo market index samples: MongoDB -> GCS NDJSON -> BigQuery "
            f"dim and fact merges for {category} grain targets."
        ),
        # Logical dates are managed in zulu time because the source database
        # keys day grained rows by utc timestamps.
        start_date=datetime(1996, 4, 1, tz="UTC"),
        schedule=schedule,
        catchup=False,
        # Concurrent runs can double-insert the same merge key through BigQuery
        # MERGE snapshot isolation, so only one run may be active at a time.
        # Across category DAGs the same safety comes from disjoint grain_ids,
        # enforced by tests/test_grain_target_files.py.
        max_active_runs=1,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True,
        },
        tags=["mongo", "gcs", "bigquery", category],
    ) as dag:
        @task()
        def extract_raw_data_to_gcs() -> list[dict]:
            context = get_current_context()
            # The logical date is authoritative because cron trigger
            # timetables derive the data interval from the trigger wall clock,
            # not from an explicitly supplied logical date. Trigger logical
            # dates must be given as utc midnights or the run resolves to the
            # prior zulu date.
            run_point = context["logical_date"] or context["data_interval_start"]
            if run_point is None:
                raise ValueError(
                    "Run provides neither a logical date nor a data interval "
                    "start to resolve the extraction date"
                )
            # Resolve the run to its zulu calendar day, then step back one day.
            # The schedule fires at 00:00Z (09:00 KST) at the very start of the
            # logical date's day, when the source has not yet published that
            # day's samples. Targeting the previous, completed zulu day (D-1)
            # gives the source a full day to populate before extraction.
            run_day = run_point.in_timezone("UTC").start_of("day")
            if run_point != run_day:
                print(
                    f"Logical date {run_point} is not a zulu midnight; "
                    f"resolved to zulu date {run_day.date()}"
                )
            start_date = run_day.subtract(days=EXTRACTION_LOOKBACK_DAYS)
            end_date = start_date.add(days=1)
            return _extract_raw_data_to_gcs(
                load_grain_targets(category),
                start_date,
                end_date,
                category,
            )

        @task(task_id="validate_raw_data")
        def validate_raw_data(raw_location):
            return _run_bigquery_step(
                raw_location,
                "validate_raw_data",
                run_validate_raw_data,
            )

        @task(task_id="dim_grains")
        def merge_dim_grains(upstream_result):
            return _run_bigquery_step(
                upstream_result,
                "dim_grains",
                run_merge_dim_grains,
            )

        @task(task_id="dim_metrics")
        def merge_dim_metrics(upstream_result):
            return _run_bigquery_step(
                upstream_result,
                "dim_metrics",
                run_merge_dim_metrics,
            )

        @task(task_id="fact_values")
        def merge_fact_values(upstream_result):
            return _run_bigquery_step(
                upstream_result,
                "fact_values",
                run_merge_fact_values,
            )

        # Each grain's transform chain expands as one mapped task group so
        # the per grain raw location rides one chain end to end. Quiet
        # grains never reach here: the batch extract returns no raw
        # location for them, so their chains simply do not expand.
        @task_group()
        def transform_grain(raw_location: dict):
            raw_checked = validate_raw_data(raw_location)
            grains = merge_dim_grains(raw_checked)
            metrics = merge_dim_metrics(grains)
            merge_fact_values(metrics)

        raw_locations = extract_raw_data_to_gcs()
        transform_grain.expand(raw_location=raw_locations)

    return dag


for _category in list_grain_target_categories():
    _dag = _build_dag(_category, SCHEDULE_OVERRIDES.get(_category))
    globals()[_dag.dag_id] = _dag
