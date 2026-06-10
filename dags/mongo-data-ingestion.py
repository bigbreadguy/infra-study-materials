import re
from datetime import timedelta, timezone

from pendulum import datetime

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task, task_group
# pyrefly: ignore [missing-import]
from airflow.sdk import Variable
# pyrefly: ignore [missing-import]
from airflow.sdk.exceptions import AirflowSkipException
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
    run_create_raw_data_samples,
    run_merge_dim_grains,
    run_merge_dim_metrics,
    run_merge_fact_values,
)
from common.bigquery_market_index_sql import RAW_DATA_SAMPLES_TABLE
from common.gcs_object import upload_replacing_object
from common.grain_targets import GRAIN_TARGETS_VARIABLE, parse_grain_targets


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


def _extract_raw_data_to_gcs(target, data_interval_start, data_interval_end):
    grain_id = target["grain_id"]
    dataset_id = ObjectId(target["dataset_id"])
    config = _load_ingestion_config()
    bucket_name = _require_variable("gcs_bucket_name", config["gcs_bucket_name"])
    object_name = _build_gcs_object_name(
        data_interval_start, grain_id, config["gcs_raw_prefix"]
    )
    print(f"Extracting data for interval: {data_interval_start} to {data_interval_end}")
    lines = []

    with MongoHook(mongo_conn_id=config["mongo_conn_id"]) as hook:
        collection = hook.get_conn().get_database(
            config["mongo_database_name"]
        ).get_collection(config["mongo_collection_name"])

        # An indexed point probe separates a wrong dataset id to grain id
        # mapping, which must fail loudly, from a day with no data, which is
        # a legitimate skip.
        if collection.find_one(
            {"datasetId": dataset_id, "grainId": grain_id}, {"_id": 1}
        ) is None:
            raise ValueError(
                f"No documents match datasetId {target['dataset_id']} with "
                f"grainId {grain_id}; check the "
                f"{GRAIN_TARGETS_VARIABLE} variable"
            )

        # Lead with datasetId so the find stays on the compound index over
        # datasetId, grainId, and ts.
        for doc in collection.find({
            "datasetId": dataset_id,
            "grainId": grain_id,
            "ts": {
                "$gte": data_interval_start,
                "$lt": data_interval_end,
            }
        }):
            lines.append(json_util.dumps(doc))

    print(f"Extracted {len(lines)} raw documents from Mongo")

    # A grain with no samples in the interval is a legitimate quiet day.
    # Trigger rules only resolve upstream skips per map index inside a
    # common mapped task group, so this skip stays scoped to one grain only
    # because the whole chain expands as one task group. Skipped runs leave
    # any previously uploaded object for the interval in place.
    if not lines:
        raise AirflowSkipException(
            f"No samples for grainId {grain_id} between "
            f"{data_interval_start} and {data_interval_end}"
        )

    payload = "\n".join(lines) + "\n"

    gcs_hook = GCSHook(
        gcp_conn_id=config["gcp_conn_id"],
        impersonation_chain=config["gcs_impersonation_chain"],
    )
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
    return {
        "bucket": bucket_name,
        "object": uploaded_object_name,
        "document_count": len(lines),
        "grain_id": grain_id,
        "grain_description": target["description"],
    }


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
        raw_table_id=config.get("raw_table_id", RAW_DATA_SAMPLES_TABLE),
        expected_raw_row_count=config.get("expected_raw_row_count"),
        grain_description=config.get("grain_description"),
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


def _grain_raw_table_id(grain_id):
    normalized_grain_id = re.sub(r"[^a-zA-Z0-9]", "_", grain_id)
    return f"{RAW_DATA_SAMPLES_TABLE}_{normalized_grain_id}"


def _run_bigquery_step(upstream_result, step_name, runner):
    raw_location = _raw_location_from_upstream(upstream_result)
    grain_id = raw_location.get("grain_id")
    config = _load_ingestion_config()

    if grain_id:
        config["raw_table_id"] = _grain_raw_table_id(grain_id)

    config["grain_description"] = raw_location.get("grain_description")

    # Scope the external table to the exact object this run uploaded so each
    # run only reprocesses its own interval; clearing a past run backfills it.
    config["raw_gcs_uri"] = f"gs://{raw_location['bucket']}/{raw_location['object']}"
    if step_name == "raw_data_samples":
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


with DAG(
    dag_id="mongo-data-ingestion",
    # Logical dates are managed in zulu time because the source database
    # keys day grained rows by utc timestamps.
    start_date=datetime(1996, 4, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    # Concurrent runs can double-insert the same merge key through BigQuery
    # MERGE snapshot isolation, so only one run may be active at a time.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
) as dag:
    @task()
    def get_grain_targets() -> list[dict]:
        raw = Variable.get(GRAIN_TARGETS_VARIABLE, default="")
        if not raw:
            raise ValueError(
                f"Airflow Variable '{GRAIN_TARGETS_VARIABLE}' must be set"
            )
        return parse_grain_targets(raw)

    @task()
    def extract_raw_data_to_gcs(target: dict):
        context = get_current_context()
        # Stick to the date only: resolve the run to its zulu calendar date
        # and extract that full utc day. The logical date is authoritative
        # because cron trigger timetables derive the data interval from the
        # trigger wall clock, not from an explicitly supplied logical date.
        # Trigger logical dates must be given as utc midnights or the run
        # resolves to the prior zulu date.
        run_point = context["logical_date"] or context["data_interval_start"]
        if run_point is None:
            raise ValueError(
                "Run provides neither a logical date nor a data interval "
                "start to resolve the extraction date"
            )
        start_date = run_point.in_timezone("UTC").start_of("day")
        end_date = start_date.add(days=1)
        if run_point != start_date:
            print(
                f"Logical date {run_point} is not a zulu midnight; "
                f"resolved to zulu date {start_date.date()}"
            )
        return _extract_raw_data_to_gcs(target, start_date, end_date)

    @task(task_id="raw_data_samples")
    def create_raw_data_samples(raw_location):
        return _run_bigquery_step(
            raw_location,
            "raw_data_samples",
            run_create_raw_data_samples,
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

    # The per grain chain must expand as one mapped task group: trigger
    # rules only narrow a skipped upstream to the matching map index when
    # both tasks share a mapped task group, so without it one empty grain
    # would skip the transform and load tasks for every grain.
    @task_group()
    def ingest_grain(target: dict):
        raw_location = extract_raw_data_to_gcs(target)
        raw_table = create_raw_data_samples(raw_location)
        grains = merge_dim_grains(raw_table)
        metrics = merge_dim_metrics(grains)
        merge_fact_values(metrics)

    grain_targets = get_grain_targets()
    ingest_grain.expand(target=grain_targets)
