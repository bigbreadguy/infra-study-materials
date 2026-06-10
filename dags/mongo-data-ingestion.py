import re
from datetime import timedelta, timezone

from pendulum import datetime

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.sdk import Variable
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pyrefly: ignore [missing-import]
from airflow.providers.mongo.hooks.mongo import MongoHook
# pyrefly: ignore [missing-import]
from bson import json_util

from common.bigquery_market_index import (
    BigQueryTransformConfig,
    run_create_raw_data_samples,
    run_merge_dim_grains,
    run_merge_dim_metrics,
    run_merge_fact_values,
)
from common.bigquery_market_index_sql import RAW_DATA_SAMPLES_TABLE
from common.gcs_object import upload_replacing_object


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
        "mongo_grain_id": _required_airflow_variable("mongo_grain_id"),
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


def _extract_raw_data_to_gcs(grain_id, data_interval_start, data_interval_end):
    config = _load_ingestion_config()
    bucket_name = _require_variable("gcs_bucket_name", config["gcs_bucket_name"])
    object_name = _build_gcs_object_name(
        data_interval_start, grain_id, config["gcs_raw_prefix"]
    )
    print(f"Extracting data for interval: {data_interval_start} to {data_interval_end}")
    lines = []

    with MongoHook(mongo_conn_id=config["mongo_conn_id"]) as hook:
        # Fetch documents by grainId, ts
        collection = hook.get_conn().get_database(
            config["mongo_database_name"]
        ).get_collection(config["mongo_collection_name"])
        for doc in collection.find({
            "grainId": grain_id,
            "ts": {
                "$gte": data_interval_start,
                "$lt": data_interval_end,
            }
        }):
            lines.append(json_util.dumps(doc))

    print(f"Extracted {len(lines)} raw documents from Mongo")

    payload = "\n".join(lines)
    if payload:
        payload = f"{payload}\n"

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

    # Scope the external table to the exact object this run uploaded so each
    # run only reprocesses its own interval; clearing a past run backfills it.
    config["raw_gcs_uri"] = f"gs://{raw_location['bucket']}/{raw_location['object']}"

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
    start_date=datetime(1996, 4, 1),
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
    def get_grain_ids() -> list[str]:
        grain_ids_str = Variable.get("mongo_grain_id", default="")
        if not grain_ids_str:
            raise ValueError("Airflow Variable 'mongo_grain_id' must be set")
        return [g.strip() for g in grain_ids_str.split(",") if g.strip()]

    @task()
    def extract_raw_data_to_gcs(grain_id: str):
        context = get_current_context()
        return _extract_raw_data_to_gcs(
            grain_id,
            context["data_interval_start"],
            context["data_interval_end"],
        )

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

    grain_ids = get_grain_ids()
    raw_location = extract_raw_data_to_gcs.expand(grain_id=grain_ids)
    raw_table = create_raw_data_samples.expand(raw_location=raw_location)
    grains = merge_dim_grains.expand(upstream_result=raw_table)
    metrics = merge_dim_metrics.expand(upstream_result=grains)
    merge_fact_values.expand(upstream_result=metrics)
