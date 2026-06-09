from datetime import timezone

from pendulum import datetime

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.sdk import Variable
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.dataform import DataformHook
# pyrefly: ignore [missing-import]
from airflow.providers.mongo.hooks.mongo import MongoHook
# pyrefly: ignore [missing-import]
from bson import json_util

from common.gcs_object import upload_replacing_object


def _require_variable(name, value):
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _required_airflow_variable(name):
    return _require_variable(name, Variable.get(name, default=""))


def _optional_int_variable(name, value):
    if not value:
        return None
    return int(value)


def _load_ingestion_config():
    return {
        "mongo_conn_id": _required_airflow_variable("mongo_conn_id"),
        "mongo_database_name": _required_airflow_variable("mongo_database_name"),
        "mongo_collection_name": _required_airflow_variable("mongo_collection_name"),
        "mongo_grain_id": _required_airflow_variable("mongo_grain_id"),
        "gcp_conn_id": _required_airflow_variable("gcp_conn_id"),
        "gcs_bucket_name": _required_airflow_variable("gcs_bucket_name"),
        "gcs_raw_prefix": _required_airflow_variable("gcs_raw_prefix"),
        "gcs_impersonation_chain": _required_airflow_variable(
            "gcs_impersonation_chain"
        ),
        "dataform_project_id": _required_airflow_variable("dataform_project_id"),
        "dataform_region": _required_airflow_variable("dataform_region"),
        "dataform_repository_id": _required_airflow_variable("dataform_repository_id"),
        "dataform_workflow_config": _required_airflow_variable(
            "dataform_workflow_config"
        ),
        "dataform_impersonation_chain": _required_airflow_variable(
            "dataform_impersonation_chain"
        ),
        "dataform_wait_time_seconds": int(
            Variable.get("dataform_wait_time_seconds", default="10")
        ),
        "dataform_timeout_seconds": _optional_int_variable(
            "dataform_timeout_seconds",
            Variable.get("dataform_timeout_seconds", default=""),
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


def _extract_raw_data_to_gcs(data_interval_start, data_interval_end):
    config = _load_ingestion_config()
    bucket_name = _require_variable("gcs_bucket_name", config["gcs_bucket_name"])
    grain_id = _require_variable("mongo_grain_id", config["mongo_grain_id"])
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
    }


def _trigger_dataform_workflow(raw_location):
    config = _load_ingestion_config()
    project_id = _require_variable("dataform_project_id", config["dataform_project_id"])
    region = _require_variable("dataform_region", config["dataform_region"])
    repository_id = _require_variable(
        "dataform_repository_id", config["dataform_repository_id"]
    )
    workflow_config = _require_variable(
        "dataform_workflow_config", config["dataform_workflow_config"]
    )
    hook = DataformHook(
        gcp_conn_id=config["gcp_conn_id"],
        impersonation_chain=config["dataform_impersonation_chain"],
    )
    workflow_invocation = hook.create_workflow_invocation(
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workflow_invocation={"workflow_config": workflow_config},
    )
    workflow_invocation_id = workflow_invocation.name.split("/")[-1]
    print(
        "Triggered Dataform workflow invocation "
        f"{workflow_invocation.name} for raw object "
        f"gs://{raw_location['bucket']}/{raw_location['object']}"
    )
    hook.wait_for_workflow_invocation(
        workflow_invocation_id=workflow_invocation_id,
        repository_id=repository_id,
        project_id=project_id,
        region=region,
        wait_time=config["dataform_wait_time_seconds"],
        timeout=config["dataform_timeout_seconds"],
    )
    print(f"Dataform workflow invocation {workflow_invocation.name} completed")
    return {
        "name": workflow_invocation.name,
        "workflow_invocation_id": workflow_invocation_id,
        "raw_location": raw_location,
    }


with DAG(
    dag_id="mongo-data-ingestion",
    start_date=datetime(1996, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    @task()
    def extract_raw_data_to_gcs():
        context = get_current_context()
        logical_date = context["logical_date"]
        start_date = logical_date.start_of("day")
        end_date = start_date.add(days=1)
        return _extract_raw_data_to_gcs(start_date, end_date)

    @task()
    def trigger_dataform_workflow(raw_location):
        return _trigger_dataform_workflow(raw_location)

    trigger_dataform_workflow(extract_raw_data_to_gcs())
