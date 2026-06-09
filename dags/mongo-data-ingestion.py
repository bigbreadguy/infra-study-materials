from pendulum import datetime

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.sdk import Variable
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pyrefly: ignore [missing-import]
from airflow.providers.mongo.hooks.mongo import MongoHook
# pyrefly: ignore [missing-import]
from bson import json_util


def _require_variable(name, value):
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _load_ingestion_config():
    return {
        "mongo_conn_id": Variable.get(
            "mongo_conn_id", default_var="mongo-default-connection"
        ),
        "mongo_database_name": Variable.get(
            "mongo_database_name", default_var="bloomberg"
        ),
        "mongo_collection_name": Variable.get(
            "mongo_collection_name", default_var="raw_data"
        ),
        "mongo_grain_id": Variable.get("mongo_grain_id", default_var=""),
        "gcp_conn_id": Variable.get("gcp_conn_id", default_var="google_cloud_default"),
        "gcs_bucket_name": Variable.get("gcs_bucket_name", default_var=""),
        "gcs_raw_prefix": Variable.get("gcs_raw_prefix", default_var="bloomberg/raw"),
    }


def _build_gcs_object_name(logical_date, grain_id, raw_prefix):
    logical_date_path = logical_date.strftime("%Y/%m/%d/%H")
    logical_date_token = logical_date.strftime("%Y%m%dT%H%M%S%z")
    path_parts = [
        raw_prefix.strip("/"),
        f"grain_id={grain_id}",
        logical_date_path,
        f"raw-{logical_date_token}.ndjson",
    ]
    return "/".join(part for part in path_parts if part)


def _extract_raw_data_to_gcs(logical_date):
    config = _load_ingestion_config()
    # bucket_name = _require_variable("gcs_bucket_name", config["gcs_bucket_name"])
    grain_id = _require_variable("mongo_grain_id", config["mongo_grain_id"])
    # object_name = _build_gcs_object_name(
    #     logical_date, grain_id, config["gcs_raw_prefix"]
    # )
    lines = []

    with MongoHook(mongo_conn_id=config["mongo_conn_id"]) as hook:
        # Fetch documents by grainId, ts
        collection = hook.get_conn().get_database(
            config["mongo_database_name"]
        ).get_collection(config["mongo_collection_name"])
        for doc in collection.find({"grainId": grain_id, "ts": logical_date}):
            lines.append(json_util.dumps(doc))

    print("=== Raw Documents from Mongo ===")
    print(lines)
    print("================================")

    payload = "\n".join(lines)
    if payload:
        payload = f"{payload}\n"

    # GCSHook(gcp_conn_id=config["gcp_conn_id"]).upload(
    #     bucket_name=bucket_name,
    #     object_name=object_name,
    #     data=payload,
    #     mime_type="application/x-ndjson",
    # )
    # print(f"Uploaded {len(lines)} documents to gs://{bucket_name}/{object_name}")
    return {
        # "bucket": bucket_name,
        # "object": object_name,
        "document_count": len(lines),
    }


with DAG(
    dag_id="mongo-data-ingestion",
    start_date=datetime(2023, 7, 1),
    schedule="@hourly",
) as dag:
    @task()
    def extract_raw_data_to_gcs():
        context = get_current_context()
        return _extract_raw_data_to_gcs(context["logical_date"])

    extract_raw_data_to_gcs
