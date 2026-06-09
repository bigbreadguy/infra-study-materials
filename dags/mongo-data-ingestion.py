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

from common.dpanda_index import to_ndjson, transform_raw_market_data
from common.gcs_object import upload_unique_object


def _require_variable(name, value):
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _load_ingestion_config():
    return {
        "mongo_conn_id": Variable.get(
            "mongo_conn_id", default="mongo-default-connection"
        ),
        "mongo_database_name": Variable.get(
            "mongo_database_name", default="dpanda"
        ),
        "mongo_collection_name": Variable.get(
            "mongo_collection_name", default="raw_data"
        ),
        "mongo_grain_id": Variable.get("mongo_grain_id", default=""),
        "gcp_conn_id": Variable.get("gcp_conn_id", default="google_cloud_default"),
        "gcs_bucket_name": Variable.get("gcs_bucket_name", default=""),
        "gcs_raw_prefix": Variable.get("gcs_raw_prefix", default="dpanda/raw"),
        "gcs_curated_prefix": Variable.get(
            "gcs_curated_prefix", default="dpanda/curated"
        ),
        "metric_value_sample_id_field": Variable.get(
            "metric_value_sample_id_field", default="sample_id"
        ),
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


def _build_curated_gcs_object_names(logical_date, grain_id, curated_prefix):
    logical_date_path = logical_date.strftime("%Y/%m/%d/%H")
    logical_date_token = logical_date.strftime("%Y%m%dT%H%M%S%z")
    object_names = {}

    for record_type in ("grains", "metrics", "metric_values"):
        file_stem = record_type.replace("_", "-")
        path_parts = [
            curated_prefix.strip("/"),
            record_type,
            f"grain_id={grain_id}",
            logical_date_path,
            f"{file_stem}-{logical_date_token}.ndjson",
        ]
        object_names[record_type] = "/".join(part for part in path_parts if part)

    return object_names


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

    print("=== Raw Documents from Mongo ===")
    print(lines)
    print("================================")

    payload = "\n".join(lines)
    if payload:
        payload = f"{payload}\n"

    gcs_hook = GCSHook(gcp_conn_id=config["gcp_conn_id"])
    uploaded_object_name = upload_unique_object(
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


def _transform_raw_data_to_curated_gcs(raw_location, logical_date):
    config = _load_ingestion_config()
    bucket_name = _require_variable("gcs_bucket_name", config["gcs_bucket_name"])
    grain_id = _require_variable("mongo_grain_id", config["mongo_grain_id"])
    raw_bucket_name = raw_location["bucket"]
    raw_object_name = raw_location["object"]
    gcs_hook = GCSHook(gcp_conn_id=config["gcp_conn_id"])
    raw_payload = gcs_hook.download(
        bucket_name=raw_bucket_name,
        object_name=raw_object_name,
    )

    if isinstance(raw_payload, bytes):
        raw_payload = raw_payload.decode("utf-8")

    transformed_records = transform_raw_market_data(
        raw_payload,
        sample_id_field=config["metric_value_sample_id_field"],
    )
    curated_object_names = _build_curated_gcs_object_names(
        logical_date,
        grain_id,
        config["gcs_curated_prefix"],
    )
    uploaded_locations = {}

    for record_type, records in transformed_records.items():
        object_name = curated_object_names[record_type]
        uploaded_object_name = upload_unique_object(
            gcs_hook,
            bucket_name=bucket_name,
            object_name=object_name,
            data=to_ndjson(records),
            mime_type="application/x-ndjson",
        )
        uploaded_locations[record_type] = {
            "bucket": bucket_name,
            "object": uploaded_object_name,
            "record_count": len(records),
        }
        print(
            f"Uploaded {len(records)} {record_type} records to "
            f"gs://{bucket_name}/{uploaded_object_name}"
        )

    return uploaded_locations


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
    def transform_raw_data_to_curated_gcs(raw_location):
        context = get_current_context()
        logical_date = context["logical_date"].start_of("day")
        return _transform_raw_data_to_curated_gcs(raw_location, logical_date)

    transform_raw_data_to_curated_gcs(extract_raw_data_to_gcs())
