"""Grain catalog metadata ingestion: MongoDB catalog -> GCS NDJSON -> BigQuery.

One manual-trigger DAG snapshots the catalog info object for every grain target
across every category's materials config, then merges the
dim_grain_metadata table in the dl_materials dataset. The DAG owns the table
(Terraform owns only the dataset): the merge opens with
``CREATE TABLE IF NOT EXISTS``. dim_grain_metadata is a standalone grain-level
reference keyed on the (id, name) = (dataset_id, grain_id) pair; the dl_materials
star schema models grains as metrics, so this table joins nothing -- it preserves
per-grain catalog metadata for analysts.

Catalog documents key grain identity at process.grainId / process.datasetId,
not at the top level, and several documents can share one grainId; the
selection rules live in external_data.common.grain_catalog. Each emitted row
carries the grain target's dataset_id (not the catalog's process.datasetId,
which can diverge) so the (dataset_id, grain_id) key stays stable.

Grain targets derive from the per-category materials configs:
  dags/external_data/configs/materials_metrics/dpanda_bloomberg.<category>.json

Required Airflow Variables (all namespaced dpanda_):
- dpanda_mongo_conn_id, dpanda_mongo_database_name,
  dpanda_mongo_catalog_collection_name
- dpanda_gcs_bucket_name, dpanda_gcs_raw_prefix
- dpanda_bigquery_project_id, dpanda_bigquery_dataset_id,
  dpanda_bigquery_region

All GCP access runs as the Airflow workload identity service account through
Application Default Credentials, so no Airflow GCP connection is required;
per dev policy tasks use no impersonation chains.
"""

from datetime import timedelta, timezone

# pyrefly: ignore [missing-import]
from pendulum import datetime
# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.sdk import Variable
# pyrefly: ignore [missing-import]
from airflow.exceptions import AirflowSkipException
# pyrefly: ignore [missing-import]
from airflow.providers.mongo.hooks.mongo import MongoHook
# pyrefly: ignore [missing-import]
from google.cloud import bigquery, storage
# pyrefly: ignore [missing-import]
from bson import json_util

from external_data.common.bigquery_grain_metadata import (
    GrainMetadataTransformConfig,
    run_create_raw_grain_catalog,
    run_merge_dim_grain_metadata,
)
from external_data.common.gcs_object import upload_replacing_object
from external_data.common.grain_catalog import (
    collect_unique_grain_targets,
    select_catalog_document,
)


def _require_variable(name, value):
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _required_airflow_variable(name):
    return _require_variable(name, Variable.get(name, default=""))


def _load_ingestion_config():
    return {
        "mongo_conn_id": _required_airflow_variable("dpanda_mongo_conn_id"),
        "mongo_database_name": _required_airflow_variable(
            "dpanda_mongo_database_name"
        ),
        "mongo_catalog_collection_name": _required_airflow_variable(
            "dpanda_mongo_catalog_collection_name"
        ),
        "gcs_bucket_name": _required_airflow_variable("dpanda_gcs_bucket_name"),
        "gcs_raw_prefix": _required_airflow_variable("dpanda_gcs_raw_prefix"),
        "bigquery_project_id": _required_airflow_variable(
            "dpanda_bigquery_project_id"
        ),
        "bigquery_dataset_id": _required_airflow_variable(
            "dpanda_bigquery_dataset_id"
        ),
        "bigquery_region": _required_airflow_variable("dpanda_bigquery_region"),
    }


def _build_gcs_object_name(logical_date, raw_prefix):
    logical_date = logical_date.astimezone(timezone.utc)
    logical_date_path = logical_date.strftime("%Y/%m/%d/%H")
    logical_date_token = logical_date.strftime("%Y%m%dT%H%M%S%z")
    path_parts = [
        raw_prefix.strip("/"),
        "catalog",
        logical_date_path,
        f"raw-{logical_date_token}.ndjson",
    ]
    return "/".join(part for part in path_parts if part)


def _build_catalog_line(target, document):
    # The grain target's dataset_id keys the row so the dim_grains join
    # always holds; the catalog's own process.datasetId can diverge from it.
    return json_util.dumps(
        {
            "datasetId": target["dataset_id"],
            "grainId": target["grain_id"],
            "catalogId": str(document.get("_id")),
            "catalogName": document.get("name"),
            "info": document.get("info"),
        }
    )


def _extract_catalog_to_gcs(targets, logical_date):
    config = _load_ingestion_config()
    lines = []
    missing_grain_ids = []

    with MongoHook(mongo_conn_id=config["mongo_conn_id"]) as hook:
        collection = hook.get_conn().get_database(
            config["mongo_database_name"]
        ).get_collection(config["mongo_catalog_collection_name"])

        for target in targets:
            # Grain identity sits nested under process; the catalog
            # datasetId can diverge from the target dataset_id, so the
            # find must key on the grain id alone and leave datasetId
            # preference to the selection rules.
            documents = list(
                collection.find({"process.grainId": target["grain_id"]})
            )
            document = select_catalog_document(
                documents,
                dataset_id=target["dataset_id"],
                grain_id=target["grain_id"],
            )
            if document is None:
                missing_grain_ids.append(target["grain_id"])
                continue
            lines.append(_build_catalog_line(target, document))

    # Catalog entries can lag grain onboarding, so absent documents stay a
    # logged warning in the task result instead of failing the snapshot.
    if missing_grain_ids:
        print(
            f"No catalog document for {len(missing_grain_ids)} grain "
            f"targets: {missing_grain_ids}"
        )
    if not lines:
        raise AirflowSkipException(
            "No catalog documents matched any grain target"
        )

    payload = "\n".join(lines) + "\n"
    object_name = _build_gcs_object_name(
        logical_date, config["gcs_raw_prefix"]
    )

    gcs_client = storage.Client()
    uploaded_object_name = upload_replacing_object(
        gcs_client,
        bucket_name=config["gcs_bucket_name"],
        object_name=object_name,
        data=payload,
        mime_type="application/x-ndjson",
    )
    print(
        f"Uploaded {len(lines)} catalog documents to "
        f"gs://{config['gcs_bucket_name']}/{uploaded_object_name}"
    )
    return {
        "bucket": config["gcs_bucket_name"],
        "object": uploaded_object_name,
        "document_count": len(lines),
        "missing_grain_ids": missing_grain_ids,
    }


def _transform_config(config, raw_location, expected_raw_row_count=None):
    # Scope the external table to the exact object this run uploaded so each
    # run only reprocesses its own snapshot.
    return GrainMetadataTransformConfig(
        project_id=config["bigquery_project_id"],
        dataset_id=config["bigquery_dataset_id"],
        region=config["bigquery_region"],
        raw_gcs_uri=f"gs://{raw_location['bucket']}/{raw_location['object']}",
        expected_raw_row_count=expected_raw_row_count,
    )


def _run_bigquery_step(upstream_result, step_name, runner):
    raw_location = upstream_result.get("raw_location", upstream_result)
    config = _load_ingestion_config()
    expected_raw_row_count = None
    if step_name == "raw_grain_catalog":
        expected_raw_row_count = raw_location.get("document_count")

    transform_config = _transform_config(
        config, raw_location, expected_raw_row_count
    )
    client = bigquery.Client(
        project=config["bigquery_project_id"],
        location=config["bigquery_region"],
    )
    result = runner(client, transform_config)
    print(
        f"Completed BigQuery transform step {step_name} with job "
        f"{result['job_id']} for raw object {transform_config.raw_gcs_uri}"
    )
    return {
        "step": step_name,
        "job_id": result["job_id"],
        "location": result["location"],
        "raw_location": raw_location,
    }


with DAG(
    dag_id="external_data__dpanda_grain_metadata",
    description=(
        "Grain catalog metadata snapshot: MongoDB catalog -> GCS NDJSON -> "
        "BigQuery dim_grain_metadata merge for every grain target category."
    ),
    start_date=datetime(2026, 1, 1, tz="UTC"),
    # Manual trigger only: catalog metadata is snapshotted on demand.
    schedule=None,
    catchup=False,
    # Concurrent runs can double-insert the same merge key through BigQuery
    # MERGE snapshot isolation, so only one run may be active at a time.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=[
        "external_data",
        "dpanda",
        "catalog",
        "mongo",
        "gcs",
        "bigquery",
    ],
) as dag:
    @task()
    def get_grain_targets() -> list[dict]:
        return collect_unique_grain_targets()

    @task()
    def extract_catalog_to_gcs(targets: list[dict]):
        context = get_current_context()
        run_point = context["logical_date"] or context["data_interval_start"]
        if run_point is None:
            raise ValueError(
                "Run provides neither a logical date nor a data interval "
                "start to name the snapshot object"
            )
        return _extract_catalog_to_gcs(targets, run_point)

    @task(task_id="raw_grain_catalog")
    def create_raw_grain_catalog(raw_location):
        return _run_bigquery_step(
            raw_location,
            "raw_grain_catalog",
            run_create_raw_grain_catalog,
        )

    @task(task_id="dim_grain_metadata")
    def merge_dim_grain_metadata(upstream_result):
        return _run_bigquery_step(
            upstream_result,
            "dim_grain_metadata",
            run_merge_dim_grain_metadata,
        )

    grain_targets = get_grain_targets()
    raw_location = extract_catalog_to_gcs(grain_targets)
    raw_table = create_raw_grain_catalog(raw_location)
    merge_dim_grain_metadata(raw_table)
