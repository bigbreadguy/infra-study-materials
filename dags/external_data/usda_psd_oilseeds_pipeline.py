"""USDA FAS PS&D oilseeds: ZIP -> GCS raw -> filter -> dl_external.psd_oilseeds.

dag_run.conf (optional):
- gcp_project: default dev-dfml-platform
- location: default asia-northeast3
- gcs_bucket: default dfml-dev-raw
- dl_dataset: default dl_external
- ingest_date: partition key (default logical date YYYY-MM-DD)
- gcs_prefix: override full GCS prefix (without filename)
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from external_data.common.usda_psd import fetch_and_upload_raw, transform_and_load_bq


def _fetch_raw(**context) -> str:
    conf = context["dag_run"].conf or {}
    ingest_date = str(
        conf.get("ingest_date") or context["logical_date"].strftime("%Y-%m-%d")
    )
    gcs_prefix = (conf.get("gcs_prefix") or "").strip() or None
    return fetch_and_upload_raw(
        gcs_bucket=str(conf.get("gcs_bucket", "dfml-dev-raw")),
        ingest_date=ingest_date,
        gcs_prefix=gcs_prefix,
    )


def _transform_load(**context) -> int:
    source_zip_uri = context["ti"].xcom_pull(task_ids="fetch_raw_to_gcs")
    if not source_zip_uri:
        raise ValueError("fetch_raw_to_gcs did not return a GCS URI")
    conf = context["dag_run"].conf or {}
    ingest_date = str(
        conf.get("ingest_date") or context["logical_date"].strftime("%Y-%m-%d")
    )
    gcs_prefix = (conf.get("gcs_prefix") or "").strip() or None
    return transform_and_load_bq(
        source_zip_uri=source_zip_uri,
        gcp_project=str(conf.get("gcp_project", "dev-dfml-platform")),
        location=str(conf.get("location", "asia-northeast3")),
        dl_dataset=str(conf.get("dl_dataset", "dl_external")),
        gcs_bucket=str(conf.get("gcs_bucket", "dfml-dev-raw")),
        ingest_date=ingest_date,
        gcs_prefix=gcs_prefix,
    )


with DAG(
    dag_id="external_data__usda_psd_oilseeds",
    description=(
        "USDA PS&D oilseeds ZIP 수집(GCS raw) 후 "
        "미국 대두 생산량 필터 -> dev-dfml-platform.dl_external.psd_oilseeds"
    ),
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["external_data", "usda", "psd", "gcs", "bigquery"],
) as dag:
    fetch_raw = PythonOperator(
        task_id="fetch_raw_to_gcs",
        python_callable=_fetch_raw,
    )

    transform_load = PythonOperator(
        task_id="transform_and_load_bq",
        python_callable=_transform_load,
    )

    fetch_raw >> transform_load
