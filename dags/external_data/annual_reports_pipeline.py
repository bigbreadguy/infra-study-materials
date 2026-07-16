"""Load a manually-uploaded annual statistics workbook (IEA WEI) into BigQuery.

The IEA World Energy Investment data file is a yearly source that **blocks bots**, so it
cannot ride the recipe -> envelope flow of ``scrape_external_data_pipeline``. The operating
model is manual:

1. An operator downloads the workbook by hand and uploads the ``.xlsx`` to GCS.
2. They trigger this DAG with ``dag_run.conf = {"source": ..., "input_uri": "gs://.../file.xlsx"}``.

The DAG then mirrors ``estat_file_pipeline`` -- the heavy XLSX parsing is **outsourced to the
``dfml-scraper`` Cloud Run Job** (``ANNUAL_REPORTS_MODE``), never done in the Airflow pod:

1. **produce_parquet** runs the scraper job in annual-reports mode: it reads the uploaded
   ``INPUT_URI`` workbook, melts it to long-format records (one ``{"row": <json>}`` per
   region/metric/year), and writes one run-scoped parquet to GCS (``OUTPUT_URI``).
2. **load_parquet_to_bigquery** loads that parquet into the ``dl_materials`` star schema via
   the shared materials transform (``source_format="PARQUET"``), keyed by the source's
   committed metric configs (``configs/materials_metrics/<source>/``). Only the configured
   series are MERGEd; the rest of the workbook's cube is ignored by design.

One pod per task (KubernetesExecutor): the Cloud Run Job does the XLSX work off-pod and the
BigQuery transform runs as one job, so each Airflow task is just an I/O orchestrator. The run
is idempotent -- the scraper rewrites the run's parquet and the fact_values MERGE dedups --
so a coarse retry safely replays it.

dag_run.conf (required):
- source:    ``iea_wei`` -- selects the scraper parser and the metric configs
- input_uri: ``gs://bucket/path/to/workbook.xlsx`` the operator uploaded
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, Variable, get_current_context, task
from pendulum import datetime

from external_data.common.materials_bigquery import run_combined_materials_transform
from external_data.common.materials_metrics import load_materials_config
from external_data.common.scrape_request import gcs_uri
from external_data.common.scraper_cloud_run import (
    SCRAPE_EXECUTION_TIMEOUT,
    execute_scraper_job,
)


# Source slug -> its committed materials metric config stems. Each source is one datasource,
# so the configs share a datasource and load in a single combined transform. Adding a curated
# metric is a config edit (no code change); adding a source means a new parser + entry here.
SOURCE_CONFIGS: dict[str, list[str]] = {
    "iea_wei": ["iea_wei.electricity_networks"],
}

_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")
_JOB_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")


def _required_variable(name: str) -> str:
    value = Variable.get(name, default=None)
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _config() -> dict[str, str]:
    return {
        "project_id": _required_variable("scraper_gcp_project_id"),
        "region": _required_variable("scraper_cloud_run_region"),
        "job_name": _required_variable("scraper_cloud_run_job_name"),
        "bucket": _required_variable("scraper_scrape_bucket_name"),
    }


def _bq_config() -> dict[str, str]:
    return {
        "project_id": _required_variable("materials_bigquery_project_id"),
        "dataset_id": Variable.get("materials_bigquery_dataset_id", default="dl_materials"),
        "region": _required_variable("materials_bigquery_region"),
    }


def _label_value(value: Any) -> str:
    return _LABEL_DISALLOWED.sub("_", str(value).lower())[:63]


def _resolve_source(params: dict[str, Any]) -> str:
    source = (params.get("source") or "").strip()
    if source not in SOURCE_CONFIGS:
        raise ValueError(
            f"dag_run.conf 'source' must be one of {sorted(SOURCE_CONFIGS)}, got {source!r}"
        )
    return source


def _resolve_input_uri(params: dict[str, Any]) -> str:
    input_uri = (params.get("input_uri") or "").strip()
    if not input_uri.startswith("gs://"):
        raise ValueError(
            "dag_run.conf 'input_uri' must be a gs:// URI to the uploaded workbook"
        )
    return input_uri


def _parquet_object_name(source: str, run_id: str) -> str:
    # Run-scoped so concurrent/historic runs never clobber each other; rewritten in place on
    # retry (idempotent).
    safe_run = _JOB_ID_DISALLOWED.sub("_", run_id)
    return f"scrape/annual_reports/{source}/{safe_run}/{source}.parquet"


with DAG(
    dag_id="external_data__annual_reports",
    description="Manually-uploaded IEA WEI annual XLSX -> parquet (Cloud Run) -> BigQuery (dl_materials)",
    # Manual only: the operator uploads the workbook to GCS and triggers with conf.
    schedule=None,
    start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    params={"source": "", "input_uri": ""},
    tags=[
        "external_data",
        "scraper",
        "iea_wei",
        "cloud-run",
        "bigquery",
        "manual",
    ],
) as dag:

    @task(execution_timeout=SCRAPE_EXECUTION_TIMEOUT)
    def produce_parquet() -> dict[str, Any]:
        """Run the scraper job in annual-reports mode; it parses the uploaded workbook to parquet."""
        context = get_current_context()
        run_id = context["run_id"]
        params = context.get("params") or {}
        source = _resolve_source(params)
        input_uri = _resolve_input_uri(params)
        config = _config()

        parquet_object = _parquet_object_name(source, run_id)
        output_uri = gcs_uri(config["bucket"], parquet_object)
        # REQUEST_URI is unused in file mode (job.py dispatches before reading it) but the
        # run-request builder requires a non-empty value; pass a run-scoped placeholder.
        request_uri = gcs_uri(
            config["bucket"], f"scrape/annual_reports/{source}/{run_id}/_unused_request.json"
        )
        extra_env = {
            "ANNUAL_REPORTS_MODE": "1",
            "ANNUAL_REPORTS_SOURCE": source,
            "INPUT_URI": input_uri,
        }

        print(f"::group::[annual-reports] produce parquet ({source}) {input_uri} -> {output_uri}")
        result = execute_scraper_job(
            project_id=config["project_id"],
            region=config["region"],
            job_name=config["job_name"],
            request_uri=request_uri,
            output_uri=output_uri,
            extra_env=extra_env,
        )
        print(
            f"[annual-reports] step=produce source={source} "
            f"execution={result.get('execution_name')} parquet={output_uri}"
        )
        print("::endgroup::")
        return {"parquet_uri": output_uri, "source": source}

    @task()
    def load_parquet_to_bigquery(produced: dict[str, Any]) -> dict[str, Any]:
        """Load the run's parquet into fact_values via the shared materials transform."""
        from google.cloud import bigquery

        context = get_current_context()
        bq = _bq_config()
        source = produced["source"]
        raw_gcs_uri = produced["parquet_uri"]
        configs = [load_materials_config(stem) for stem in SOURCE_CONFIGS[source]]

        labels = {
            "dag_id": _label_value(getattr(context.get("dag_run"), "dag_id", "") or ""),
            "run_id": _label_value(context.get("run_id", "")),
            "source": _label_value(source),
            "step": "load",
        }
        job_id_prefix = _JOB_ID_DISALLOWED.sub(
            "_", f"annual_reports_{source}_{context.get('run_id', '')}_load_"
        )[:512]

        client = bigquery.Client(project=bq["project_id"], location=bq["region"])
        result = run_combined_materials_transform(
            client,
            project_id=bq["project_id"],
            dataset_id=bq["dataset_id"],
            region=bq["region"],
            raw_gcs_uri=raw_gcs_uri,
            configs=configs,
            # The parquet row count is not known to the DAG (the scraper wrote it off-pod),
            # so no expected_row_count assert; the MERGE is idempotent regardless.
            expected_row_count=None,
            source_format="PARQUET",
            labels=labels,
            job_id_prefix=job_id_prefix,
        )
        merged = result.get("dml_affected_rows")
        print(f"::group::[annual-reports] load {source} ({len(configs)} configs)")
        print(
            f"[annual-reports] step=load source={source} merged={merged} "
            f"job_id={result.get('job_id')}"
        )
        print("::endgroup::")
        return {"merged_rows": merged, "job_id": result.get("job_id")}

    load_parquet_to_bigquery(produce_parquet())
